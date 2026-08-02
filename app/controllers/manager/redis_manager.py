import json
import time
import uuid
from typing import Dict

import redis

from app.controllers.manager.base_manager import IdempotencyConflictError, TaskManager
from app.models.schema import VideoParams
from app.services import task as tm

FUNC_MAP = {
    "start": tm.start,
    # 'start_test': tm.start_test
}


class RedisTaskManager(TaskManager):
    def __init__(
        self,
        max_concurrent_tasks: int,
        redis_url: str,
        max_queued_tasks: int = 100,
    ):
        self.redis_client = redis.Redis.from_url(redis_url)
        super().__init__(max_concurrent_tasks, max_queued_tasks=max_queued_tasks)

    def create_queue(self):
        return "task_queue"

    def reserve_idempotency(
        self, idempotency_key: str, payload_hash: str, task_id: str
    ) -> tuple[str, str | None, bool]:
        storage_key = self.idempotency_storage_key(idempotency_key)
        owner_token = uuid.uuid4().hex
        record = self.idempotency_record(payload_hash, task_id, "pending", owner_token)
        deadline = time.monotonic() + 0.25
        while True:
            if self.redis_client.set(storage_key, record, nx=True):
                return task_id, owner_token, True

            existing_raw = self.redis_client.get(storage_key)
            if existing_raw is not None:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("idempotency reservation is unavailable")
            time.sleep(0.01)
        if isinstance(existing_raw, bytes):
            existing_raw = existing_raw.decode("utf-8")
        existing = json.loads(existing_raw)
        if existing["payload_hash"] != payload_hash:
            raise IdempotencyConflictError(
                "idempotency key already used with a different payload"
            )
        return existing["task_id"], None, False

    def wait_idempotency(self, idempotency_key, payload_hash, timeout):
        storage_key = self.idempotency_storage_key(idempotency_key)
        deadline = time.monotonic() + max(0, timeout)
        while True:
            raw = self.redis_client.get(storage_key)
            if raw is None:
                return None
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            record = json.loads(raw)
            if record["payload_hash"] != payload_hash:
                raise IdempotencyConflictError("idempotency key already used with a different payload")
            if record["state"] == "committed":
                return record["task_id"]
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            time.sleep(min(0.02, remaining))

    def commit_idempotency(self, idempotency_key, payload_hash, task_id, owner_token):
        storage_key = self.idempotency_storage_key(idempotency_key)
        expected = self.idempotency_record(payload_hash, task_id, "pending", owner_token)
        committed = self.idempotency_record(payload_hash, task_id, "committed", None)
        changed = self.redis_client.eval(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('set', KEYS[1], ARGV[2]) else return 0 end",
            1, storage_key, expected, committed,
        )
        if not changed:
            raise RuntimeError("idempotency reservation ownership lost")

    def release_idempotency(
        self, idempotency_key: str, payload_hash: str, task_id: str, owner_token: str
    ) -> None:
        storage_key = self.idempotency_storage_key(idempotency_key)
        expected = self.idempotency_record(payload_hash, task_id, "pending", owner_token)
        self.redis_client.eval(
            "if redis.call('get', KEYS[1]) == ARGV[1] then "
            "return redis.call('del', KEYS[1]) else return 0 end",
            1,
            storage_key,
            expected,
        )

    def enqueue(self, task: Dict):
        task_with_serializable_params = task.copy()
        # task.copy() 只复制最外层字典；如果直接改写嵌套 kwargs，会把调用方
        # 持有的 VideoParams 同步替换成 dict。后续日志或重试仍可能读取原任务，
        # 因此这里单独复制 kwargs，确保序列化过程没有意外副作用。
        task_kwargs = task.get("kwargs", {})
        task_with_serializable_params["kwargs"] = task_kwargs.copy()

        if "params" in task_kwargs and isinstance(task_kwargs["params"], VideoParams):
            task_with_serializable_params["kwargs"]["params"] = task_kwargs[
                "params"
            ].model_dump(warnings=False)

        # 将函数对象转换为其名称
        task_with_serializable_params["func"] = task["func"].__name__
        self.redis_client.rpush(self.queue, json.dumps(task_with_serializable_params))

    def dequeue(self):
        task_json = self.redis_client.lpop(self.queue)
        if task_json:
            task_info = json.loads(task_json)
            # 将函数名称转换回函数对象
            task_info["func"] = FUNC_MAP[task_info["func"]]

            if "params" in task_info["kwargs"] and isinstance(
                task_info["kwargs"]["params"], dict
            ):
                task_info["kwargs"]["params"] = VideoParams(
                    **task_info["kwargs"]["params"]
                )

            return task_info
        return None

    def is_queue_empty(self):
        return self.redis_client.llen(self.queue) == 0

    def queue_size(self):
        return self.redis_client.llen(self.queue)
