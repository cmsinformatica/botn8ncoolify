import hashlib
import json
import threading
import time
import uuid
from typing import Any, Callable, Dict

from loguru import logger


class TaskQueueFullError(ValueError):
    pass


class IdempotencyConflictError(ValueError):
    pass


class TaskManager:
    def __init__(self, max_concurrent_tasks: int, max_queued_tasks: int = 100):
        self.max_concurrent_tasks = max_concurrent_tasks
        self.max_queued_tasks = max_queued_tasks
        self.current_tasks = 0
        self.lock = threading.Lock()
        self._idempotency_changed = threading.Condition(self.lock)
        self._idempotency_records = {}
        self.queue = self.create_queue()

    @staticmethod
    def idempotency_storage_key(idempotency_key: str) -> str:
        digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
        return f"moneyprinter:idempotency:videos:{digest}"

    @staticmethod
    def idempotency_record(payload_hash: str, task_id: str, state="committed", owner_token=None) -> str:
        return json.dumps(
            {"owner_token": owner_token, "payload_hash": payload_hash, "state": state, "task_id": task_id},
            sort_keys=True,
            separators=(",", ":"),
        )

    def reserve_idempotency(
        self, idempotency_key: str, payload_hash: str, task_id: str
    ) -> tuple[str, str | None, bool]:
        storage_key = self.idempotency_storage_key(idempotency_key)
        with self.lock:
            existing = self._idempotency_records.get(storage_key)
            if existing is None:
                owner_token = uuid.uuid4().hex
                self._idempotency_records[storage_key] = {
                    "payload_hash": payload_hash,
                    "task_id": task_id,
                    "state": "pending",
                    "owner_token": owner_token,
                }
                return task_id, owner_token, True
            if existing["payload_hash"] != payload_hash:
                raise IdempotencyConflictError(
                    "idempotency key already used with a different payload"
                )
            return existing["task_id"], None, False

    def wait_idempotency(self, idempotency_key, payload_hash, timeout):
        storage_key = self.idempotency_storage_key(idempotency_key)
        deadline = time.monotonic() + max(0, timeout)
        with self._idempotency_changed:
            while True:
                existing = self._idempotency_records.get(storage_key)
                if existing is None:
                    return None
                if existing["payload_hash"] != payload_hash:
                    raise IdempotencyConflictError("idempotency key already used with a different payload")
                if existing["state"] == "committed":
                    return existing["task_id"]
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self._idempotency_changed.wait(remaining)

    def commit_idempotency(self, idempotency_key, payload_hash, task_id, owner_token):
        storage_key = self.idempotency_storage_key(idempotency_key)
        expected = {"payload_hash": payload_hash, "task_id": task_id, "state": "pending", "owner_token": owner_token}
        with self._idempotency_changed:
            if self._idempotency_records.get(storage_key) != expected:
                raise RuntimeError("idempotency reservation ownership lost")
            self._idempotency_records[storage_key] = {**expected, "state": "committed", "owner_token": None}
            self._idempotency_changed.notify_all()

    def release_idempotency(
        self, idempotency_key: str, payload_hash: str, task_id: str, owner_token: str
    ) -> None:
        storage_key = self.idempotency_storage_key(idempotency_key)
        with self.lock:
            existing = self._idempotency_records.get(storage_key)
            if existing == {"payload_hash": payload_hash, "task_id": task_id, "state": "pending", "owner_token": owner_token}:
                self._idempotency_records.pop(storage_key, None)
                self._idempotency_changed.notify_all()

    def create_queue(self):
        raise NotImplementedError()

    def add_task(self, func: Callable, *args: Any, **kwargs: Any):
        with self.lock:
            if self.current_tasks < self.max_concurrent_tasks:
                logger.info(
                    f"add task: {func.__name__}, current_tasks: {self.current_tasks}"
                )
                # 在线程启动前先预占并发名额。原实现在线程内部递增，连续请求
                # 可能都在子线程获得锁之前看到 current_tasks=0，从而突破并发
                # 上限。启动失败时回滚名额，让后续请求仍可正常调度。
                self.current_tasks += 1
                try:
                    self.execute_task(func, *args, **kwargs)
                except Exception:
                    self.current_tasks -= 1
                    raise
            else:
                queue_size = self.queue_size()
                # 并发数已满时才进入排队。队列必须有上限，否则匿名接口可以持续
                # 堆积任务对象和请求参数，最终造成内存耗尽或第三方 API 成本失控。
                if queue_size >= self.max_queued_tasks:
                    logger.warning(
                        f"reject task: {func.__name__}, queue_size: {queue_size}, "
                        f"max_queued_tasks: {self.max_queued_tasks}"
                    )
                    raise TaskQueueFullError("task queue is full, please try again later")

                logger.info(
                    f"enqueue task: {func.__name__}, current_tasks: {self.current_tasks}, "
                    f"queue_size: {queue_size}"
                )
                self.enqueue({"func": func, "args": args, "kwargs": kwargs})

    def execute_task(self, func: Callable, *args: Any, **kwargs: Any):
        thread = threading.Thread(
            target=self.run_task, args=(func, *args), kwargs=kwargs
        )
        thread.start()

    def run_task(self, func: Callable, *args: Any, **kwargs: Any):
        try:
            func(*args, **kwargs)  # call the function here, passing *args and **kwargs.
        finally:
            self.task_done()

    def check_queue(self):
        with self.lock:
            if (
                self.current_tasks < self.max_concurrent_tasks
                and not self.is_queue_empty()
            ):
                task_info = self.dequeue()
                func = task_info["func"]
                args = task_info.get("args", ())
                kwargs = task_info.get("kwargs", {})
                # 与直接创建任务保持同一计数时机，避免刚出队的任务尚未在线程
                # 内计数时，又有新请求绕过队列占用同一个并发名额。
                self.current_tasks += 1
                try:
                    self.execute_task(func, *args, **kwargs)
                except Exception:
                    self.current_tasks -= 1
                    self.enqueue(task_info)
                    raise

    def task_done(self):
        with self.lock:
            self.current_tasks -= 1
        self.check_queue()

    def enqueue(self, task: Dict):
        raise NotImplementedError()

    def dequeue(self):
        raise NotImplementedError()

    def is_queue_empty(self):
        raise NotImplementedError()

    def queue_size(self):
        raise NotImplementedError()
