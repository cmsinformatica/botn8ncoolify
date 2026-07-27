import os
import shutil
import fcntl
from pathlib import Path

import toml


ROOT = Path("/MoneyPrinterTurbo")
DATA = Path("/data")
CONFIG = DATA / "config.toml"

DATA.mkdir(parents=True, exist_ok=True)
(DATA / "storage").mkdir(parents=True, exist_ok=True)

lock_path = DATA / ".config.lock"
with lock_path.open("w") as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)

    if not CONFIG.exists():
        shutil.copy2(ROOT / "config.example.toml", CONFIG)

    config = toml.load(CONFIG)
    app = config.setdefault("app", {})

    overrides = {
        "MPT_API_KEY": (app, "api_key"),
        "MPT_PEXELS_API_KEY": (app, "pexels_api_keys"),
        "MPT_PIXABAY_API_KEY": (app, "pixabay_api_keys"),
        "MPT_OPENAI_API_KEY": (app, "openai_api_key"),
        "MPT_OPENAI_BASE_URL": (app, "openai_base_url"),
        "MPT_OPENAI_MODEL": (app, "openai_model_name"),
        "MPT_GEMINI_API_KEY": (app, "gemini_api_key"),
    }

    for env_name, (section, key) in overrides.items():
        value = os.getenv(env_name, "").strip()
        if not value:
            continue
        section[key] = [value] if key.endswith("_api_keys") else value

    provider = os.getenv("MPT_LLM_PROVIDER", "").strip()
    if provider:
        app["llm_provider"] = provider

    config["listen_host"] = "0.0.0.0"
    config["listen_port"] = 8080

    temporary = DATA / "config.toml.tmp"
    with temporary.open("w", encoding="utf-8") as handle:
        toml.dump(config, handle)
    temporary.replace(CONFIG)

app_config = ROOT / "config.toml"
if app_config.exists() or app_config.is_symlink():
    app_config.unlink()
app_config.symlink_to(CONFIG)

storage = ROOT / "storage"
if storage.exists() and not storage.is_symlink():
    if storage.is_dir() and not any(storage.iterdir()):
        storage.rmdir()
if not storage.exists():
    storage.symlink_to(DATA / "storage", target_is_directory=True)
