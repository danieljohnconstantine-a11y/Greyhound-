import os, time, json, gzip, pathlib, random
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from fake_useragent import UserAgent

OUT_BASE = pathlib.Path("data")
OUT_BASE.mkdir(parents=True, exist_ok=True)

def utcstamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def ensure_dir(p: pathlib.Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def write_text(path: pathlib.Path, text: str) -> None:
    ensure_dir(path.parent)
    path.write_text(text, encoding="utf-8")

def write_json(path: pathlib.Path, obj) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

def write_bytes(path: pathlib.Path, b: bytes) -> None:
    ensure_dir(path.parent)
    path.write_bytes(b)

def ua_string() -> str:
    try:
        return UserAgent().chrome
    except Exception:
        # simple fallback
        return (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )

def today_au():
    # Use UTC date for consistency in CI; you can switch to AU if you self-host.
    return datetime.utcnow().strftime("%Y-%m-%d")

def env_proxy():
    return os.environ.get("PROXY_URL", "").strip() or None

class SoftError(RuntimeError):
    """Non-fatal error; pipeline continues but notes issue."""
    pass
