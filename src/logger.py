from pathlib import Path
from datetime import datetime, date
import contextvars

BASE_DIR = Path(__file__).resolve().parents[1]
LOGS_DIR = BASE_DIR / "logs"

_request_id = contextvars.ContextVar("request_id", default="-")


def set_request_id(value: str):
    _request_id.set(value)


def get_request_id() -> str:
    return _request_id.get()


def today_str():
    return date.today().isoformat()


def log_path():
    return LOGS_DIR / f"app_{today_str()}.log"


def log_line(component: str, level: str, message: str):
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rid = get_request_id()

    line = f"{ts} | REQ={rid} | {component:<10} | {level.upper():<5} | {message}\n"

    with log_path().open("a", encoding="utf-8") as f:
        f.write(line)