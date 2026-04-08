"""Rebuild progress tracking.

Holds a module-level dict representing the current state of a dashboard
rebuild so that a poll endpoint can expose it to the frontend while the
rebuild is still running in a threadpool.

State shape:
    {
        "running": bool,            # True while rebuild in flight
        "pct": int,                 # 0..100
        "stage": str,               # short label, e.g. "Assembling workbook"
        "message": str,             # longer human-readable detail
        "started_at": iso-string,   # when the current rebuild kicked off
        "finished_at": iso-string,  # set when rebuild completes
        "error": str | None,        # set if rebuild failed
        "log_tail": [str, ...],     # last N captured log lines
    }
"""
from datetime import datetime
from threading import Lock

_LOCK = Lock()
_MAX_LOG_LINES = 40

_STATE = {
    "running": False,
    "pct": 0,
    "stage": "Idle",
    "message": "",
    "started_at": None,
    "finished_at": None,
    "error": None,
    "log_tail": [],
}


def snapshot():
    """Return a shallow copy of the current progress state."""
    with _LOCK:
        return {
            "running": _STATE["running"],
            "pct": _STATE["pct"],
            "stage": _STATE["stage"],
            "message": _STATE["message"],
            "started_at": _STATE["started_at"],
            "finished_at": _STATE["finished_at"],
            "error": _STATE["error"],
            "log_tail": list(_STATE["log_tail"]),
        }


def start():
    with _LOCK:
        _STATE["running"] = True
        _STATE["pct"] = 0
        _STATE["stage"] = "Starting rebuild"
        _STATE["message"] = ""
        _STATE["started_at"] = datetime.now().isoformat()
        _STATE["finished_at"] = None
        _STATE["error"] = None
        _STATE["log_tail"] = []


def set_stage(pct, stage, message=""):
    """Update the current stage. pct is clamped to 0..100."""
    try:
        pct_i = int(max(0, min(100, pct)))
    except (TypeError, ValueError):
        pct_i = 0
    with _LOCK:
        _STATE["pct"] = pct_i
        _STATE["stage"] = str(stage)[:120]
        if message:
            _STATE["message"] = str(message)[:300]


def log(line):
    """Append one line to the rolling log tail."""
    if not line:
        return
    s = str(line).rstrip()
    if not s:
        return
    with _LOCK:
        _STATE["log_tail"].append(s)
        if len(_STATE["log_tail"]) > _MAX_LOG_LINES:
            del _STATE["log_tail"][: len(_STATE["log_tail"]) - _MAX_LOG_LINES]


def finish(error=None):
    with _LOCK:
        _STATE["running"] = False
        _STATE["pct"] = 100 if not error else _STATE["pct"]
        _STATE["stage"] = "Failed" if error else "Complete"
        _STATE["error"] = str(error) if error else None
        _STATE["finished_at"] = datetime.now().isoformat()
