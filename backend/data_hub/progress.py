"""Rebuild progress tracking — file-backed so it's robust across
Python module import paths and any worker/thread boundary.

The state is persisted to ``cache/rebuild_progress.json`` on every
update so a poll endpoint running on any worker / any request context
reads the same, most-recent snapshot.

State shape:
    {
        "running": bool,
        "pct": int,                 # 0..100
        "stage": str,
        "message": str,
        "started_at": iso-string,
        "finished_at": iso-string,
        "error": str | None,
        "log_tail": [str, ...],     # rolling tail
    }
"""
import json
import os
from datetime import datetime
from threading import Lock

_LOCK = Lock()
_MAX_LOG_LINES = 40

_INITIAL_STATE = {
    "running": False,
    "pct": 0,
    "stage": "Idle",
    "message": "",
    "started_at": None,
    "finished_at": None,
    "error": None,
    "log_tail": [],
}


def _progress_path():
    """Resolve the absolute path of the JSON progress file.

    Prefers the same ``backend/cache`` directory the orchestrator uses
    so state survives alongside other cached artifacts. Falls back to a
    temp dir if that can't be created.
    """
    here = os.path.dirname(os.path.abspath(__file__))           # backend/data_hub
    backend = os.path.dirname(here)                             # backend
    cache = os.path.join(backend, 'cache')
    try:
        os.makedirs(cache, exist_ok=True)
        return os.path.join(cache, 'rebuild_progress.json')
    except Exception:
        import tempfile
        return os.path.join(tempfile.gettempdir(), 'rebuild_progress.json')


def _read():
    """Read the current state from disk, falling back to initial state."""
    path = _progress_path()
    if not os.path.exists(path):
        return dict(_INITIAL_STATE, log_tail=[])
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # Guard against partial/corrupt files
        for k, v in _INITIAL_STATE.items():
            if k not in data:
                data[k] = v if not isinstance(v, list) else []
        if not isinstance(data.get('log_tail'), list):
            data['log_tail'] = []
        return data
    except Exception:
        return dict(_INITIAL_STATE, log_tail=[])


def _write(state):
    """Atomic-ish write — write to .tmp then rename."""
    path = _progress_path()
    tmp = path + '.tmp'
    try:
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(state, f)
        os.replace(tmp, path)
    except Exception as e:
        # Last resort: direct write, don't crash the caller
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(state, f)
        except Exception:
            pass


def snapshot():
    """Return the current progress state (as seen on disk)."""
    with _LOCK:
        return _read()


def start():
    with _LOCK:
        state = {
            "running": True,
            "pct": 1,
            "stage": "Starting rebuild",
            "message": "Preparing data sources",
            "started_at": datetime.now().isoformat(),
            "finished_at": None,
            "error": None,
            "log_tail": [],
        }
        _write(state)


def set_stage(pct, stage, message=""):
    """Update the current stage. pct is clamped to 0..100."""
    try:
        pct_i = int(max(0, min(100, pct)))
    except (TypeError, ValueError):
        pct_i = 0
    with _LOCK:
        state = _read()
        state["running"] = True
        state["pct"] = pct_i
        state["stage"] = str(stage)[:120]
        if message:
            state["message"] = str(message)[:300]
        _write(state)


def log(line):
    """Append one line to the rolling log tail."""
    if not line:
        return
    s = str(line).rstrip()
    if not s:
        return
    with _LOCK:
        state = _read()
        tail = state.get("log_tail") or []
        tail.append(s)
        if len(tail) > _MAX_LOG_LINES:
            tail = tail[-_MAX_LOG_LINES:]
        state["log_tail"] = tail
        _write(state)


def finish(error=None):
    with _LOCK:
        state = _read()
        state["running"] = False
        state["pct"] = state.get("pct", 0) if error else 100
        state["stage"] = "Failed" if error else "Complete"
        state["error"] = str(error) if error else None
        state["finished_at"] = datetime.now().isoformat()
        _write(state)
