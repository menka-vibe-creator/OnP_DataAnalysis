"""Audit-logging hooks: write every tool call to a newline-delimited JSON log.

Two hooks work together:
  pre_timing_hook  (PreToolUse)  — records the start time keyed by tool_use_id
  post_audit_hook  (PostToolUse) — writes the completed record to audit.jsonl

Log location: logs/audit.jsonl  (one JSON object per line, UTC timestamps).

Example log line:
  {
    "ts": "2026-03-14T19:00:01Z",
    "session_id": "abc123",
    "agent_id": "main",
    "agent_type": "primary",
    "tool_use_id": "toolu_xyz",
    "tool_name": "Bash",
    "tool_input": {"command": "python analyse.py"},
    "duration_ms": 412,
    "result_preview": "mean=3.14, std=0.71..."
  }
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# Module-level store: tool_use_id → monotonic start time
_start_times: dict[str, float] = {}

# Where to write audit records; override in tests via set_log_path()
# When None, _get_log_path() returns a daily rotating path: logs/audit_YYYY-MM-DD.jsonl
_LOG_PATH: Path | None = None
_MAX_PREVIEW_CHARS = 300
_MAX_INPUT_CHARS = 500


def _daily_log_path() -> Path:
    """Return today's rotating daily log path."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return Path(f"logs/audit_{today}.jsonl")


def set_log_path(path: str | Path) -> None:
    """Override the audit log path (used in tests)."""
    global _LOG_PATH
    _LOG_PATH = Path(path)


async def pre_timing_hook(
    input_data: dict,
    tool_use_id: str,
    context: dict,
) -> dict:
    """PreToolUse hook — record the wall-clock start time for each tool call."""
    _start_times[tool_use_id] = time.monotonic()
    return {}


async def post_audit_hook(
    input_data: dict,
    tool_use_id: str,
    context: dict,
) -> dict:
    """PostToolUse hook — append a structured JSON record to the audit log."""
    t_end = time.monotonic()
    t_start = _start_times.pop(tool_use_id, t_end)
    duration_ms = round((t_end - t_start) * 1000)

    tool_name: str = input_data.get("tool_name", "unknown")
    tool_input: dict = input_data.get("tool_input", {})
    tool_response = input_data.get("tool_response", {})

    # Build a short preview of the result
    result_preview = _extract_preview(tool_response)

    # Truncate tool_input values that are very long (e.g. full file contents)
    safe_input = _truncate_input(tool_input)

    record = {
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "session_id": str(context.get("session_id", "")),
        "agent_id": str(input_data.get("agent_id", "")),
        "agent_type": str(input_data.get("agent_type", "")),
        "tool_use_id": tool_use_id,
        "tool_name": tool_name,
        "tool_input": safe_input,
        "duration_ms": duration_ms,
        "result_preview": result_preview,
    }

    _write_record(record)
    logger.debug(
        "audit.written",
        extra={
            "tool_name": tool_name,
            "tool_use_id": tool_use_id,
            "duration_ms": duration_ms,
        },
    )
    return {}


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _write_record(record: dict) -> None:
    """Append a single JSON record to the daily log and, if set, the per-run temp log."""
    paths = [_daily_log_path()]
    if _LOG_PATH is not None:
        paths.append(_LOG_PATH)
    for log_path in paths:
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        except OSError as exc:
            logger.error("audit.write_failed", extra={"error": str(exc), "path": str(log_path)})


def _extract_preview(tool_response: object) -> str:
    """Pull a short text preview out of the tool response (varies by type)."""
    if isinstance(tool_response, str):
        text = tool_response
    elif isinstance(tool_response, dict):
        # MCP content format: {"content": [{"type": "text", "text": "..."}]}
        parts = tool_response.get("content", [])
        text = " ".join(
            p.get("text", "") for p in parts if isinstance(p, dict) and p.get("type") == "text"
        )
        if not text:
            text = json.dumps(tool_response)
    elif isinstance(tool_response, list):
        text = json.dumps(tool_response)
    else:
        text = str(tool_response)

    text = text.strip().replace("\n", " ")
    if len(text) > _MAX_PREVIEW_CHARS:
        text = text[:_MAX_PREVIEW_CHARS] + "…"
    return text


def _truncate_input(tool_input: dict) -> dict:
    """Return a copy of tool_input with very long string values truncated."""
    result: dict = {}
    for k, v in tool_input.items():
        if isinstance(v, str) and len(v) > _MAX_INPUT_CHARS:
            result[k] = v[:_MAX_INPUT_CHARS] + "…"
        else:
            result[k] = v
    return result
