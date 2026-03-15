"""CSV Data Analyst Agent — powered by Claude Agent SDK.

Entry points
------------
CLI:
    uv run python agent.py "Analyse the claims data" --data data/op_claims_data.csv
    uv run python agent.py "..." --max-turns 15 --budget 0.50 --log-level DEBUG

Python:
    import anyio
    from agent import run_agent
    result = anyio.run(run_agent, "Summarise key trends", "data/op_claims_data.csv")
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

import anyio
from dotenv import load_dotenv

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    ResultMessage,
    SystemMessage,
    TextBlock,
)

from hooks import POST_TOOL_USE_HOOKS, PRE_TOOL_USE_HOOKS
from hooks.audit import set_log_path
from prompts.system import SYSTEM_PROMPT
from tools.sqlite_tool import build_sqlite_mcp_server

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
load_dotenv(".env.local")
load_dotenv()

# ---------------------------------------------------------------------------
# Defaults (override via CLI flags or env vars)
# ---------------------------------------------------------------------------
DEFAULT_MAX_TURNS = int(os.environ.get("AGENT_MAX_TURNS", "25"))
DEFAULT_MAX_BUDGET_USD = float(os.environ.get("AGENT_MAX_BUDGET_USD", "1.00"))
MCP_CONFIG_PATH = Path(os.environ.get("MCP_CONFIG_PATH", "mcp_servers.json"))
AUDIT_LOG_PATH = Path(os.environ.get("AUDIT_LOG_PATH", "logs/audit.jsonl"))

PROJECT_ROOT = Path(__file__).parent

# ---------------------------------------------------------------------------
# Structured logging
# ---------------------------------------------------------------------------

class _StructuredFormatter(logging.Formatter):
    """Emit log records as key=value structured lines, easy to grep / parse."""

    _SKIP = frozenset({
        "name", "msg", "args", "levelname", "levelno", "pathname",
        "filename", "module", "exc_info", "exc_text", "stack_info",
        "lineno", "funcName", "created", "msecs", "relativeCreated",
        "thread", "threadName", "processName", "process", "message",
        "taskName",
    })

    def format(self, record: logging.LogRecord) -> str:
        ts = self.formatTime(record, "%Y-%m-%dT%H:%M:%S")
        level = record.levelname.ljust(8)
        msg = record.getMessage()
        extras = " ".join(
            f"{k}={v!r}"
            for k, v in record.__dict__.items()
            if k not in self._SKIP
        )
        parts = [f"{ts} {level} [{record.name}] {msg}"]
        if extras:
            parts.append(extras)
        if record.exc_info:
            parts.append(self.formatException(record.exc_info))
        return "  ".join(parts)


def configure_logging(level: str = "INFO") -> None:
    """Configure root logger with the structured formatter (idempotent)."""
    if logging.root.handlers:
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_StructuredFormatter())
    logging.root.setLevel(level)
    logging.root.handlers = [handler]
    for noisy in ("httpx", "httpcore", "asyncio", "mcp"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# MCP server config loader
# ---------------------------------------------------------------------------

def load_mcp_servers(config_path: Path = MCP_CONFIG_PATH) -> dict:
    """Load external MCP server definitions from mcp_servers.json.

    Only entries without ``"_disabled": true`` are included.
    Environment variable placeholders ``${VAR}`` in string values are expanded.

    Returns a dict suitable for ``ClaudeAgentOptions.mcp_servers``.
    """
    if not config_path.exists():
        logger.debug("mcp_config.not_found", extra={"path": str(config_path)})
        return {}

    try:
        raw = json.loads(config_path.read_text())
    except json.JSONDecodeError as exc:
        logger.warning("mcp_config.parse_error", extra={"path": str(config_path), "error": str(exc)})
        return {}

    servers: dict = {}
    for name, cfg in raw.get("servers", {}).items():
        if name.startswith("_") or cfg.get("_disabled"):
            continue
        expanded = _expand_env(cfg)
        # Strip internal comment keys before passing to SDK
        servers[name] = {k: v for k, v in expanded.items() if not k.startswith("_")}
        logger.info("mcp_config.loaded", extra={"server": name})

    return servers


def _expand_env(obj: object) -> object:
    """Recursively expand ``${VAR}`` placeholders using os.environ."""
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    if isinstance(obj, dict):
        return {k: _expand_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env(item) for item in obj]
    return obj


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------

async def run_agent(
    user_prompt: str,
    data_path: str | None = None,
    *,
    max_turns: int = DEFAULT_MAX_TURNS,
    max_budget_usd: float = DEFAULT_MAX_BUDGET_USD,
    log_level: str = "INFO",
    audit_log: str | None = None,
) -> str:
    """Run the CSV data analyst agent.

    Args:
        user_prompt:    Natural-language analysis task.
        data_path:      Optional CSV/DB path to include in the prompt.
        max_turns:      Maximum agent turns before stopping (default 25).
        max_budget_usd: Maximum spend in USD before the agent is halted (default $1.00).
        log_level:      Python logging level string (default "INFO").
        audit_log:      Override path for the audit JSONL log.

    Returns:
        The agent's final result text.

    Raises:
        EnvironmentError:  If ANTHROPIC_API_KEY is not set.
        FileNotFoundError: If data_path does not exist.
        RuntimeError:      If the agent produces no result.
    """
    configure_logging(log_level)
    set_log_path(audit_log or AUDIT_LOG_PATH)

    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise EnvironmentError(
            "ANTHROPIC_API_KEY is not set. "
            "Add it to .env.local or export it in your shell."
        )

    # Build prompt with data file hint
    prompt = user_prompt
    if data_path:
        abs_data = Path(data_path).resolve()
        if not abs_data.exists():
            raise FileNotFoundError(f"Data file not found: {data_path}")
        prompt = (
            f"{user_prompt}\n\n"
            f"Data file to analyse: {abs_data}\n"
            f"Tip: import to SQLite then use query_sqlite for complex queries:\n"
            f"    from tools.data_tools import csv_to_sqlite\n"
            f"    csv_to_sqlite('{abs_data}', 'data/analysis.db')"
        )

    # Assemble MCP servers: always include the in-process SQLite tool;
    # merge in any external servers from mcp_servers.json.
    mcp_servers: dict = {"sqlite": build_sqlite_mcp_server()}
    mcp_servers.update(load_mcp_servers())

    options = ClaudeAgentOptions(
        cwd=str(PROJECT_ROOT),
        allowed_tools=["Read", "Glob", "Grep", "Bash", "Write"],
        permission_mode="acceptEdits",
        system_prompt=SYSTEM_PROMPT,
        max_turns=max_turns,
        max_budget_usd=max_budget_usd,
        mcp_servers=mcp_servers,
        hooks={
            "PreToolUse": PRE_TOOL_USE_HOOKS,
            "PostToolUse": POST_TOOL_USE_HOOKS,
        },
    )

    logger.info(
        "agent.start",
        extra={
            "max_turns": max_turns,
            "max_budget_usd": max_budget_usd,
            "mcp_servers": list(mcp_servers.keys()),
            "prompt_preview": user_prompt[:120],
        },
    )
    t0 = time.monotonic()
    result_text: str | None = None
    session_id: str = ""

    try:
        async with ClaudeSDKClient(options=options) as client:
            await client.query(prompt)

            async for message in client.receive_response():
                if isinstance(message, SystemMessage) and message.subtype == "init":
                    session_id = message.data.get("session_id", "")
                    logger.info("agent.session", extra={"session_id": session_id})

                elif isinstance(message, AssistantMessage):
                    for block in message.content:
                        if isinstance(block, TextBlock) and block.text.strip():
                            logger.debug("agent.chunk", extra={"chars": len(block.text)})

                elif isinstance(message, ResultMessage):
                    result_text = message.result
                    logger.info(
                        "agent.result",
                        extra={
                            "session_id": session_id,
                            "chars": len(result_text),
                            "stop_reason": getattr(message, "stop_reason", ""),
                        },
                    )

    except Exception as exc:
        logger.error(
            "agent.error",
            extra={"session_id": session_id, "error": str(exc)},
            exc_info=True,
        )
        raise

    finally:
        elapsed = time.monotonic() - t0
        logger.info(
            "agent.complete",
            extra={"session_id": session_id, "duration_s": f"{elapsed:.1f}"},
        )

    if result_text is None:
        raise RuntimeError("Agent finished without producing a result message.")

    return result_text


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="agent",
        description="CSV Data Analyst Agent powered by Claude Agent SDK",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("prompt", help="Analysis task or question")
    p.add_argument("--data", metavar="PATH", help="CSV or SQLite file to analyse")
    p.add_argument(
        "--max-turns",
        type=int,
        default=DEFAULT_MAX_TURNS,
        metavar="N",
        help="Maximum agent turns",
    )
    p.add_argument(
        "--budget",
        type=float,
        default=DEFAULT_MAX_BUDGET_USD,
        metavar="USD",
        dest="max_budget_usd",
        help="Maximum spend in USD",
    )
    p.add_argument(
        "--output",
        metavar="PATH",
        default=None,
        help="Save result to this file (default: reports/<data_stem>.md when --data is given)",
    )
    p.add_argument(
        "--audit-log",
        metavar="PATH",
        default=None,
        help="Override audit log path (default: logs/audit.jsonl)",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    return p


def main() -> None:
    args = _build_parser().parse_args()
    try:
        async def _run():
            return await run_agent(
                args.prompt,
                args.data,
                max_turns=args.max_turns,
                max_budget_usd=args.max_budget_usd,
                log_level=args.log_level,
                audit_log=args.audit_log,
            )

        result = anyio.run(_run)
    except (EnvironmentError, FileNotFoundError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    except RuntimeError as exc:
        print(f"Agent error: {exc}", file=sys.stderr)
        sys.exit(2)

    # Determine output path
    out_path: Path | None = None
    if args.output:
        out_path = Path(args.output)
    elif args.data:
        out_path = PROJECT_ROOT / "reports" / (Path(args.data).stem + ".md")

    if out_path:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(result)
        print(f"Report saved to {out_path}", file=sys.stderr)

    print("\n=== Agent Result ===\n")
    print(result)


if __name__ == "__main__":
    main()
