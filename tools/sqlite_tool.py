"""Custom SQLite query tool registered as an in-process MCP tool.

The agent can call `query_sqlite` to run read-only SELECT queries against any
SQLite database file and receive results formatted as a Markdown table.

Usage (internal):
    from tools.sqlite_tool import build_sqlite_mcp_server
    server = build_sqlite_mcp_server()
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from claude_agent_sdk import tool, create_sdk_mcp_server

logger = logging.getLogger(__name__)

# Maximum rows returned to avoid flooding the context window
_MAX_ROWS = 500


async def run_query_sqlite(args: dict) -> dict:
    """Core logic for the query_sqlite tool — directly testable.

    Accepts ``{"db_path": str, "sql": str}`` and returns an MCP content dict.
    """
    db_path: str = args.get("db_path", "").strip()
    sql: str = args.get("sql", "").strip()

    def _err(msg: str) -> dict:
        logger.warning("query_sqlite.rejected", extra={"db": db_path, "reason": msg})
        return {"content": [{"type": "text", "text": f"Error: {msg}"}]}

    # --- Validation -------------------------------------------------------
    if not db_path:
        return _err("db_path is required")
    if not sql:
        return _err("sql is required")
    if not sql.upper().startswith("SELECT"):
        return _err("only SELECT queries are allowed")

    path = Path(db_path)
    if not path.exists():
        return _err(f"database file not found: {db_path}")
    if not path.is_file():
        return _err(f"path is not a file: {db_path}")

    # --- Execute -----------------------------------------------------------
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(sql)
            rows = cursor.fetchall()
            columns = [d[0] for d in cursor.description] if cursor.description else []

        if not rows:
            logger.info(
                "query_sqlite.empty",
                extra={"db": db_path, "sql_preview": sql[:120]},
            )
            return {"content": [{"type": "text", "text": "Query returned no rows."}]}

        # Build Markdown table (capped)
        truncated = len(rows) > _MAX_ROWS
        display_rows = rows[:_MAX_ROWS]

        header = "| " + " | ".join(columns) + " |"
        separator = "| " + " | ".join("---" for _ in columns) + " |"
        body = "\n".join(
            "| " + " | ".join(_cell(row[col]) for col in columns) + " |"
            for row in display_rows
        )
        table = "\n".join([header, separator, body])
        if truncated:
            table += f"\n\n_Showing first {_MAX_ROWS} of {len(rows)} rows._"

        logger.info(
            "query_sqlite.ok",
            extra={"db": db_path, "rows": len(rows), "truncated": truncated},
        )
        return {"content": [{"type": "text", "text": table}]}

    except sqlite3.OperationalError as exc:
        logger.error("query_sqlite.sql_error", extra={"db": db_path, "error": str(exc)})
        return _err(f"SQL error — {exc}")
    except sqlite3.DatabaseError as exc:
        logger.error(
            "query_sqlite.db_error", extra={"db": db_path, "error": str(exc)}
        )
        return _err(f"Database error — {exc}")


# The @tool decorator turns the function into an SdkMcpTool object (not
# directly callable).  Tests use run_query_sqlite(); the agent uses this.
@tool(
    "query_sqlite",
    (
        "Execute a read-only SQL SELECT query against a SQLite database file. "
        "Returns results as a Markdown table. "
        "Only SELECT statements are permitted. "
        "Parameters: db_path (str) — path to the .db file; "
        "sql (str) — the SELECT query to run."
    ),
    {"db_path": str, "sql": str},
)
async def query_sqlite(args: dict) -> dict:  # noqa: F811 — intentional re-use of name
    return await run_query_sqlite(args)


def _cell(value: object) -> str:
    """Format a single cell value for a Markdown table (escapes pipes)."""
    return str(value).replace("|", "\\|") if value is not None else ""


def build_sqlite_mcp_server():
    """Return an in-process MCP server that exposes the query_sqlite tool."""
    return create_sdk_mcp_server("sqlite-tools", tools=[query_sqlite])
