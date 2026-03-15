"""Pre-tool-use safety hook: blocks dangerous Bash commands.

Returning {"decision": "block", "reason": "..."} from a PreToolUse hook
prevents the tool from running and feeds the reason back to the model.
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Blocklist — (human-readable label, compiled pattern)
# Patterns are matched case-insensitively against the full command string.
# ---------------------------------------------------------------------------
_BLOCKLIST: list[tuple[str, re.Pattern[str]]] = [
    # Recursive force-delete of sensitive paths
    (
        "recursive force-delete",
        re.compile(r"\brm\s+(-\w*[rR]\w*[fF]\w*|-\w*[fF]\w*[rR]\w*)\s*/", re.I),
    ),
    # Any rm -rf that looks like it targets broad paths
    (
        "rm -rf broad target",
        re.compile(
            r"\brm\s+-[a-z]*r[a-z]*f[a-z]*\s+(~|\.\.?/?|/[a-z]{1,4}/?)\s*$", re.I | re.M
        ),
    ),
    # Piping curl/wget output directly into a shell
    (
        "remote code execution via pipe",
        re.compile(r"\b(curl|wget)\b.+\|\s*(ba)?sh\b", re.I | re.S),
    ),
    # Privilege escalation
    (
        "sudo / su usage",
        re.compile(r"\b(sudo|su)\b", re.I),
    ),
    # Writing directly to block devices
    (
        "direct disk write (dd/mkfs)",
        re.compile(r"\b(dd\b.+of=/dev/|mkfs\b)", re.I),
    ),
    # Fork bomb
    (
        "fork bomb",
        re.compile(r":\(\)\s*\{", re.I),
    ),
    # Kill all processes
    (
        "kill all processes",
        re.compile(r"\bkill\s+-9\s+-1\b", re.I),
    ),
    # Overwriting /etc files directly
    (
        "overwrite /etc",
        re.compile(r">\s*/etc/", re.I),
    ),
    # System shutdown/reboot
    (
        "shutdown / reboot / halt",
        re.compile(r"\b(shutdown|reboot|halt|poweroff)\b", re.I),
    ),
    # chmod 777 applied broadly
    (
        "broad chmod 777",
        re.compile(r"\bchmod\b.+777\s+(/|\.|~)", re.I),
    ),
]


async def dangerous_bash_hook(
    input_data: dict,
    tool_use_id: str,
    context: dict,
) -> dict:
    """PreToolUse hook — intercepts Bash calls and blocks dangerous commands.

    Returns ``{"decision": "block", "reason": "..."}`` to prevent execution,
    or ``{}`` to allow it.
    """
    command: str = input_data.get("tool_input", {}).get("command", "")

    for label, pattern in _BLOCKLIST:
        if pattern.search(command):
            reason = (
                f"Blocked: command matched safety rule '{label}'. "
                "If this operation is genuinely required, ask the user to run it manually."
            )
            logger.warning(
                "safety.blocked",
                extra={
                    "tool_use_id": tool_use_id,
                    "rule": label,
                    "command_preview": command[:200],
                    "agent_id": input_data.get("agent_id", ""),
                },
            )
            return {"decision": "block", "reason": reason}

    logger.debug(
        "safety.allowed",
        extra={
            "tool_use_id": tool_use_id,
            "command_preview": command[:80],
        },
    )
    return {}


# ---------------------------------------------------------------------------
# Helpers for testing
# ---------------------------------------------------------------------------

def check_command(command: str) -> tuple[bool, str]:
    """Synchronous helper: returns (is_dangerous, matched_rule_label).

    Useful in tests and for manual inspection without running the async hook.
    """
    for label, pattern in _BLOCKLIST:
        if pattern.search(command):
            return True, label
    return False, ""
