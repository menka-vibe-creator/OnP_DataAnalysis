"""Agent hooks for safety enforcement and audit logging.

Hooks are registered in agent.py via ClaudeAgentOptions.hooks.
Import the pre-built HookMatcher lists from this module:

    from hooks import PRE_TOOL_USE_HOOKS, POST_TOOL_USE_HOOKS
"""

from hooks.audit import post_audit_hook, pre_timing_hook
from hooks.safety import dangerous_bash_hook

from claude_agent_sdk import HookMatcher

# PreToolUse: block dangerous Bash commands + record start times for audit
PRE_TOOL_USE_HOOKS: list[HookMatcher] = [
    HookMatcher(matcher="Bash", hooks=[dangerous_bash_hook]),
    HookMatcher(matcher=".*", hooks=[pre_timing_hook]),
]

# PostToolUse: write every tool call to the JSON audit log
POST_TOOL_USE_HOOKS: list[HookMatcher] = [
    HookMatcher(matcher=".*", hooks=[post_audit_hook]),
]
