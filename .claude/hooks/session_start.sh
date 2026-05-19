#!/usr/bin/env bash
# session_start.sh — fires on Claude Code SessionStart event.
#
# Job: print cwd / branch / Claude config dir / git committer identity
# and the expected mapping per .claude/rules/git-workflow.md, so main
# Claude can verify alignment on the first turn before any tool call.
#
# Hooks cannot invoke Claude tools, so this hook only prints. Stdout
# becomes part of session-start context.

set -euo pipefail

PROJECT_DIR="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"
CWD="$(pwd)"
BRANCH="$(git -C "$PROJECT_DIR" branch --show-current 2>/dev/null || echo '?')"
CONFIG_DIR="${CLAUDE_CONFIG_DIR:-$HOME/.claude}"
GIT_NAME="$(git -C "$PROJECT_DIR" config --get user.name 2>/dev/null || echo '(unset)')"
GIT_EMAIL="$(git -C "$PROJECT_DIR" config --get user.email 2>/dev/null || echo '(unset)')"

echo "=== chili SessionStart (identity) ==="
echo "cwd:        $CWD"
echo "branch:     $BRANCH"
echo "claude dir: $CONFIG_DIR"
echo "committer:  $GIT_NAME <$GIT_EMAIL>"
echo ""
echo "Expected mapping (.claude/rules/git-workflow.md):"
echo "  ~/team/oak/chili      oak     ~/.claude-team   Oak Claude     <oak-claude@chili.local>"
echo "  ~/team/sid/chili      sid     ~/.claude-team   Sid Claude     <sid-claude@chili.local>"
echo "  ~/team/sam/chili      sam     ~/.claude-team   Sam Claude     <sam-claude@chili.local>"
echo "  ~/team/balaji/chili   balaji  ~/.claude-team   Balaji Claude  <balaji-claude@chili.local>"
echo "  ~/code/chili          claude  ~/.claude        Claude Code    <claude-code@chili.local>"
echo ""
echo "If cwd / branch / config-dir / committer do not all agree with one row above, STOP and tell the user before any tool call."
