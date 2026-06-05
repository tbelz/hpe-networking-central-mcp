#!/usr/bin/env bash
set -euo pipefail

IMAGE="${1:?image name required}"
REPO="${2:-${GITHUB_REPOSITORY:-tbelz/hpe-networking-central-mcp}}"

VOL="central-mcp-v2-smoke-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}-$$"
OUT="$(mktemp)"
ERR="$(mktemp)"

cleanup() {
  docker volume rm -f "$VOL" >/dev/null || true
  rm -f "$OUT" "$ERR"
}
trap cleanup EXIT

docker volume create "$VOL" >/dev/null

LATEST_KNOWLEDGE_TAG="$(python3 - "$REPO" <<'PY'
import json
import sys
import urllib.request

repo = sys.argv[1]
with urllib.request.urlopen(
    f"https://api.github.com/repos/{repo}/releases/latest",
    timeout=15,
) as response:
    print(json.load(response)["tag_name"])
PY
)"

docker run --rm --entrypoint sh -v "$VOL:/data" "$IMAGE" -c "
  cat > /data/manifest.json <<EOF
{\"release_tag\":\"$LATEST_KNOWLEDGE_TAG\",\"schema_version\":10,\"knowledge_asset\":\"knowledge_db_compiler.tar.gz\",\"knowledge_archive_member\":\"knowledge_db_compiler\",\"knowledge_projection\":\"v2\"}
EOF
  printf 'not a ladybug database' > /data/graph_db
  printf 'stale corrupt wal' > /data/graph_db.wal
"

TIMEOUT_CMD=()
if command -v timeout >/dev/null 2>&1; then
  TIMEOUT_CMD=(timeout 240)
elif command -v gtimeout >/dev/null 2>&1; then
  TIMEOUT_CMD=(gtimeout 240)
fi

set +e
if [ "${#TIMEOUT_CMD[@]}" -gt 0 ]; then
  (
    printf '%s\n' '{"jsonrpc":"2.0","method":"initialize","id":1,"params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"v2-smoke","version":"0.1"}}}'
    printf '%s\n' '{"jsonrpc":"2.0","method":"notifications/initialized"}'
    printf '%s\n' '{"jsonrpc":"2.0","method":"tools/list","id":2,"params":{}}'
    sleep 10
  ) | "${TIMEOUT_CMD[@]}" docker run -i --rm \
    -v "$VOL:/data" \
    -e KNOWLEDGE_RELEASE_REPO="$REPO" \
    -e MCP_KNOWLEDGE_PROJECTION=v2 \
    -e MCP_COMPILER_TOOLS=true \
    "$IMAGE" >"$OUT" 2>"$ERR"
else
  (
    printf '%s\n' '{"jsonrpc":"2.0","method":"initialize","id":1,"params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"v2-smoke","version":"0.1"}}}'
    printf '%s\n' '{"jsonrpc":"2.0","method":"notifications/initialized"}'
    printf '%s\n' '{"jsonrpc":"2.0","method":"tools/list","id":2,"params":{}}'
    sleep 10
  ) | docker run -i --rm \
    -v "$VOL:/data" \
    -e KNOWLEDGE_RELEASE_REPO="$REPO" \
    -e MCP_KNOWLEDGE_PROJECTION=v2 \
    -e MCP_COMPILER_TOOLS=true \
    "$IMAGE" >"$OUT" 2>"$ERR"
fi
status=$?
set -e

if [ "$status" -ne 0 ] && [ "$status" -ne 124 ]; then
  echo "v2 smoke container failed with status $status" >&2
  echo "--- stderr ---" >&2
  cat "$ERR" >&2
  echo "--- stdout ---" >&2
  cat "$OUT" >&2
  exit "$status"
fi

python3 - "$OUT" "$ERR" <<'PY'
import json
import sys
from pathlib import Path

out_path = Path(sys.argv[1])
err_path = Path(sys.argv[2])
tools = None
for line in out_path.read_text().splitlines():
    try:
        message = json.loads(line)
    except json.JSONDecodeError:
        continue
    if message.get("id") == 2:
        tools = {tool["name"] for tool in message.get("result", {}).get("tools", [])}
        break

expected = {
    "find_api_endpoints",
    "get_api_endpoint_context",
    "get_api_schema_context",
    "get_openapi_source_detail",
    "get_compiler_graph_health",
}
if tools is None:
    print("No tools/list response found", file=sys.stderr)
    print("--- stderr ---", file=sys.stderr)
    print(err_path.read_text(), file=sys.stderr)
    print("--- stdout ---", file=sys.stderr)
    print(out_path.read_text(), file=sys.stderr)
    raise SystemExit(1)

missing = expected - tools
if missing:
    print(f"Missing compiler tools: {sorted(missing)}", file=sys.stderr)
    print(f"Registered tools: {sorted(tools)}", file=sys.stderr)
    print("--- stderr ---", file=sys.stderr)
    print(err_path.read_text(), file=sys.stderr)
    raise SystemExit(1)

stderr = err_path.read_text()
if "knowledge_db_refresh_forced" not in stderr:
    print("Expected forced DB refresh recovery log was not emitted", file=sys.stderr)
    print(stderr, file=sys.stderr)
    raise SystemExit(1)
if "Corrupted wal file" in stderr.split("knowledge_db_reinstall_recovered")[-1]:
    print("WAL corruption persisted after recovery", file=sys.stderr)
    print(stderr, file=sys.stderr)
    raise SystemExit(1)

print("v2 compiler smoke profile recovered persisted DB and registered compiler tools")
PY
