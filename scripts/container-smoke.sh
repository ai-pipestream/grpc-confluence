#!/usr/bin/env bash
# Boot a built release image without host ports or real upstream credentials.
# Requires Docker and GNU timeout. Helper images may be overridden with digests.
# Usage: scripts/container-smoke.sh MODE IMAGE
set -euo pipefail
mode=${1:-}
image=${2:-}
case "$mode" in
  confluence) ;;
  *) echo 'Usage: scripts/container-smoke.sh confluence IMAGE' >&2; exit 2 ;;
esac
[[ -n "$image" && $# == 2 ]] || { echo 'An image reference is required' >&2; exit 2; }
command -v timeout >/dev/null
probe_image=${GRPCURL_IMAGE:-fullstorydev/grpcurl:v1.9.3}
prefix="split-smoke-${mode}-$(date +%s)-$$-${RANDOM}"
network="$prefix"
server="$prefix-server"
probe="$prefix-probe"
created_network=false
containers=()
# Bound every Docker operation as well as each RPC; no published host ports.
d() { timeout --kill-after=5s 30s docker "$@"; }
cleanup() {
  result=$?
  trap - EXIT
  if (( result != 0 )); then
    d logs --tail 80 "$server" >&2 || true
  fi
  for container in "${containers[@]}"; do
    if d container inspect "$container" >/dev/null 2>&1; then
      if ! d rm -f "$container" >/dev/null; then
        echo "Cleanup failed for owned container $container" >&2
        result=1
      fi
    fi
  done
  if $created_network && ! d network rm "$network" >/dev/null; then
    echo "Cleanup failed for owned network $network" >&2
    result=1
  fi
  printf 'container-smoke mode=%s exit=%s\n' "$mode" "$result"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
ensure_image() {
  if ! d image inspect "$1" >/dev/null 2>&1; then timeout --kill-after=5s 180s docker pull "$1"; fi
  d image inspect --format 'helper={{.Id}} repoDigests={{json .RepoDigests}}' "$1"
}
d image inspect --format 'image={{.Id}} repoDigests={{json .RepoDigests}}' "$image"
ensure_image "$probe_image"
created_network=true
d network create --internal "$network" >/dev/null
runtime=(--network "$network" --read-only --tmpfs /tmp:rw,exec,nosuid,size=128m
         --cap-drop ALL --security-opt no-new-privileges --cpus 2 --memory 768m
         -e JAVA_TOOL_OPTIONS=-XX:ActiveProcessorCount=2)
env_args=()
case "$mode" in
  confluence)
    port=9095
    env_args=(-e CONFLUENCE_BASE_URL=http://unused.invalid/wiki
              -e CONFLUENCE_EMAIL=smoke@example.invalid -e CONFLUENCE_API_TOKEN=smoke-only)
    ;;
esac
containers+=("$server" "$probe")
d run -d --name "$server" --network-alias service "${runtime[@]}" "${env_args[@]}" "$image" >/dev/null
ready=false
for attempt in {1..30}; do
  [[ $(d inspect --format '{{.State.Running}}' "$server") == true ]] || {
    echo 'Service exited before readiness' >&2; exit 1;
  }
  if response=$(d run --name "$probe" --network "$network" --read-only --cap-drop ALL \
      --security-opt no-new-privileges --cpus 1 --memory 128m "$probe_image" \
      -plaintext -connect-timeout 2 -max-time 4 -d '{}' "service:$port" grpc.health.v1.Health/Check 2>&1) \
      && [[ "$response" == *'"SERVING"'* ]]; then
    printf '%s\n' "$response"
    ready=true; break
  fi
  printf 'readiness attempt %s: %s\n' "$attempt" "$response" >&2
  d rm -f "$probe" >/dev/null 2>&1 || true
  sleep 1
done
$ready || { echo 'Service did not become ready within 30 attempts' >&2; exit 1; }
d inspect --format 'runtime image={{.Image}} running={{.State.Running}} user={{.Config.User}} readonly={{.HostConfig.ReadonlyRootfs}} ports={{json .HostConfig.PortBindings}}' "$server"
d logs --tail 20 "$server"
