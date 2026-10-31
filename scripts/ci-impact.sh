#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
head_sha="${CI_IMPACT_HEAD:-}"
base_sha="${CI_IMPACT_BASE:-}"

event_name="${GITHUB_EVENT_NAME:-}"
if [[ -z "$head_sha" && "$event_name" == "workflow_dispatch" ]]; then
  head_sha="${GITHUB_SHA:-}"
fi

if [[ -z "$head_sha" || ( -z "$base_sha" && "$event_name" != "workflow_dispatch" ) ]]; then
  if [[ -z "${GITHUB_EVENT_PATH:-}" || ! -f "$GITHUB_EVENT_PATH" ]]; then
    echo "CI impact: missing GitHub event payload" >&2
    exit 2
  fi
  event_values="$(python3 - "$event_name" "$GITHUB_EVENT_PATH" <<'PYTHON'
import json
import sys

event_name, event_path = sys.argv[1:]
with open(event_path, encoding="utf-8") as source:
    event = json.load(source)
if event_name == "pull_request":
    pull = event.get("pull_request", {})
    head = pull.get("head", {}).get("sha", "")
    base = pull.get("base", {}).get("sha", "")
elif event_name == "push":
    head = event.get("after", "")
    base = event.get("before", "")
else:
    head = ""
    base = ""
print(head + "\t" + base)
PYTHON
)"
event_head="${event_values%%$'\t'*}"
event_base="${event_values#*$'\t'}"
  [[ -n "$head_sha" ]] || head_sha="$event_head"
  [[ -n "$base_sha" ]] || base_sha="$event_base"
fi

if [[ ! "$head_sha" =~ ^[0-9a-f]{40}$ ]]; then
  echo "CI impact: head must be a full commit SHA" >&2
  exit 2
fi

if [[ "$event_name" == "pull_request" && -z "${CI_IMPACT_BASE:-}" ]]; then
  if [[ ! "${event_base:-}" =~ ^[0-9a-f]{40}$ ]]; then
    echo "CI impact: pull request base must be a full commit SHA" >&2
    exit 2
  fi
  base_sha="$(git merge-base "$event_base" "$head_sha")"
fi

if [[ -z "$base_sha" ]]; then
  case "$event_name" in
    push)
      base_sha="$event_base"
      ;;
    workflow_dispatch)
      if [[ "${GITHUB_REF:-}" != "refs/heads/main" ]]; then
        echo "CI impact: workflow_dispatch is restricted to refs/heads/main" >&2
        exit 2
      fi
      base_sha="$(git rev-parse "$head_sha^" 2>/dev/null || true)"
      ;;
    *)
      echo "CI impact: unsupported event ${event_name:-<unset>}" >&2
      exit 2
      ;;
  esac
fi

if [[ ! "$base_sha" =~ ^[0-9a-f]{40}$ ]]; then
  echo "CI impact: base must be a full commit SHA" >&2
  exit 2
fi

if [[ ! "$base_sha" =~ [1-9a-f] ]]; then
  echo "CI impact: baseline SHA is all zeroes" >&2
  exit 2
fi

if [[ -z "${GITHUB_OUTPUT:-}" || -z "${GITHUB_STEP_SUMMARY:-}" || -z "${RUNNER_TEMP:-}" ]]; then
  echo "CI impact: GitHub output paths are required" >&2
  exit 2
fi

cd "$repo_root/go-tools"
go run ./cmd/ci-impact \
  --root .. \
  --base "$base_sha" \
  --head "$head_sha" \
  --output "$RUNNER_TEMP/ci-impact.json" \
  --github-output "$GITHUB_OUTPUT" \
  --summary "$GITHUB_STEP_SUMMARY"
