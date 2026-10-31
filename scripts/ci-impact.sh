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
    default_branch = event.get("repository", {}).get("default_branch", "")
elif event_name == "push":
    head = event.get("after", "")
    base = event.get("before", "")
    default_branch = event.get("repository", {}).get("default_branch", "")
else:
    head = ""
    base = ""
    default_branch = ""
print((head or "-") + "\t" + (base or "-") + "\t" + (default_branch or "-"))
PYTHON
)"
event_head="${event_values%%$'\t'*}"
event_rest="${event_values#*$'\t'}"
event_base="${event_rest%%$'\t'*}"
event_default_branch="${event_rest#*$'\t'}"
[[ "$event_head" == "-" ]] && event_head=""
[[ "$event_base" == "-" ]] && event_base=""
[[ "$event_default_branch" == "-" ]] && event_default_branch=""
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

if [[ "$event_name" == "push" && -z "${CI_IMPACT_BASE:-}" ]]; then
  before_sha="${event_base:-}"
  before_is_ancestor=false
  if [[ "$before_sha" =~ ^[0-9a-f]{40}$ ]] && [[ ! "$before_sha" =~ ^0+$ ]]; then
    if git merge-base --is-ancestor "$before_sha" "$head_sha" 2>/dev/null; then
      before_is_ancestor=true
    fi
  fi
  if [[ "$before_is_ancestor" != true ]]; then
    if [[ -z "${event_default_branch:-}" ]] || ! git check-ref-format --branch "$event_default_branch" >/dev/null 2>&1; then
      echo "CI impact: default branch is missing or invalid for push fallback" >&2
      exit 2
    fi
    default_ref="refs/remotes/origin/$event_default_branch"
    default_sha="$(git rev-parse --verify "$default_ref^{commit}" 2>/dev/null || true)"
    if [[ ! "$default_sha" =~ ^[0-9a-f]{40}$ ]]; then
      default_ref="refs/heads/$event_default_branch"
      default_sha="$(git rev-parse --verify "$default_ref^{commit}" 2>/dev/null || true)"
    fi
    if [[ ! "$default_sha" =~ ^[0-9a-f]{40}$ ]]; then
      echo "CI impact: declared default branch is unavailable locally" >&2
      exit 2
    fi
    base_sha="$(git merge-base "$default_sha" "$head_sha" 2>/dev/null || true)"
    if [[ ! "$base_sha" =~ ^[0-9a-f]{40}$ || "$base_sha" == "$head_sha" ]]; then
      echo "CI impact: default branch merge-base is unavailable or empty" >&2
      exit 2
    fi
  fi
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
