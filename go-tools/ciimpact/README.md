# CI impact planner

From `go-tools`, run:

```sh
go run ./cmd/ci-impact --root .. --base "$BASE_SHA" --head "$HEAD_SHA" \
  --output impact.json --github-output "$GITHUB_OUTPUT" --summary "$GITHUB_STEP_SUMMARY"
```

Use the PR merge base for PRs, and the last verified published catalog revision for a release. The caller must establish the baseline's provenance. Both IDs must resolve to commits in complete history, and base must be an ancestor of head. Output uses full commit IDs. A missing commit, shallow checkout, unresolved local import or an unreviewed Dockerfile fails the command; never interpret failure as an empty plan.

The schema version 1 JSON contains sorted `modules`, `targets`, `fixture_targets`, per-target `source_fingerprints`, `checks`, `integration_required`, `reasons` and `fallback_full`. GitHub output includes compact JSON arrays for the three selections, Boolean `integration_required`, `fallback_full`, and `check_` followed by each check name. Summary and GitHub files are appended; JSON output is replaced. Treat reasons as informational, not as an execution contract.

Go imports are parsed conservatively from all build-tag variants in both revisions, including test imports for reverse test selection. Runtime reachability is separate from test selection. Fingerprints include runtime package inputs, embedded-resource directories, module manifests, final web assets, build recipe and frontend build inputs. An explicit SHA-256 guard on each Dockerfile forces catalog review when the recipes change. Unknown fixture and non-Go inputs fall back to full checks. Document-only paths are handled separately. Fallback is conservative and may run extra jobs; an error blocks reuse instead of guessing.

Mutable upstream base images are not resolved by source fingerprints. Updating them requires an explicit rebuild decision. Fingerprints describe source inputs, not the resulting image identity or acceptance of a component combination.

When a Dockerfile changes, inspect every stage, COPY, generated asset, dependency and toolchain reference for both old and new recipes. Update the image input graph and tests to conservatively cover every still-supported baseline and the new recipe; then append the new SHA-256 to `reviewedRecipes` while retaining prior reviewed hashes. A previously published baseline must remain calculable with the updated graph. Unreviewed hashes continue to fail closed, and a common core Dockerfile change selects all core targets. Removing an old hash requires first proving no supported publication baseline needs it.

`workflowInputs` mirrors the dedicated workflow path triggers; update it alongside any trigger changes. `sharedInputs` records direct cross-module file reads found during the audit, including editorial parity fixtures, scheduler migration and managed-setup grants consumed by domain tests, and configuration files read by tools tests. An unknown shared fixture outside the explicitly handled operational snapshot falls back to all checks.

`image_changed_browser` is a separate Boolean in JSON and GitHub output: it selects a browser image build for changes to the reviewed browser Dockerfile, its copied browser directory and scripts, or the root `.dockerignore`. `check_browser` also covers contract tests and composition inputs, which do not by themselves change the image. Browser remains outside the core image and extractor fingerprint catalog. The browser Dockerfile is reviewed and versioned in the same recipe registry.

The tools config catalog scans Go source across modules for environment keys. A changed runtime Go file with a potential uppercase environment key selects tools tests, in addition to the explicitly listed file consumers.

The core and extractor source fingerprints also cover `scripts/ci-core.py` and `.github/workflows/docker-build-push.yml`, which control build invocation and arguments. A change to either selects all three core targets and the extractor fixture. `scripts/test_ci_core.py`, `scripts/ci-baseline.sh`, and the artifact utilities are CI controls and require integration checks, without independently selecting runtime images. The extractor workflow is part of the extractor fingerprint; changes to the shared-browser workflow select a browser build because it controls that build's invocation.

Git snapshots containing symlinks or gitlinks are rejected before archive parsing, including when an object was deleted in the head but remains in the baseline. This intentionally blocks planning until their image and test semantics can be reviewed. The planner never follows a worktree symlink.
