#!/usr/bin/env python3
"""Selective core CI orchestration. Credentials stay in tool environments."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

TARGETS = ("web", "background", "executor")
TAGS = {"web": "evening-news-web:consolidation-fixture",
        "background": "evening-news-background:consolidation-fixture",
        "executor": "evening-news-executor:consolidation-fixture"}
FIXTURE_TAG = "evening-news-collector:consolidation-fixture"
SHA = re.compile(r"^[0-9a-f]{40}$")
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


class Failure(Exception):
    pass


def run(args, *, env=None):
    p = subprocess.run([str(a) for a in args], capture_output=True, env=env)
    if p.returncode:
        # Tool output may include registry diagnostics. Do not echo it.
        raise Failure("command_failed:" + Path(str(args[0])).name)
    return p.stdout


def tool(name, *args):
    return run([Path(os.environ["NEWS_CI_TOOLS"]) / name, *args])


def read(path):
    path = Path(path)
    if path.stat().st_size > 2 << 20:
        raise Failure("metadata_too_large")
    return json.loads(path.read_text())


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x") as f:
        json.dump(value, f, sort_keys=True, indent=2)
        f.write("\n")


def key(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def outputs(**values):
    if not os.environ.get("GITHUB_OUTPUT"):
        return
    with open(os.environ["GITHUB_OUTPUT"], "a") as f:
        for name, value in values.items():
            if not re.fullmatch(r"[a-z_]+", name):
                raise Failure("output_name_invalid")
            value = json.dumps(value, separators=(",", ":")) if not isinstance(value, str) else value
            if "\n" in value or "\r" in value:
                raise Failure("output_value_invalid")
            f.write(name + "=" + value + "\n")


def artifact_args(a):
    return ["--repository", os.environ["GITHUB_REPOSITORY"], "--root", a.root, "--head", a.head]


def find(a, kind, fingerprint=None):
    args = ["find", *artifact_args(a), "--kind", kind]
    if fingerprint:
        args += ["--key", fingerprint]
    return json.loads(tool("ci-artifacts", *args))


def download(a, meta, destination, kind="candidate", current_pr=False):
    args = ["download", *artifact_args(a), "--kind", kind,
            "--run-id", str(meta["run_id"]), "--name", meta["name"], "--output", destination]
    if str(meta["run_id"]) == os.environ.get("GITHUB_RUN_ID"):
        args += ["--current-run-id", str(meta["run_id"])]
        if current_pr:
            args += ["--allow-current-pr-only-for-gate"]
    tool("ci-artifacts", *args)


def inspect(ref):
    value = json.loads(run(["docker", "image", "inspect", ref]))
    if len(value) != 1:
        raise Failure("image_identity_ambiguous")
    v = value[0]
    if not DIGEST.fullmatch(v.get("Id", "")):
        raise Failure("image_identity_invalid")
    return {"imageID": v["Id"], "platform": v["Os"] + "/" + v["Architecture"],
            "revision": (v["Config"].get("Labels") or {}).get("org.opencontainers.image.revision", "")}


def normalized_baseline(a, raw, output):
    if raw.get("schemaVersion") == 2:
        write(output, raw)
        return raw
    if raw.get("schemaVersion") != 1:
        raise Failure("baseline_version_invalid")
    with tempfile.TemporaryDirectory(prefix="news-ci-baseline-") as d:
        d = Path(d)
        write(d / "raw.json", raw)
        rev = raw["revision"]
        tool("ci-impact", "--root", a.root, "--base", rev, "--head", rev, "--output", d / "impact.json")
        for target in TARGETS:
            image = raw["images"][target]
            expected_repo = "dzarlax-ai/" + {"web": "rss-web", "background": "rss-background", "executor": "rss-summarizer"}[target]
            if image["repository"] != expected_repo or not DIGEST.fullmatch(image["digest"]):
                raise Failure("baseline_image_invalid")
            run(["docker", "pull", "ghcr.io/" + image["repository"] + "@" + image["digest"]])
        tool("core-image-catalog", "normalize-v1", "--baseline", d / "raw.json",
             "--impact-plan", d / "impact.json", "--output", output)
    return read(output)


def baseline(a):
    raw = json.loads(tool("ci-artifacts", "baseline", *artifact_args(a)))
    with tempfile.TemporaryDirectory(prefix="news-ci-resolver-") as d:
        result = normalized_baseline(a, raw, Path(d) / "baseline.json")
    # Publisher invokes this resolver repeatedly while holding the publication lock.
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


def valid_reuse(bundle, impact, raw):
    return (read(bundle / "impact.json") == impact and
            read(bundle / "baseline.raw.json") == raw and
            read(bundle / "metadata.json").get("mode") == "catalog" and
            read(bundle / "catalog.json").get("revision") == impact["head"])


def reconcile_fingerprints(impact, raw):
    """A revised input graph must not reuse an older, differently hashed image."""
    if raw.get("schemaVersion") != 2:
        return impact
    selected = set(impact["targets"])
    for target in TARGETS:
        if raw["images"][target]["sourceFingerprint"] != impact["source_fingerprints"][target] and target not in selected:
            selected.add(target)
            impact["integration_required"] = True
            impact.setdefault("reasons", {}).setdefault(target, []).append("baseline_catalog_fingerprint_changed")
    impact["targets"] = sorted(selected)
    return impact


def summarize(title, value):
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as summary:
            summary.write("## " + title + "\n\n```json\n" + json.dumps(value, sort_keys=True, indent=2) + "\n```\n")


def pull_reused(old, targets):
    for target in TARGETS:
        if target not in targets:
            image = old["images"][target]
            ref = "ghcr.io/" + image["repository"] + "@" + image["digest"]
            run(["docker", "pull", ref])
            if inspect(ref) != {k: image[k] for k in ("imageID", "platform", "revision")}:
                raise Failure("reused_registry_identity_mismatch")
            run(["docker", "tag", ref, TAGS[target]])


def pr_full_fallback(out, source):
    for name in ("baseline.json", "baseline.raw.json", "impact.json"):
        (out / name).unlink(missing_ok=True)
    shutil.copyfile(source / "impact.json", out / "impact.json")
    return read(out / "impact.json")


def plan(a):
    out = Path(a.output)
    out.mkdir(parents=True, exist_ok=True)
    raw = None
    base = a.base
    if not a.pr:
        raw = json.loads(tool("ci-artifacts", "baseline", *artifact_args(a)))
        base = raw["revision"]
        write(out / "baseline.raw.json", raw)
    if not base or not SHA.fullmatch(base):
        raise Failure("base_required")
    args = ["--root", a.root, "--base", base, "--head", a.head, "--output", out / "impact.json"]
    tool("ci-impact", *args)
    impact = read(out / "impact.json")
    if raw is not None:
        impact = reconcile_fingerprints(impact, raw)
        (out / "impact.json").write_text(json.dumps(impact, sort_keys=True, indent=2) + "\n")
    summarize("Final CI selection", impact)
    required = bool(impact["integration_required"] or a.full or impact["targets"] or impact["fixture_targets"])
    if a.pr:
        required = impact["integration_required"]
    reuse = None
    if not a.pr and required:
        meta = find(a, "candidate")
        if meta["found"]:
            with tempfile.TemporaryDirectory(prefix="news-ci-reuse-") as d:
                bundle = Path(d) / "bundle"
                download(a, meta, bundle)
                if valid_reuse(bundle, impact, raw):
                    reuse = meta
    write(out / "selection.json", {"head": a.head, "pr": a.pr, "required": required,
                                    "reuse": reuse, "full": a.full})
    outputs(head_sha=a.head, base_sha=base, modules=impact["modules"], targets=impact["targets"],
            fixture_targets=impact["fixture_targets"], integration_required=impact["integration_required"],
            candidate_required=required, reuse_candidate=bool(reuse))


def build_core(targets, head):
    if not targets:
        return
    if len(set(targets)) != len(targets) or any(t not in TARGETS for t in targets):
        raise Failure("build_selection_invalid")
    env = dict(os.environ, REVISION=head, PLATFORM="linux/amd64")
    run(["docker", "buildx", "bake", "-f", "docker-bake.hcl", "--set", "*.platform=linux/amd64", "--load", *targets], env=env)


def build_fixture():
    # Preserve the existing fixture build version; it is independent of unrelated commits.
    run(["docker", "build", "--platform", "linux/amd64", "-f", "docker/Dockerfile.extractor", "-t", FIXTURE_TAG, "go"])


def verify_images(bundle):
    meta = read(bundle / "metadata.json")
    if meta["mode"] not in ("catalog", "legacy"):
        raise Failure("candidate_mode_invalid")
    for target in TARGETS:
        actual = inspect(TAGS[target])
        expected = meta["images"][target]
        if actual != expected or actual["platform"] != "linux/amd64":
            raise Failure("candidate_image_changed")
    if meta.get("fixture"):
        actual = inspect(FIXTURE_TAG)
        if actual["imageID"] != meta["fixture"]["imageID"] or actual["platform"] != "linux/amd64":
            raise Failure("fixture_image_changed")
    if meta["mode"] == "catalog":
        if key(read(bundle / "catalog.json")) != meta["key"]:
            raise Failure("catalog_key_mismatch")
        for target in TARGETS:
            image = read(bundle / "catalog.json")["images"][target]
            if {k: image[k] for k in ("imageID", "platform", "revision")} != meta["images"][target]:
                raise Failure("catalog_identity_mismatch")
    elif key({"head": meta["head"], "images": meta["images"]}) != meta["key"]:
        raise Failure("legacy_key_mismatch")
    return meta


def candidate(a):
    source = Path(a.plan)
    selection, impact = read(source / "selection.json"), read(source / "impact.json")
    out = Path(a.output)
    if selection["reuse"]:
        download(a, selection["reuse"], out)
        if not valid_reuse(out, impact, read(source / "baseline.raw.json")):
            raise Failure("reuse_binding_invalid")
        meta = read(out / "metadata.json")
        outputs(reused=True, candidate_run_id=str(selection["reuse"]["run_id"]),
                candidate_artifact=selection["reuse"]["name"], catalog_key=meta["key"])
        return
    out.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(source / "impact.json", out / "impact.json")
    legacy = False
    if selection["pr"]:
        # PR checks do not need registry access. Only their optional image gate
        # attempts reuse; an unavailable/unusable baseline falls back to an
        # isolated complete build that can never authorize publication.
        try:
            raw = json.loads(tool("ci-artifacts", "baseline", *artifact_args(a)))
            old = normalized_baseline(a, raw, out / "baseline.json")
            tool("ci-impact", "--root", a.root, "--base", raw["revision"], "--head", a.head,
                 "--output", out / "impact.json")
            impact = read(out / "impact.json")
            impact = reconcile_fingerprints(impact, raw)
            (out / "impact.json").write_text(json.dumps(impact, sort_keys=True, indent=2) + "\n")
            write(out / "baseline.raw.json", raw)
        except (Failure, OSError, ValueError, KeyError):
            legacy = True
            impact = pr_full_fallback(out, source)
    else:
        raw = read(source / "baseline.raw.json")
        write(out / "baseline.raw.json", raw)
        old = normalized_baseline(a, raw, out / "baseline.json")
    targets = list(TARGETS) if legacy else impact["targets"]
    if not legacy:
        try:
            pull_reused(old, targets)
        except (Failure, OSError, ValueError, KeyError):
            if not selection["pr"]:
                raise
            legacy = True
            impact = pr_full_fallback(out, source)
            targets = list(TARGETS)
    summarize("Actual candidate build selection", {
        "targets": targets, "baseline": impact["base"], "head": a.head,
        "mode": "legacy" if legacy else "catalog",
        "reasons": {"all": ["pr_baseline_unavailable_or_unusable"]} if legacy else impact.get("reasons", {}),
        "fixture_targets": ["extractor"] if legacy else impact["fixture_targets"],
    })
    build_core(targets, a.head)
    if legacy:
        catalog_key = key({"head": a.head, "images": {t: inspect(TAGS[t]) for t in TARGETS}})
    else:
        tool("core-image-catalog", "prepare", "--baseline", out / "baseline.json",
             "--impact-plan", out / "impact.json", "--output", out / "catalog.json")
        catalog_key = key(read(out / "catalog.json"))
    meta = {"mode": "legacy" if legacy else "catalog", "head": a.head, "key": catalog_key,
            "images": {t: inspect(TAGS[t]) for t in TARGETS}, "fixture": None}
    for image in meta["images"].values():
        if image["platform"] != "linux/amd64" or (legacy and image["revision"] != a.head):
            raise Failure("build_identity_invalid")
    tags = list(TAGS.values())
    if legacy or "extractor" in impact["fixture_targets"]:
        build_fixture()
        meta["fixture"] = {"imageID": inspect(FIXTURE_TAG)["imageID"], "sourceFingerprint": impact["source_fingerprints"]["extractor"]}
        tags.append(FIXTURE_TAG)
    write(out / "metadata.json", meta)
    run(["docker", "image", "save", "-o", out / "images.tar", *tags])
    outputs(reused=False, candidate_run_id=os.environ["GITHUB_RUN_ID"],
            candidate_artifact="core-candidate-" + a.head, catalog_key=catalog_key)


def restore(a, out):
    meta = {"run_id": a.run_id, "name": a.name}
    download(a, meta, out, current_pr=a.pr)
    run(["docker", "image", "load", "-i", out / "images.tar"])
    loaded = verify_images(out)
    if loaded["head"] != a.head:
        raise Failure("candidate_head_mismatch")
    if loaded["mode"] == "catalog":
        with tempfile.TemporaryDirectory(prefix="news-ci-verify-") as d:
            tool("core-image-catalog", "prepare", "--baseline", out / "baseline.json",
                 "--impact-plan", out / "impact.json", "--output", Path(d) / "catalog.json")
            if read(Path(d) / "catalog.json") != read(out / "catalog.json"):
                raise Failure("candidate_binding_changed")
    return loaded


def fixture(a, bundle, meta, output):
    expected = read(bundle / "impact.json")["source_fingerprints"]["extractor"]
    result = Path(output)
    result.mkdir(parents=True, exist_ok=False)
    if meta.get("fixture"):
        record = meta["fixture"]
        fresh = True
    else:
        found = find(a, "fixture", expected)
        if found["found"]:
            cached = result / "cached"
            download(a, found, cached, "fixture")
            record = read(cached / "fixture.json")
            run(["docker", "image", "load", "-i", cached / "fixture.tar"])
            fresh = False
        elif a.bootstrap_fixture:
            build_fixture()
            record = {"sourceFingerprint": expected, "imageID": inspect(FIXTURE_TAG)["imageID"]}
            fresh = True
        else:
            raise Failure("fixture_bootstrap_required")
    actual = inspect(FIXTURE_TAG)
    if record["sourceFingerprint"] != expected or record["imageID"] != actual["imageID"] or actual["platform"] != "linux/amd64":
        raise Failure("fixture_binding_invalid")
    write(result / "fixture.json", record)
    run(["docker", "image", "save", "-o", result / "fixture.tar", FIXTURE_TAG])
    return expected, fresh


def gate(a):
    out = Path(a.output)
    out.mkdir(parents=True, exist_ok=False)
    bundle = out / "candidate"
    meta = restore(a, bundle)
    fingerprint, fresh = fixture(a, bundle, meta, out / "fixture")
    if a.key != meta["key"]:
        raise Failure("gate_key_mismatch")
    for pg in ("16", "17"):
        run(["docker", "pull", "postgres:" + pg + "-alpine"])
    run(["docker", "pull", "alpine:3.21"])
    for pg in ("16", "17"):
        env = dict(os.environ, NEWS_FIXTURE_PG_MAJOR=pg)
        args = [Path(os.environ["NEWS_CI_TOOLS"]) / "vertical-fixture-gate", "--output-dir", out / ("acceptance-" + pg)]
        if meta["mode"] == "catalog":
            args += ["--catalog", bundle / "catalog.json"]
        run(args, env=env)
        run([Path(os.environ["NEWS_CI_TOOLS"]) / "managed-adoption-gate", "--postgres-major", pg, "--output-dir", out / ("managed-adoption-" + pg)])
    first, second = read(out / "acceptance-16/receipt.json"), read(out / "acceptance-17/receipt.json")
    if first != second:
        raise Failure("postgres_receipts_differ")
    verify_images(bundle)
    (out / "accepted").mkdir()
    write(out / "accepted/receipt.json", first)
    outputs(fixture_key=fingerprint, fixture_new=fresh, gate_artifact="core-acceptance-" + a.head + "-" + a.key)


def lookup_gate(a):
    if a.pr:
        result = {"found": False}
    else:
        result = find(a, "gate", a.key)
    outputs(found=result["found"], gate_run_id=str(result.get("run_id", "")), gate_artifact=result.get("name", ""))


def publish(a):
    out = Path(a.output)
    out.mkdir(parents=True, exist_ok=False)
    bundle = out / "candidate"
    meta = restore(a, bundle)
    if meta["mode"] != "catalog" or meta["key"] != a.key:
        raise Failure("publication_requires_catalog")
    receipt = out / "acceptance"
    download(a, {"run_id": a.gate_run_id, "name": "core-acceptance-" + a.head + "-" + a.key}, receipt, "gate")
    resolver = out / "baseline-resolver"
    # Fixed argv and environment; no untrusted command interpolation.
    resolver.write_text("#!/usr/bin/env python3\nimport os,subprocess,sys\np=subprocess.run([sys.executable,os.environ['NEWS_CI_SCRIPT'],'baseline','--root',os.environ['GITHUB_WORKSPACE'],'--head',os.environ['NEWS_CI_HEAD']],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)\nsys.stdout.buffer.write(p.stdout)\nsys.exit(p.returncode)\n")
    resolver.chmod(0o700)
    env = dict(os.environ, NEWS_CI_SCRIPT=str(Path(__file__).resolve()), NEWS_CI_HEAD=a.head)
    run([Path(os.environ["NEWS_CI_TOOLS"]) / "core-image-release", "--revision", a.head, "--platform", "linux/amd64",
         "--catalog", bundle / "catalog.json", "--impact-plan", bundle / "impact.json", "--baseline", bundle / "baseline.json",
         "--baseline-resolver", resolver, "--acceptance-dir", receipt, "--output", out / "release-images.json"], env=env)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=("baseline", "plan", "candidate", "gate", "lookup-gate", "publish"))
    p.add_argument("--root", default=".")
    p.add_argument("--head", required=True)
    p.add_argument("--base", default="")
    p.add_argument("--output", default="")
    p.add_argument("--plan", default="")
    p.add_argument("--run-id", type=int, default=0)
    p.add_argument("--gate-run-id", type=int, default=0)
    p.add_argument("--name", default="")
    p.add_argument("--key", default="")
    p.add_argument("--pr", action="store_true")
    p.add_argument("--full", action="store_true")
    p.add_argument("--bootstrap-fixture", action="store_true")
    a = p.parse_args()
    a.root = str(Path(a.root).resolve())
    if not SHA.fullmatch(a.head):
        raise Failure("head_invalid")
    if a.key and not re.fullmatch(r"[0-9a-f]{64}", a.key):
        raise Failure("catalog_key_invalid")
    os.chdir(a.root)
    globals()[a.command.replace("-", "_")](a)


if __name__ == "__main__":
    try:
        main()
    except (Failure, OSError, ValueError, KeyError) as e:
        code = str(e) if isinstance(e, Failure) else "metadata_or_environment_invalid"
        print("core CI failed (" + code + ")", file=sys.stderr)
        sys.exit(1)
