"""Synthetic orchestration tests: no Docker, network, registry or application calls."""
import importlib.util
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("ci_core", Path(__file__).with_name("ci-core.py"))
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)


class CoreTests(unittest.TestCase):
    def exercise_candidate(self, directory, *, pr=False, unavailable=False, registry_unavailable=False):
        root = Path(directory)
        source, output = root / "plan", root / "bundle"
        source.mkdir()
        head, base = "a" * 40, "b" * 40
        fps = {t: str(i) * 64 for i, t in enumerate((*ci.TARGETS, "extractor"), 1)}
        impact = {"schemaVersion": 1, "base": "c" * 40 if pr else base, "head": head,
                  "modules": ["go-domain"], "targets": ["executor"], "fixture_targets": [], "source_fingerprints": fps}
        old = {"schemaVersion": 2, "revision": base, "baselineRevision": base,
               "platform": "linux/amd64", "images": {}}
        images = {}
        for i, target in enumerate(ci.TARGETS, 1):
            old["images"][target] = {"repository": "fixture/" + target, "revision": base,
                "platform": "linux/amd64", "imageID": "sha256:" + str(i) * 64,
                "digest": "sha256:" + str(i + 3) * 64, "sourceFingerprint": fps[target], "origin": "reused"}
            images[ci.TAGS[target]] = {k: old["images"][target][k] for k in ("imageID", "platform", "revision")}
            images["ghcr.io/" + old["images"][target]["repository"] + "@" + old["images"][target]["digest"]] = copy.deepcopy(images[ci.TAGS[target]])
            if target == "executor" or unavailable:
                images[ci.TAGS[target]]["revision"] = head
        images[ci.FIXTURE_TAG] = {"imageID": "sha256:" + "9" * 64, "platform": "linux/amd64", "revision": ""}
        ci.write(source / "impact.json", impact)
        ci.write(source / "selection.json", {"reuse": None, "pr": pr})
        if not pr:
            ci.write(source / "baseline.raw.json", old)
        commands = []

        def fake_tool(name, *args):
            if name == "ci-artifacts":
                if unavailable:
                    raise ci.Failure("github_http_error")
                return json.dumps(old).encode()
            if name == "ci-impact":
                changed = dict(impact, base=base)
                Path(args[args.index("--output") + 1]).write_text(json.dumps(changed))
                return b""
            if name == "core-image-catalog":
                catalog = copy.deepcopy(old)
                catalog["revision"] = head
                catalog["images"]["executor"].update(images[ci.TAGS["executor"]], origin="built", digest="")
                ci.write(args[args.index("--output") + 1], catalog)
                return b""
            raise AssertionError("unexpected tool")

        def fake_run(args, **kw):
            commands.append([str(v) for v in args])
            if args[:2] == ["docker", "pull"] and registry_unavailable:
                raise ci.Failure("registry_unavailable")
            if args[:3] == ["docker", "buildx", "bake"]:
                for target in args[args.index("--load") + 1:]:
                    images[ci.TAGS[target]]["revision"] = head
            if "save" in args:
                Path(args[args.index("-o") + 1]).write_bytes(b"synthetic OCI archive")
            return b""

        a = SimpleNamespace(plan=str(source), output=str(output), head=head, root=str(root))
        with patch.dict(ci.os.environ, {"GITHUB_RUN_ID": "42", "GITHUB_REPOSITORY": "fixture/repo",
                                       "GITHUB_STEP_SUMMARY": str(root / "summary.md")}), \
                patch.object(ci, "tool", side_effect=fake_tool), patch.object(ci, "run", side_effect=fake_run), \
                patch.object(ci, "inspect", side_effect=lambda ref: images[ref]):
            ci.candidate(a)
        return commands, output

    def test_main_candidate_builds_only_selected_runtime(self):
        with tempfile.TemporaryDirectory() as d:
            commands, output = self.exercise_candidate(d)
            builds = [v for v in commands if v[:3] == ["docker", "buildx", "bake"]]
            self.assertEqual(len(builds), 1)
            self.assertEqual(builds[0][-1], "executor")
            self.assertFalse(any(v[:2] == ["docker", "push"] for v in commands))
            self.assertFalse(any(v[:2] == ["docker", "build"] for v in commands))
            self.assertEqual(ci.read(output / "metadata.json")["mode"], "catalog")

    def test_pr_with_trusted_baseline_uses_selected_mixed_catalog(self):
        with tempfile.TemporaryDirectory() as d:
            commands, output = self.exercise_candidate(d, pr=True)
            build = next(v for v in commands if v[:3] == ["docker", "buildx", "bake"])
            self.assertEqual(build[-1], "executor")
            self.assertEqual(ci.read(output / "metadata.json")["mode"], "catalog")
            self.assertEqual(ci.read(output / "impact.json")["base"], "b" * 40)
            summary = json.loads((Path(d) / "summary.md").read_text().split("```json\n")[1].split("\n```", 1)[0])
            self.assertEqual(summary["baseline"], "b" * 40)
            self.assertEqual(summary["targets"], ["executor"])

    def test_pr_without_baseline_builds_isolated_unpublishable_set(self):
        with tempfile.TemporaryDirectory() as d:
            commands, output = self.exercise_candidate(d, pr=True, unavailable=True)
            build = next(v for v in commands if v[:3] == ["docker", "buildx", "bake"])
            self.assertEqual(build[-3:], list(ci.TARGETS))
            self.assertEqual(ci.read(output / "metadata.json")["mode"], "legacy")
            self.assertTrue(any(v[:2] == ["docker", "build"] for v in commands))
            self.assertFalse((output / "catalog.json").exists())

    def test_pr_registry_access_failure_also_falls_back_to_unpublishable_set(self):
        with tempfile.TemporaryDirectory() as d:
            commands, output = self.exercise_candidate(d, pr=True, registry_unavailable=True)
            build = next(v for v in commands if v[:3] == ["docker", "buildx", "bake"])
            self.assertEqual(build[-3:], list(ci.TARGETS))
            self.assertEqual(ci.read(output / "metadata.json")["mode"], "legacy")
            self.assertFalse((output / "baseline.json").exists())
            summary = json.loads((Path(d) / "summary.md").read_text().split("```json\n")[1].split("\n```", 1)[0])
            self.assertEqual(summary["targets"], list(ci.TARGETS))
            self.assertEqual(summary["mode"], "legacy")
            self.assertIn("pr_baseline_unavailable_or_unusable", summary["reasons"]["all"])

    def test_main_registry_failure_cannot_fall_back(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaisesRegex(ci.Failure, "registry_unavailable"):
                self.exercise_candidate(d, registry_unavailable=True)

    def test_input_graph_revision_forces_rebuild_instead_of_stale_reuse(self):
        impact = {"targets": [], "source_fingerprints": {t: "a" * 64 for t in ci.TARGETS}}
        baseline = {"schemaVersion": 2, "images": {t: {"sourceFingerprint": "a" * 64} for t in ci.TARGETS}}
        baseline["images"]["executor"]["sourceFingerprint"] = "b" * 64
        result = ci.reconcile_fingerprints(impact, baseline)
        self.assertEqual(result["targets"], ["executor"])
        self.assertTrue(result["integration_required"])

    def test_plan_summary_includes_final_fingerprint_reconciliation(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            impact = {"targets": [], "fixture_targets": [], "modules": [], "integration_required": False,
                      "source_fingerprints": {t: "a" * 64 for t in ci.TARGETS}}
            old = {"schemaVersion": 2, "revision": "b" * 40,
                   "images": {t: {"sourceFingerprint": "a" * 64} for t in ci.TARGETS}}
            old["images"]["executor"]["sourceFingerprint"] = "c" * 64

            def fake_tool(name, *args):
                if name == "ci-artifacts":
                    return json.dumps(old).encode()
                self.assertEqual(name, "ci-impact")
                self.assertNotIn("--summary", args)
                ci.write(args[args.index("--output") + 1], impact)

            a = SimpleNamespace(output=str(root / "plan"), base=None, pr=False, full=False,
                                root=str(root), head="d" * 40)
            summary = root / "summary.md"
            with patch.dict(ci.os.environ, {"GITHUB_STEP_SUMMARY": str(summary), "GITHUB_REPOSITORY": "fixture/repo"}), \
                    patch.object(ci, "tool", side_effect=fake_tool), \
                    patch.object(ci, "find", return_value={"found": False}), patch.object(ci, "outputs"):
                ci.plan(a)
            reported = json.loads(summary.read_text().split("```json\n", 1)[1].split("\n```", 1)[0])
            self.assertEqual(reported, ci.read(root / "plan" / "impact.json"))
            self.assertEqual(reported["targets"], ["executor"])
            self.assertIn("baseline_catalog_fingerprint_changed", reported["reasons"]["executor"])

    def test_reuse_requires_identical_impact_and_baseline(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            impact = {"head": "a" * 40, "targets": ["executor"], "source_fingerprints": {"executor": "b" * 64}}
            old = {"revision": "c" * 40}
            for name, value in (("impact.json", impact), ("baseline.raw.json", old),
                                ("metadata.json", {"mode": "catalog"}), ("catalog.json", {"revision": impact["head"]})):
                ci.write(p / name, value)
            self.assertTrue(ci.valid_reuse(p, impact, old))
            self.assertFalse(ci.valid_reuse(p, impact, {"revision": "d" * 40}))
            self.assertFalse(ci.valid_reuse(p, dict(impact, targets=["web"]), old))

    def test_single_target_build_never_builds_others(self):
        commands = []
        with patch.object(ci, "run", side_effect=lambda args, **kw: commands.append(list(args))):
            ci.build_core(["executor"], "a" * 40)
        self.assertEqual(commands[0][-1], "executor")
        self.assertNotIn("web", commands[0])
        self.assertNotIn("background", commands[0])
        commands.clear()
        with patch.object(ci, "run", side_effect=lambda args, **kw: commands.append(args)):
            ci.build_core([], "a" * 40)
        self.assertEqual(commands, [])
        with self.assertRaises(ci.Failure):
            ci.build_core(["executor", "untrusted"], "a" * 40)

    def test_image_tag_replacement_is_rejected_before_gate(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            expected = {"imageID": "sha256:" + "a" * 64, "platform": "linux/amd64", "revision": "b" * 40}
            ci.write(p / "metadata.json", {"mode": "legacy", "images": {t: expected for t in ci.TARGETS}})
            with patch.object(ci, "inspect", return_value=dict(expected, imageID="sha256:" + "c" * 64)):
                with self.assertRaisesRegex(ci.Failure, "candidate_image_changed"):
                    ci.verify_images(p)

    def test_fixture_missing_does_not_silently_rebuild(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            ci.write(p / "impact.json", {"source_fingerprints": {"extractor": "a" * 64}})
            a = SimpleNamespace(bootstrap_fixture=False)
            with patch.object(ci, "find", return_value={"found": False}), patch.object(ci, "build_fixture") as build:
                with self.assertRaisesRegex(ci.Failure, "fixture_bootstrap_required"):
                    ci.fixture(a, p, {}, p / "fixture")
                build.assert_not_called()

    def test_failed_gate_never_emits_acceptance(self):
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "gate"
            a = SimpleNamespace(output=str(out), key="a" * 64, head="b" * 40)
            commands = []
            def fail_gate(args, **kw):
                commands.append(args)
                if "--output-dir" in args:
                    raise ci.Failure("fixture_failed")
                return b""
            with patch.dict(ci.os.environ, {"NEWS_CI_TOOLS": "/fixture/tools"}), \
                    patch.object(ci, "restore", return_value={"mode": "catalog", "key": a.key}), \
                    patch.object(ci, "fixture", return_value=("c" * 64, True)), patch.object(ci, "run", side_effect=fail_gate):
                with self.assertRaisesRegex(ci.Failure, "fixture_failed"):
                    ci.gate(a)
            self.assertFalse((out / "accepted/receipt.json").exists())

    def test_publisher_rejects_legacy_before_any_publish(self):
        with tempfile.TemporaryDirectory() as d:
            a = SimpleNamespace(output=str(Path(d) / "publish"), key="a" * 64)
            with patch.object(ci, "restore", return_value={"mode": "legacy", "key": a.key}), patch.object(ci, "run") as invoke:
                with self.assertRaisesRegex(ci.Failure, "publication_requires_catalog"):
                    ci.publish(a)
                invoke.assert_not_called()

    def test_metadata_write_is_exclusive(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "data.json"
            ci.write(p, {"success": True})
            with self.assertRaises(FileExistsError):
                ci.write(p, {"success": False})
            self.assertTrue(json.loads(p.read_text())["success"])


if __name__ == "__main__":
    unittest.main()
