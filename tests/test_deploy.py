import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("deploy", ROOT / "scripts/deploy.py")
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)


class TraderOnlyDeployTests(unittest.TestCase):
    def test_pins_only_uma_and_rejects_missing_service_or_mutable_image(self):
        source = (ROOT / "deploy/compose.live.yaml").read_bytes()
        image = deploy.REPOSITORY + "@sha256:" + "a" * 64
        pinned = deploy.pin_uma_image(source, image)
        self.assertIn(b"  trader:\n    image: ${DEMO_IMAGE", pinned)
        self.assertIn(("  uma:\n    image: " + image).encode(), pinned)
        self.assertEqual(pinned.count(b"${DEMO_IMAGE"), 1)
        for text, value in [(source, deploy.REPOSITORY + ":latest"), (b"services: {}", image)]:
            with self.assertRaises(RuntimeError):
                deploy.pin_uma_image(text, value)

class UmaModeDeployTests(unittest.TestCase):
    def test_live_requires_explicit_account_and_keeps_separate_journal(self):
        import json
        import sys
        from unittest.mock import patch

        module_spec = importlib.util.spec_from_file_location("uma_deploy", ROOT / "scripts/deploy_uma_dry.py")
        module = importlib.util.module_from_spec(module_spec)
        with patch.dict(sys.modules, {"deploy": deploy}):
            module_spec.loader.exec_module(module)
        for account in (None, "airdrop_224"):
            calls = []
            def fake_run(args, **kwargs):
                if "--show-current" in args:
                    return b"main"
                if "rev-parse" in args:
                    return b"1" * 40
                return b""
            def fake_remote(host, command, **kwargs):
                calls.append((host, command, kwargs.get("data")))
                if "RepoDigests" in command:
                    return json.dumps([module.REPOSITORY + "@sha256:" + "a" * 64]).encode()
                if "database-reader.json && echo" in command:
                    return b"yes"
                if "--format '{{.Id}}'" in command:
                    return b"original-trader-and-uma"
                if command.startswith("curl"):
                    return json.dumps({"mode": "live" if account else "shadow", "account": account, "ready": True}).encode()
                return b""
            argv = ["deploy_uma_dry.py"] + (["--live-account", account] if account else [])
            with patch.object(sys, "argv", argv), patch.object(module, "run", fake_run), patch.object(module, "remote", fake_remote), patch.object(module, "temp", return_value="/tmp/polym-rust-deploy.test"), patch.object(module, "credentials", return_value=b"{}"), patch("builtins.print"):
                module.main()
            compose = next(data for _, command, data in calls if command.endswith("/compose.yaml"))
            script = next(data for _, command, data in calls if command == "bash -s")
            self.assertIn(b"trader-uma", script)
            self.assertNotIn(b"polym-rust-demo-trader-1", script)
            self.assertIn(b"--profile live" if account else b" up -d", script)
            self.assertIn(b"trade-live" if account else b"[shadow,", compose)
        live = json.loads((ROOT / "deploy/uma-trader.json").read_text())
        dry = json.loads((ROOT / "deploy/uma-dry-run.json").read_text())
        self.assertIsNone(live["total_budget_pusd"])
        self.assertNotEqual(live["journal"], dry["journal"])


if __name__ == "__main__":
    unittest.main()
