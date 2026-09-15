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


if __name__ == "__main__":
    unittest.main()
