from __future__ import annotations

import subprocess
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = [
    PROJECT_ROOT / "scripts" / "common.sh",
    PROJECT_ROOT / "scripts" / "install.sh",
    PROJECT_ROOT / "scripts" / "mock_install.sh",
    PROJECT_ROOT / "scripts" / "test_telegram_bot.sh",
    PROJECT_ROOT / "scripts" / "update.sh",
]


class ScriptTests(unittest.TestCase):
    def test_shell_scripts_have_valid_bash_syntax(self) -> None:
        for script in SCRIPTS:
            completed = subprocess.run(
                ["bash", "-n", str(script)],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, msg=f"{script} failed syntax check: {completed.stderr}")

    def test_install_flow_references_interactive_setup_steps(self) -> None:
        install_text = (PROJECT_ROOT / "scripts" / "install.sh").read_text(encoding="utf-8")
        self.assertIn("prompt_value", install_text)
        self.assertIn("ensure_panel_ready", install_text)
        self.assertNotIn("setup_inkypi.sh", install_text)

    def test_install_and_update_turn_off_inkypi_and_prepare_the_panel(self) -> None:
        for name in ("install.sh", "update.sh"):
            text = (PROJECT_ROOT / "scripts" / name).read_text(encoding="utf-8")
            with self.subTest(script=name):
                self.assertIn("ensure_panel_ready", text)
                self.assertIn("retire_inkypi_service", text)
                # The body runs from main so a git pull cannot change the script mid-run.
                self.assertTrue(text.rstrip().endswith('main "$@"'))

    def test_scripts_no_longer_reference_inkypi_setup(self) -> None:
        for script in SCRIPTS:
            text = script.read_text(encoding="utf-8")
            with self.subTest(script=script.name):
                self.assertNotIn("setup_inkypi.sh", text)
                self.assertNotIn("app.inkypi_", text)

    def test_update_script_supports_clear_images_flag(self) -> None:
        update_text = (PROJECT_ROOT / "scripts" / "update.sh").read_text(encoding="utf-8")
        self.assertIn("--clear-images", update_text)
        self.assertIn("app.library_cleanup", update_text)

    def test_foreground_telegram_runner_is_non_daemonized(self) -> None:
        runner_text = (PROJECT_ROOT / "scripts" / "test_telegram_bot.sh").read_text(encoding="utf-8")
        self.assertIn("-m app.main", runner_text)
        self.assertIn("wait \"${BOT_PID}\"", runner_text)
        self.assertIn("trap cleanup EXIT INT TERM", runner_text)


if __name__ == "__main__":
    unittest.main()
