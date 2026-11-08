from __future__ import annotations

import os
import tempfile
import unittest
from importlib.util import find_spec
from pathlib import Path


class ConfigTests(unittest.TestCase):
    def test_load_config_merges_env_and_yaml(self) -> None:
        if find_spec("yaml") is None or find_spec("dotenv") is None:
            self.skipTest("PyYAML and python-dotenv are required for config tests.")

        import yaml

        from app.config import load_config

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "telegram": {"bot_token_env": "TEST_TELEGRAM_BOT_TOKEN"},
                        "security": {"admin_user_ids": [123], "whitelisted_user_ids": [456]},
                        "database": {"path": "data/db/test.db"},
                        "storage": {
                            "incoming_dir": "data/incoming",
                            "rendered_dir": "data/rendered",
                            "cache_dir": "data/cache",
                            "archive_dir": "data/archive",
                            "inkypi_payload_dir": "data/inkypi",
                            "current_payload_path": "data/inkypi/current.json",
                            "current_image_path": "data/inkypi/current.png",
                            "keep_recent_rendered": 3,
                        },

                        "display": {
                            "width": 800,
                            "height": 480,
                            "caption_height": 120,
                            "margin": 12,
                            "metadata_font_size": 20,
                            "caption_font_size": 26,
                            "max_caption_lines": 2,
                            "font_path": "/tmp/does-not-exist.ttf",
                            "background_color": "#FFFFFF",
                            "text_color": "#000000",
                            "divider_color": "#333333",
                        },
                        "inkypi": {
                            "repo_path": "~/InkyPi",
                            "install_path": "/usr/local/inkypi",
                            "validated_commit": "main",
                            "waveshare_model": "epd7in3e",
                            "plugin_id": "telegram_frame",
                            "payload_dir": "data/inkypi",
                            "refresh_command": "echo refresh",
                        },
                    }
                ),
                encoding="utf-8",
            )

            os.environ["TEST_TELEGRAM_BOT_TOKEN"] = "token-123"
            try:
                config = load_config(config_path)
            finally:
                os.environ.pop("TEST_TELEGRAM_BOT_TOKEN", None)

            self.assertEqual(config.telegram.bot_token, "token-123")
            self.assertEqual(config.security.admin_user_ids, [123])
            self.assertEqual(config.security.whitelisted_user_ids, [456])
            self.assertTrue(str(config.database.path).endswith("data/db/test.db"))
            self.assertEqual(config.display.caption_character_limit, 72)
            # An old config with an inkypi section still loads; it only supplies the old paths.
            self.assertEqual(config.display.panel, "waveshare")
            self.assertEqual(config.display.waveshare_model, "epd7in3e")
            self.assertTrue(config.display.clear_before_display)
            self.assertEqual(config.display.refresh_timeout_seconds, 300)
            self.assertEqual(
                config.legacy_inkypi_device_paths,
                (
                    Path("/usr/local/inkypi/src/config/device.json"),
                    Path.home() / "InkyPi" / "src" / "config" / "device.json",
                ),
            )

    def test_missing_telegram_token_raises_error(self) -> None:
        if find_spec("yaml") is None or find_spec("dotenv") is None:
            self.skipTest("PyYAML and python-dotenv are required for config tests.")

        import yaml

        from app.config import ConfigError, load_config

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "telegram": {"bot_token_env": "MISSING_TOKEN"},
                        "security": {},
                        "database": {"path": "data/db/test.db"},
                        "storage": {
                            "incoming_dir": "data/incoming",
                            "rendered_dir": "data/rendered",
                            "cache_dir": "data/cache",
                            "archive_dir": "data/archive",
                            "inkypi_payload_dir": "data/inkypi",
                            "current_payload_path": "data/inkypi/current.json",
                            "current_image_path": "data/inkypi/current.png",
                            "keep_recent_rendered": 3,
                        },

                        "display": {
                            "width": 800,
                            "height": 480,
                            "caption_height": 120,
                            "margin": 12,
                            "metadata_font_size": 20,
                            "caption_font_size": 26,
                            "max_caption_lines": 2,
                            "font_path": "/tmp/does-not-exist.ttf",
                            "background_color": "#FFFFFF",
                            "text_color": "#000000",
                            "divider_color": "#333333",
                        },
                        "inkypi": {
                            "repo_path": "~/InkyPi",
                            "install_path": "/usr/local/inkypi",
                            "validated_commit": "main",
                            "waveshare_model": "epd7in3e",
                            "plugin_id": "telegram_frame",
                            "payload_dir": "data/inkypi",
                            "refresh_command": "echo refresh",
                        },
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaises(ConfigError):
                load_config(config_path)

    def test_load_config_uses_env_override_paths(self) -> None:
        if find_spec("yaml") is None or find_spec("dotenv") is None:
            self.skipTest("PyYAML and python-dotenv are required for config tests.")

        import yaml

        from app.config import load_config

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            config_path = tmpdir_path / "mock-config.yaml"
            env_path = tmpdir_path / "mock.env"
            env_path.write_text("OVERRIDE_TELEGRAM_TOKEN=token-from-env\n", encoding="utf-8")
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "telegram": {"bot_token_env": "OVERRIDE_TELEGRAM_TOKEN"},
                        "security": {},
                        "database": {"path": "data/db/test.db"},
                        "storage": {
                            "incoming_dir": "data/incoming",
                            "rendered_dir": "data/rendered",
                            "cache_dir": "data/cache",
                            "archive_dir": "data/archive",
                            "inkypi_payload_dir": "data/inkypi",
                            "current_payload_path": "data/inkypi/current.json",
                            "current_image_path": "data/inkypi/current.png",
                            "keep_recent_rendered": 3,
                        },

                        "display": {
                            "width": 800,
                            "height": 480,
                            "caption_height": 120,
                            "margin": 12,
                            "metadata_font_size": 20,
                            "caption_font_size": 26,
                            "max_caption_lines": 2,
                            "font_path": "/tmp/does-not-exist.ttf",
                            "background_color": "#FFFFFF",
                            "text_color": "#000000",
                            "divider_color": "#333333",
                        },
                        "inkypi": {
                            "repo_path": "~/InkyPi",
                            "install_path": "/usr/local/inkypi",
                            "validated_commit": "main",
                            "waveshare_model": "epd7in3e",
                            "plugin_id": "telegram_frame",
                            "payload_dir": "data/inkypi",
                            "refresh_command": "echo refresh",
                        },
                    }
                ),
                encoding="utf-8",
            )

            old_config = os.environ.get("PHOTO_FRAME_CONFIG")
            old_env = os.environ.get("PHOTO_FRAME_ENV_FILE")
            os.environ["PHOTO_FRAME_CONFIG"] = str(config_path)
            os.environ["PHOTO_FRAME_ENV_FILE"] = str(env_path)
            try:
                config = load_config()
            finally:
                if old_config is None:
                    os.environ.pop("PHOTO_FRAME_CONFIG", None)
                else:
                    os.environ["PHOTO_FRAME_CONFIG"] = old_config
                if old_env is None:
                    os.environ.pop("PHOTO_FRAME_ENV_FILE", None)
                else:
                    os.environ["PHOTO_FRAME_ENV_FILE"] = old_env

            self.assertEqual(config.telegram.bot_token, "token-from-env")

    def _write_minimal_config(self, tmpdir: str, display: dict) -> Path:
        import yaml

        config_path = Path(tmpdir) / "config.yaml"
        config_path.write_text(
            yaml.safe_dump(
                {
                    "telegram": {"bot_token_env": "PANEL_TELEGRAM_TOKEN"},
                    "database": {"path": "data/db/test.db"},
                    "display": display,
                }
            ),
            encoding="utf-8",
        )
        return config_path

    def _load_with_token(self, config_path: Path):
        from app.config import load_config

        os.environ["PANEL_TELEGRAM_TOKEN"] = "panel-token"
        try:
            return load_config(config_path)
        finally:
            os.environ.pop("PANEL_TELEGRAM_TOKEN", None)

    def test_load_config_reads_panel_settings(self) -> None:
        if find_spec("yaml") is None or find_spec("dotenv") is None:
            self.skipTest("PyYAML and python-dotenv are required for config tests.")

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = self._write_minimal_config(
                tmpdir,
                {
                    "panel": "file",
                    "waveshare_model": "epd7in3f",
                    "driver_dir": "/opt/drivers/waveshare_epd",
                    "clear_before_display": False,
                    "refresh_timeout_seconds": 120,
                    "panel_output_path": "/tmp/frame-panel.png",
                },
            )

            config = self._load_with_token(config_path)

        self.assertEqual(config.display.panel, "file")
        self.assertEqual(config.display.waveshare_model, "epd7in3f")
        self.assertEqual(config.display.driver_dir, Path("/opt/drivers/waveshare_epd"))
        self.assertFalse(config.display.clear_before_display)
        self.assertEqual(config.display.refresh_timeout_seconds, 120)
        self.assertEqual(config.display.panel_output_path, Path("/tmp/frame-panel.png"))

    def test_load_config_rejects_unknown_panel_and_bad_wipe_flag(self) -> None:
        if find_spec("yaml") is None or find_spec("dotenv") is None:
            self.skipTest("PyYAML and python-dotenv are required for config tests.")

        from app.config import ConfigError

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = self._write_minimal_config(
                tmpdir,
                {"panel": "hdmi", "clear_before_display": "sometimes"},
            )

            with self.assertRaises(ConfigError) as caught:
                self._load_with_token(config_path)

        self.assertIn("display.panel", str(caught.exception))
        self.assertIn("display.clear_before_display", str(caught.exception))

    def test_load_config_migrates_legacy_large_caption_defaults_to_compact_bar(self) -> None:
        if find_spec("yaml") is None or find_spec("dotenv") is None:
            self.skipTest("PyYAML and python-dotenv are required for config tests.")

        import yaml

        from app.config import load_config

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "telegram": {"bot_token_env": "COMPACT_TELEGRAM_TOKEN"},
                        "security": {},
                        "database": {"path": "data/db/test.db"},
                        "storage": {
                            "incoming_dir": "data/incoming",
                            "rendered_dir": "data/rendered",
                            "cache_dir": "data/cache",
                            "archive_dir": "data/archive",
                            "inkypi_payload_dir": "data/inkypi",
                            "current_payload_path": "data/inkypi/current.json",
                            "current_image_path": "data/inkypi/current.png",
                            "keep_recent_rendered": 3,
                        },

                        "display": {
                            "width": 800,
                            "height": 480,
                            "caption_height": 132,
                            "margin": 18,
                            "metadata_font_size": 22,
                            "caption_font_size": 28,
                            "max_caption_lines": 2,
                            "font_path": "/tmp/does-not-exist.ttf",
                            "background_color": "#FFFFFF",
                            "text_color": "#000000",
                            "divider_color": "#333333",
                        },
                        "inkypi": {
                            "repo_path": "~/InkyPi",
                            "install_path": "/usr/local/inkypi",
                            "validated_commit": "main",
                            "waveshare_model": "epd7in3e",
                            "plugin_id": "telegram_frame",
                            "payload_dir": "data/inkypi",
                            "refresh_command": "sudo systemctl restart inkypi.service",
                        },
                    }
                ),
                encoding="utf-8",
            )

            os.environ["COMPACT_TELEGRAM_TOKEN"] = "compact-token"
            try:
                config = load_config(config_path)
            finally:
                os.environ.pop("COMPACT_TELEGRAM_TOKEN", None)

            self.assertEqual(config.display.caption_height, 44)
            self.assertEqual(config.display.metadata_font_size, 14)
            self.assertEqual(config.display.caption_font_size, 20)
            self.assertEqual(config.display.caption_character_limit, 72)

if __name__ == "__main__":
    unittest.main()
