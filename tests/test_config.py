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
                            "emoji_font_path": "/tmp/emoji.ttf",
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
            self.assertEqual(config.inkypi.repo_path, Path.home() / "InkyPi")
            self.assertEqual(config.inkypi.install_path, Path("/usr/local/inkypi"))
            self.assertEqual(config.inkypi.update_method, "command")
            self.assertEqual(config.inkypi.update_now_url, "http://127.0.0.1/update_now")
            self.assertEqual(config.inkypi.waveshare_model, "epd7in3e")
            self.assertEqual(config.display.caption_character_limit, 72)
            self.assertEqual(config.display.emoji_font_path, "/tmp/emoji.ttf")

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

    def test_load_config_migrates_legacy_restart_command_to_http_update_now(self) -> None:
        if find_spec("yaml") is None or find_spec("dotenv") is None:
            self.skipTest("PyYAML and python-dotenv are required for config tests.")

        import yaml

        from app.config import load_config

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "telegram": {"bot_token_env": "LEGACY_TELEGRAM_TOKEN"},
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
                            "refresh_command": "sudo systemctl restart inkypi.service",
                        },
                    }
                ),
                encoding="utf-8",
            )

            os.environ["LEGACY_TELEGRAM_TOKEN"] = "legacy-token"
            try:
                config = load_config(config_path)
            finally:
                os.environ.pop("LEGACY_TELEGRAM_TOKEN", None)

            self.assertEqual(config.inkypi.update_method, "http_update_now")
            self.assertEqual(config.inkypi.update_now_url, "http://127.0.0.1/update_now")

    def test_load_config_uses_default_emoji_font_path_when_omitted(self) -> None:
        if find_spec("yaml") is None or find_spec("dotenv") is None:
            self.skipTest("PyYAML and python-dotenv are required for config tests.")

        import yaml

        from app.config import DEFAULT_EMOJI_FONT_PATH, load_config

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "telegram": {"bot_token_env": "TEST_TELEGRAM_BOT_TOKEN"},
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
                            "caption_height": 44,
                            "margin": 12,
                            "metadata_font_size": 20,
                            "caption_font_size": 26,
                            "max_caption_lines": 1,
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

            self.assertEqual(config.display.emoji_font_path, DEFAULT_EMOJI_FONT_PATH)

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
            self.assertEqual(config.display.max_caption_lines, 1)

if __name__ == "__main__":
    unittest.main()
