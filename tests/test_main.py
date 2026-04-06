from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from telegram import Update

from app import main as app_main
from app.bot import build_application
from app.database import Database


class MainTests(unittest.TestCase):
    @patch("app.main.build_application")
    @patch("app.main.InkyPiAdapter")
    @patch("app.main.RenderService")
    @patch("app.main.AuthService")
    @patch("app.main.Database")
    @patch("app.main.StorageService")
    @patch("app.main.load_config")
    @patch("app.main.parse_args")
    def test_main_preserves_pending_updates_on_startup(
        self,
        mock_parse_args,
        mock_load_config,
        mock_storage_service,
        mock_database,
        mock_auth_service,
        mock_render_service,
        mock_adapter,
        mock_build_application,
    ) -> None:
        mock_parse_args.return_value = SimpleNamespace(config=None, log_level="INFO")
        mock_load_config.return_value = SimpleNamespace(
            storage=SimpleNamespace(),
            database=SimpleNamespace(path="/tmp/frame.db"),
            security=SimpleNamespace(admin_user_ids=[], whitelisted_user_ids=[]),
            display=SimpleNamespace(),
            inkypi=SimpleNamespace(),
            telegram=SimpleNamespace(bot_token="token"),
        )
        mock_storage = Mock()
        mock_storage_service.return_value = mock_storage
        mock_db_instance = Mock()
        mock_database.return_value = mock_db_instance
        mock_application = Mock()
        mock_build_application.return_value = mock_application

        app_main.main()

        mock_application.run_polling.assert_called_once()
        kwargs = mock_application.run_polling.call_args.kwargs
        self.assertEqual(kwargs["allowed_updates"], Update.ALL_TYPES)
        self.assertNotIn("drop_pending_updates", kwargs)

    def test_build_application_succeeds_with_real_conversation_handlers(self) -> None:
        with patch("telegram.ext.ExtBot.initialize", new=Mock()), patch(
            "telegram.ext.ExtBot.shutdown",
            new=Mock(),
        ):
            database = Database(Path("/tmp/test-build-application.db"))
            database.initialize()
            services = SimpleNamespace(
                config=SimpleNamespace(
                    telegram=SimpleNamespace(bot_token="token"),
                    storage=SimpleNamespace(
                        current_payload_path=Path("/tmp/current.json"),
                        current_image_path=Path("/tmp/current.png"),
                    ),
                ),
                auth=SimpleNamespace(
                    sync_user=lambda user: None,
                    is_whitelisted=lambda user_id: True,
                    is_admin=lambda user_id: True,
                ),
                database=database,
                display=SimpleNamespace(
                    current_orientation=lambda: "horizontal",
                    get_slideshow_interval=lambda: 86400,
                    get_sleep_schedule=lambda: None,
                    runtime_settings_diagnostics=lambda: {"degraded": False, "message": ""},
                ),
                storage=SimpleNamespace(
                    rendered_path=lambda image_id: Path(f"/tmp/{image_id}.png"),
                    healthcheck=lambda: True,
                ),
                renderer=SimpleNamespace(render=lambda *args, **kwargs: None),
            )

            application = build_application(services)

            self.assertIsNotNone(application)
            self.assertTrue(application.error_handlers)
            database.close()


if __name__ == "__main__":
    unittest.main()
