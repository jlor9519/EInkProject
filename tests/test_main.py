from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from telegram import Update

from app import main as app_main


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


if __name__ == "__main__":
    unittest.main()
