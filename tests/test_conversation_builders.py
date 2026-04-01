from __future__ import annotations

import unittest

from telegram.ext import ConversationHandler

from app.conversations import build_photo_conversation
from app.settings_conversation import build_settings_conversation


class ConversationBuilderTests(unittest.TestCase):
    def test_build_photo_conversation_returns_handler(self) -> None:
        handler = build_photo_conversation()

        self.assertIsInstance(handler, ConversationHandler)

    def test_build_settings_conversation_returns_handler(self) -> None:
        handler = build_settings_conversation()

        self.assertIsInstance(handler, ConversationHandler)


if __name__ == "__main__":
    unittest.main()
