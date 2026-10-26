from __future__ import annotations

import datetime
import unittest

from telegram import CallbackQuery, Chat, Message, Update, User
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


    def test_timeouts_also_run_when_the_last_update_was_a_button_tap(self) -> None:
        # PTB runs the timeout handlers with the conversation's last update, often a callback query.
        user = User(1, "Test", False)
        message = Message(1, datetime.datetime.now(datetime.timezone.utc), Chat(1, "private"), from_user=user)
        button_tap = Update(2, callback_query=CallbackQuery("q", user, "chat", data="photo_text_yes", message=message))

        for handler in (build_photo_conversation(), build_settings_conversation()):
            timeout_handlers = handler.states[ConversationHandler.TIMEOUT]
            self.assertTrue(any(h.check_update(button_tap) for h in timeout_handlers), handler.name)


if __name__ == "__main__":
    unittest.main()
