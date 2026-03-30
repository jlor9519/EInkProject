from __future__ import annotations

import functools
import logging
from typing import Any

logger = logging.getLogger(__name__)

from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler


def require_whitelist(_func: Any = None, *, conversation: bool = False) -> Any:
    def decorator(handler: Any) -> Any:
        @functools.wraps(handler)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Any:
            services = context.application.bot_data["services"]
            user = update.effective_user
            message = update.effective_message
            if user is None or message is None:
                return ConversationHandler.END if conversation else None
            services.auth.sync_user(user)
            if not services.auth.is_whitelisted(user.id):
                logger.warning("Whitelist denied user %d for %s", user.id, handler.__name__)
                await message.reply_text(
                    "Du bist für diesen Fotorahmen noch nicht freigegeben. "
                    "Bitte einen Admin, deine Telegram-ID hinzuzufügen."
                )
                return ConversationHandler.END if conversation else None
            return await handler(update, context)

        return wrapper

    if _func is not None:
        return decorator(_func)
    return decorator


def require_admin(handler: Any) -> Any:
    @functools.wraps(handler)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Any:
        services = context.application.bot_data["services"]
        user = update.effective_user
        message = update.effective_message
        if user is None or message is None:
            return None
        services.auth.sync_user(user)
        if not services.auth.is_admin(user.id):
            logger.warning("Admin denied user %d for %s", user.id, handler.__name__)
            await message.reply_text("Dieser Befehl ist nur für Admins verfügbar.")
            return None
        return await handler(update, context)

    return wrapper


class AuthService:
    def __init__(self, database: Any):
        self.database = database

    def sync_user(self, user: Any) -> None:
        username = getattr(user, "username", None)
        first_name = getattr(user, "first_name", "") or ""
        last_name = getattr(user, "last_name", "") or ""
        display_name = " ".join(part for part in [first_name, last_name] if part).strip() or username
        self.database.ensure_user(user.id, username=username, display_name=display_name)

    def is_whitelisted(self, user_id: int) -> bool:
        return self.database.is_whitelisted(user_id)

    def is_admin(self, user_id: int) -> bool:
        return self.database.is_admin(user_id)

    def whitelist_user(self, user_id: int) -> None:
        self.database.whitelist_user(user_id)

