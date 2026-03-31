from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from app.models import ImageRecord


def utcnow_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.db_path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._lock = threading.Lock()

    def close(self) -> None:
        self._connection.close()

    def initialize(self) -> None:
        with self._lock:
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    telegram_user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    display_name TEXT,
                    is_admin INTEGER NOT NULL DEFAULT 0,
                    is_whitelisted INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS images (
                    image_id TEXT PRIMARY KEY,
                    telegram_file_id TEXT NOT NULL,
                    telegram_chat_id INTEGER,
                    local_original_path TEXT NOT NULL,
                    local_rendered_path TEXT,
                    location TEXT NOT NULL,
                    taken_at TEXT NOT NULL,
                    caption TEXT NOT NULL,
                    uploaded_by INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_error TEXT
                );

                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_images_created_at ON images(created_at DESC);
                """
            )
            columns = {
                row["name"]
                for row in self._connection.execute("PRAGMA table_info(images)").fetchall()
            }
            if "telegram_chat_id" not in columns:
                self._connection.execute(
                    "ALTER TABLE images ADD COLUMN telegram_chat_id INTEGER"
                )
            self._connection.commit()
        logger.info("Database initialized at %s", self.db_path)

    def healthcheck(self) -> bool:
        with self._lock:
            row = self._connection.execute("SELECT 1").fetchone()
            return bool(row and row[0] == 1)

    def ensure_user(
        self,
        telegram_user_id: int,
        username: str | None = None,
        display_name: str | None = None,
    ) -> None:
        with self._lock:
            existing = self._connection.execute(
                "SELECT telegram_user_id FROM users WHERE telegram_user_id = ?",
                (telegram_user_id,),
            ).fetchone()
            if existing:
                self._connection.execute(
                    """
                    UPDATE users
                    SET username = COALESCE(?, username),
                        display_name = COALESCE(?, display_name)
                    WHERE telegram_user_id = ?
                    """,
                    (username, display_name, telegram_user_id),
                )
            else:
                self._connection.execute(
                    """
                    INSERT INTO users (
                        telegram_user_id, username, display_name, is_admin, is_whitelisted, created_at
                    ) VALUES (?, ?, ?, 0, 0, ?)
                    """,
                    (telegram_user_id, username, display_name, utcnow_iso()),
                )
            self._connection.commit()

    def seed_admins(self, admin_user_ids: list[int]) -> None:
        for user_id in admin_user_ids:
            self.whitelist_user(user_id, is_admin=True)

    def seed_whitelist(self, user_ids: list[int]) -> None:
        for user_id in user_ids:
            self.whitelist_user(user_id, is_admin=False)

    def whitelist_user(self, telegram_user_id: int, *, is_admin: bool = False) -> None:
        self.ensure_user(telegram_user_id)
        with self._lock:
            self._connection.execute(
                """
                UPDATE users
                SET is_whitelisted = 1,
                    is_admin = CASE WHEN ? THEN 1 ELSE is_admin END
                WHERE telegram_user_id = ?
                """,
                (1 if is_admin else 0, telegram_user_id),
            )
            self._connection.commit()

    def is_whitelisted(self, telegram_user_id: int) -> bool:
        with self._lock:
            row = self._connection.execute(
                "SELECT is_whitelisted FROM users WHERE telegram_user_id = ?",
                (telegram_user_id,),
            ).fetchone()
            return bool(row and row["is_whitelisted"])

    def is_admin(self, telegram_user_id: int) -> bool:
        with self._lock:
            row = self._connection.execute(
                "SELECT is_admin FROM users WHERE telegram_user_id = ?",
                (telegram_user_id,),
            ).fetchone()
            return bool(row and row["is_admin"])

    def count_whitelisted_users(self) -> int:
        with self._lock:
            row = self._connection.execute(
                "SELECT COUNT(*) AS count FROM users WHERE is_whitelisted = 1"
            ).fetchone()
            return int(row["count"] if row else 0)

    def upsert_image(self, record: ImageRecord) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO images (
                    image_id,
                    telegram_file_id,
                    telegram_chat_id,
                    local_original_path,
                    local_rendered_path,
                    location,
                    taken_at,
                    caption,
                    uploaded_by,
                    created_at,
                    status,
                    last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(image_id) DO UPDATE SET
                    telegram_file_id = excluded.telegram_file_id,
                    telegram_chat_id = excluded.telegram_chat_id,
                    local_original_path = excluded.local_original_path,
                    local_rendered_path = excluded.local_rendered_path,
                    location = excluded.location,
                    taken_at = excluded.taken_at,
                    caption = excluded.caption,
                    uploaded_by = excluded.uploaded_by,
                    created_at = excluded.created_at,
                    status = excluded.status,
                    last_error = excluded.last_error
                """,
                (
                    record.image_id,
                    record.telegram_file_id,
                    record.telegram_chat_id,
                    record.local_original_path,
                    record.local_rendered_path,
                    record.location,
                    record.taken_at,
                    record.caption,
                    record.uploaded_by,
                    record.created_at,
                    record.status,
                    record.last_error,
                ),
            )
            self._connection.commit()

    def get_latest_image(self) -> ImageRecord | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM images ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            return self._row_to_image(row)

    def delete_image(self, image_id: str) -> bool:
        with self._lock:
            cursor = self._connection.execute(
                "DELETE FROM images WHERE image_id = ?", (image_id,)
            )
            self._connection.commit()
            return cursor.rowcount > 0

    def get_image_by_id(self, image_id: str) -> ImageRecord | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM images WHERE image_id = ?", (image_id,)
            ).fetchone()
            if row is None:
                return None
            return self._row_to_image(row)

    _DISPLAYED_STATUSES = ("displayed", "displayed_with_warnings")

    def get_adjacent_image(self, current_image_id: str, direction: str) -> ImageRecord | None:
        with self._lock:
            current = self._connection.execute(
                "SELECT created_at FROM images WHERE image_id = ?", (current_image_id,)
            ).fetchone()
            if current is None:
                return None
            current_created_at = current["created_at"]

            if direction == "next":
                row = self._connection.execute(
                    "SELECT * FROM images WHERE created_at > ? AND status IN (?, ?) ORDER BY created_at ASC LIMIT 1",
                    (current_created_at, *self._DISPLAYED_STATUSES),
                ).fetchone()
                if row is None:
                    row = self._connection.execute(
                        "SELECT * FROM images WHERE image_id != ? AND status IN (?, ?) ORDER BY created_at ASC LIMIT 1",
                        (current_image_id, *self._DISPLAYED_STATUSES),
                    ).fetchone()
            else:
                row = self._connection.execute(
                    "SELECT * FROM images WHERE created_at < ? AND status IN (?, ?) ORDER BY created_at DESC LIMIT 1",
                    (current_created_at, *self._DISPLAYED_STATUSES),
                ).fetchone()
                if row is None:
                    row = self._connection.execute(
                        "SELECT * FROM images WHERE image_id != ? AND status IN (?, ?) ORDER BY created_at DESC LIMIT 1",
                        (current_image_id, *self._DISPLAYED_STATUSES),
                    ).fetchone()

            if row is None:
                return None
            return self._row_to_image(row)

    def count_displayed_images(self) -> int:
        with self._lock:
            row = self._connection.execute(
                "SELECT COUNT(*) AS count FROM images WHERE status IN (?, ?)",
                self._DISPLAYED_STATUSES,
            ).fetchone()
            return int(row["count"] if row else 0)

    def get_displayed_image_position(self, image_id: str) -> int:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT COUNT(*) AS pos FROM images
                WHERE created_at <= (SELECT created_at FROM images WHERE image_id = ?)
                AND status IN (?, ?)
                """,
                (image_id, *self._DISPLAYED_STATUSES),
            ).fetchone()
            return int(row["pos"] if row else 0)

    def _row_to_image(self, row: sqlite3.Row) -> ImageRecord:
        return ImageRecord(
            image_id=row["image_id"],
            telegram_file_id=row["telegram_file_id"],
            telegram_chat_id=row["telegram_chat_id"],
            local_original_path=row["local_original_path"],
            local_rendered_path=row["local_rendered_path"],
            location=row["location"],
            taken_at=row["taken_at"],
            caption=row["caption"],
            uploaded_by=row["uploaded_by"],
            created_at=row["created_at"],
            status=row["status"],
            last_error=row["last_error"],
        )

    def reconcile_pending_images(self) -> list[ImageRecord]:
        with self._lock:
            self._connection.execute(
                "UPDATE images SET status = 'queued' WHERE status = 'processing'"
            )
            rows = self._connection.execute(
                "SELECT * FROM images WHERE status = 'queued' ORDER BY created_at ASC"
            ).fetchall()
            self._connection.commit()
            return [self._row_to_image(row) for row in rows]

    def get_oldest_rendered_image(self) -> ImageRecord | None:
        """Return the oldest image with status 'rendered' (waiting for display)."""
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM images WHERE status = 'rendered' ORDER BY created_at ASC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            return self._row_to_image(row)

    def count_rendered_images(self) -> int:
        """Count images with status 'rendered' (queued for display after cooldown)."""
        with self._lock:
            row = self._connection.execute(
                "SELECT COUNT(*) AS count FROM images WHERE status = 'rendered'"
            ).fetchone()
            return int(row["count"] if row else 0)

    def get_next_images(self, current_image_id: str, n: int) -> list[ImageRecord]:
        """Get the next N displayed images after current_image_id, wrapping around."""
        with self._lock:
            current = self._connection.execute(
                "SELECT created_at FROM images WHERE image_id = ?", (current_image_id,)
            ).fetchone()
            if current is None:
                return []
            current_created_at = current["created_at"]

            rows = self._connection.execute(
                "SELECT * FROM images WHERE created_at > ? AND status IN (?, ?) ORDER BY created_at ASC LIMIT ?",
                (current_created_at, *self._DISPLAYED_STATUSES, n),
            ).fetchall()
            results = [self._row_to_image(r) for r in rows]

            if len(results) < n:
                remaining = n - len(results)
                wrap_rows = self._connection.execute(
                    "SELECT * FROM images WHERE created_at <= ? AND image_id != ? AND status IN (?, ?) ORDER BY created_at ASC LIMIT ?",
                    (current_created_at, current_image_id, *self._DISPLAYED_STATUSES, remaining),
                ).fetchall()
                results.extend(self._row_to_image(r) for r in wrap_rows)

            return results

    def get_whitelisted_users(self) -> list[dict]:
        """Return all whitelisted users ordered by created_at."""
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM users WHERE is_whitelisted = 1 ORDER BY created_at ASC"
            ).fetchall()
            return [dict(r) for r in rows]

    def remove_whitelist(self, telegram_user_id: int) -> bool:
        """Set is_whitelisted = 0 for the user. Returns True if the user existed."""
        with self._lock:
            cursor = self._connection.execute(
                "UPDATE users SET is_whitelisted = 0, is_admin = 0 WHERE telegram_user_id = ?",
                (telegram_user_id,),
            )
            self._connection.commit()
            return cursor.rowcount > 0

    def get_setting(self, key: str) -> str | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT value FROM settings WHERE key = ?",
                (key,),
            ).fetchone()
            return str(row["value"]) if row else None

    def set_setting(self, key: str, value: str) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO settings(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )
            self._connection.commit()
