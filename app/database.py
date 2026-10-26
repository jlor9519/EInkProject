from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

from app.models import ImageRecord
from app.orientation import ACTIVE_ORIENTATIONS, ORIENTATION_SHARED, orientation_pool

DEFAULT_ROTATION_LIMIT = 100


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
        self._configure_connection()

    def _configure_connection(self) -> None:
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA busy_timeout=5000")
        self._connection.execute("PRAGMA synchronous=NORMAL")
        self._connection.execute("PRAGMA foreign_keys=ON")

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
                    last_error TEXT,
                    orientation_bucket TEXT NOT NULL DEFAULT 'shared',
                    display_attempts INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS error_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    source TEXT NOT NULL,
                    message TEXT NOT NULL,
                    image_id TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_images_created_at ON images(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_error_log_timestamp ON error_log(timestamp DESC);
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
            if "orientation_bucket" not in columns:
                self._connection.execute(
                    "ALTER TABLE images ADD COLUMN orientation_bucket TEXT NOT NULL DEFAULT 'shared'"
                )
            if "display_attempts" not in columns:
                self._connection.execute(
                    "ALTER TABLE images ADD COLUMN display_attempts INTEGER NOT NULL DEFAULT 0"
                )
            self._connection.execute(
                "UPDATE images SET orientation_bucket = ? WHERE orientation_bucket IS NULL OR orientation_bucket = ''",
                (ORIENTATION_SHARED,),
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_images_orientation ON images(orientation_bucket)"
            )
            self._connection.execute(
                """
                INSERT INTO settings(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO NOTHING
                """,
                ("rotation_limit", str(DEFAULT_ROTATION_LIMIT)),
            )
            self._connection.commit()
        logger.info("Database initialized at %s", self.db_path)

    def healthcheck(self) -> bool:
        details = self.health_details()
        return bool(details["read_ok"] and details["write_ok"])

    def health_details(self) -> dict[str, bool]:
        with self._lock:
            read_ok = False
            write_ok = False
            try:
                row = self._connection.execute("SELECT 1").fetchone()
                read_ok = bool(row and row[0] == 1)
            except sqlite3.DatabaseError:
                logger.exception("Database read healthcheck failed")

            if read_ok:
                try:
                    self._connection.execute("SAVEPOINT healthcheck")
                    self._connection.execute(
                        """
                        INSERT INTO settings(key, value) VALUES(?, ?)
                        ON CONFLICT(key) DO UPDATE SET value = excluded.value
                        """,
                        ("__healthcheck__", utcnow_iso()),
                    )
                    self._connection.execute("ROLLBACK TO healthcheck")
                    self._connection.execute("RELEASE healthcheck")
                    write_ok = True
                except sqlite3.DatabaseError:
                    logger.exception("Database write healthcheck failed")
                    try:
                        self._connection.execute("ROLLBACK")
                    except sqlite3.DatabaseError:
                        logger.exception("Database rollback after failed healthcheck also failed")

            return {"read_ok": read_ok, "write_ok": write_ok}

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
            self._upsert_image_locked(record)
            self._connection.commit()

    def apply_image_and_settings(
        self,
        record: ImageRecord | None = None,
        *,
        settings: dict[str, str | None] | None = None,
        clear_keys: tuple[str, ...] = (),
    ) -> None:
        with self._lock:
            if record is not None:
                self._upsert_image_locked(record)
            if settings:
                self._set_settings_locked(settings)
            if clear_keys:
                self._connection.executemany(
                    "DELETE FROM settings WHERE key = ?",
                    ((key,) for key in clear_keys),
                )
            self._connection.commit()

    def update_image_status(self, image_id: str, status: str, last_error: str | None) -> bool:
        """Update status and error of an existing image. Unlike upsert_image, never re-creates
        a row that was deleted in the meantime. Returns True if the image still exists."""
        with self._lock:
            cursor = self._connection.execute(
                "UPDATE images SET status = ?, last_error = ? WHERE image_id = ?",
                (status, last_error, image_id),
            )
            self._connection.commit()
            return cursor.rowcount > 0

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
    _ROTATION_ELIGIBLE_STATUSES = ("queued", "processing", "rendered", "displayed", "displayed_with_warnings")

    def count_rotation_pool_images(self, active_orientation: str | None = None) -> int:
        with self._lock:
            return len(self._get_rotation_pool_rows_locked(active_orientation))

    def count_hidden_rotation_images(self, active_orientation: str | None = None) -> int:
        with self._lock:
            total = self._count_rotation_eligible_images_locked(active_orientation)
            active = len(self._get_rotation_pool_rows_locked(active_orientation))
            return max(0, total - active)

    def is_image_in_rotation_pool(self, image_id: str, active_orientation: str | None = None) -> bool:
        with self._lock:
            return any(
                row["image_id"] == image_id
                for row in self._get_rotation_pool_rows_locked(active_orientation)
            )

    def get_adjacent_image(self, current_image_id: str, direction: str, active_orientation: str | None = None) -> ImageRecord | None:
        with self._lock:
            pool_rows = [
                row
                for row in self._get_rotation_pool_rows_locked(active_orientation)
                if row["status"] in self._DISPLAYED_STATUSES
            ]
            row = self._select_relative_row_locked(
                current_image_id,
                pool_rows,
                direction=direction,
            )
            if row is None:
                return None
            return self._row_to_image(row)

    def count_displayed_images(self, active_orientation: str | None = None) -> int:
        with self._lock:
            return sum(
                1
                for row in self._get_rotation_pool_rows_locked(active_orientation)
                if row["status"] in self._DISPLAYED_STATUSES
            )

    def get_displayed_image_position(self, image_id: str, active_orientation: str | None = None) -> int:
        with self._lock:
            displayed_rows = [
                row
                for row in self._get_rotation_pool_rows_locked(active_orientation)
                if row["status"] in self._DISPLAYED_STATUSES
            ]
            for index, row in enumerate(displayed_rows, start=1):
                if row["image_id"] == image_id:
                    return index
            return 0

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
            orientation_bucket=row["orientation_bucket"] or ORIENTATION_SHARED,
            display_attempts=int(row["display_attempts"] or 0),
        )

    def reconcile_runtime_state(
        self,
        current_image_id: str | None,
        *,
        transition_keys: tuple[str, ...] = (),
    ) -> list[ImageRecord]:
        promoted_to_displayed = False
        with self._lock:
            if current_image_id:
                current_row = self._connection.execute(
                    "SELECT * FROM images WHERE image_id = ?",
                    (current_image_id,),
                ).fetchone()
                if current_row is not None:
                    current_status = str(current_row["status"] or "")
                    current_last_error = current_row["last_error"]
                    if current_status not in self._DISPLAYED_STATUSES:
                        promoted_status = "displayed_with_warnings" if current_last_error else "displayed"
                        self._connection.execute(
                            """
                            UPDATE images
                            SET status = ?
                            WHERE image_id = ?
                            """,
                            (promoted_status, current_image_id),
                        )
                        promoted_to_displayed = True
                    displayed_at = self._connection.execute(
                        "SELECT value FROM settings WHERE key = ?",
                        ("current_image_displayed_at",),
                    ).fetchone()
                    if displayed_at is None:
                        self._set_settings_locked({"current_image_displayed_at": utcnow_iso()})

            if current_image_id:
                self._connection.execute(
                    "UPDATE images SET status = 'queued' WHERE status = 'processing' AND image_id != ?",
                    (current_image_id,),
                )
            else:
                self._connection.execute(
                    "UPDATE images SET status = 'queued' WHERE status = 'processing'"
                )

            if transition_keys:
                self._connection.executemany(
                    "DELETE FROM settings WHERE key = ?",
                    ((key,) for key in transition_keys),
                )

            rows = self._connection.execute(
                "SELECT * FROM images WHERE status = 'queued' ORDER BY created_at ASC"
            ).fetchall()
            self._connection.commit()

        if promoted_to_displayed:
            logger.warning(
                "Recovered display state from payload by promoting %s to displayed on startup",
                current_image_id,
            )
        return [self._row_to_image(row) for row in rows]

    def get_oldest_rendered_image_for_orientation(self, active_orientation: str | None = None) -> ImageRecord | None:
        """Return the oldest rendered image visible in the active orientation pool."""
        with self._lock:
            # The pool is ordered newest-first; walk it backwards so the queue is first-in, first-out.
            row = next(
                (
                    candidate
                    for candidate in reversed(self._get_rotation_pool_rows_locked(active_orientation))
                    if candidate["status"] == "rendered"
                ),
                None,
            )
            if row is None:
                return None
            return self._row_to_image(row)

    def count_rendered_images(self, active_orientation: str | None = None) -> int:
        """Count images with status 'rendered' (queued for display after cooldown)."""
        with self._lock:
            return sum(
                1
                for row in self._get_rotation_pool_rows_locked(active_orientation)
                if row["status"] == "rendered"
            )

    def get_next_images(self, current_image_id: str, n: int, active_orientation: str | None = None) -> list[ImageRecord]:
        """Get the next N displayed images after current_image_id, wrapping around."""
        with self._lock:
            current = self._connection.execute(
                "SELECT created_at FROM images WHERE image_id = ?", (current_image_id,)
            ).fetchone()
            if current is None:
                return []
            displayed_rows = [
                row
                for row in self._get_rotation_pool_rows_locked(active_orientation)
                if row["status"] in self._DISPLAYED_STATUSES
            ]
            if not displayed_rows:
                return []

            current_index = next(
                (index for index, row in enumerate(displayed_rows) if row["image_id"] == current_image_id),
                None,
            )
            if current_index is not None:
                if len(displayed_rows) == 1:
                    return []
                ordered = displayed_rows[current_index + 1:] + displayed_rows[:current_index]
            else:
                start_index = self._get_rotation_start_index_locked(
                    displayed_rows,
                    current_image_id,
                    current["created_at"],
                    include_current=False,
                )
                if start_index is None:
                    return []
                ordered = displayed_rows[start_index:] + displayed_rows[:start_index]
            return [self._row_to_image(row) for row in ordered[:n]]

    def get_all_displayed_images_ordered(self, current_image_id: str, active_orientation: str | None = None) -> list[ImageRecord]:
        """Return all displayed images in newest-first slideshow order, starting from current_image_id."""
        with self._lock:
            current = self._connection.execute(
                "SELECT created_at FROM images WHERE image_id = ?", (current_image_id,)
            ).fetchone()
            if current is None:
                return []
            displayed_rows = [
                row
                for row in self._get_rotation_pool_rows_locked(active_orientation)
                if row["status"] in self._DISPLAYED_STATUSES
            ]
            if not displayed_rows:
                return []

            start_index = self._get_rotation_start_index_locked(
                displayed_rows,
                current_image_id,
                current["created_at"],
                include_current=True,
            )
            if start_index is None:
                return []

            ordered = displayed_rows[start_index:] + displayed_rows[:start_index]
            return [self._row_to_image(row) for row in ordered]

    def get_newest_eligible_orientation_image(self, active_orientation: str) -> ImageRecord | None:
        with self._lock:
            row = next(
                (
                    candidate
                    for candidate in self._get_rotation_pool_rows_locked(active_orientation)
                    if candidate["status"] in ("rendered", *self._DISPLAYED_STATUSES)
                ),
                None,
            )
            if row is None:
                return None
            return self._row_to_image(row)

    def get_images_excluding(self, image_id: str | None) -> list[ImageRecord]:
        with self._lock:
            if image_id:
                rows = self._connection.execute(
                    "SELECT * FROM images WHERE image_id != ? ORDER BY created_at ASC",
                    (image_id,),
                ).fetchall()
            else:
                rows = self._connection.execute(
                    "SELECT * FROM images ORDER BY created_at ASC"
                ).fetchall()
            return [self._row_to_image(row) for row in rows]

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
            self._set_settings_locked({key: value})
            self._connection.commit()

    def set_settings(self, values: dict[str, str | None]) -> None:
        with self._lock:
            self._set_settings_locked(values)
            self._connection.commit()

    _ERROR_LOG_MAX_ROWS = 50

    def log_error(self, source: str, message: str, *, image_id: str | None = None) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self._connection.execute(
                "INSERT INTO error_log (timestamp, source, message, image_id) VALUES (?, ?, ?, ?)",
                (ts, source, message, image_id),
            )
            self._connection.execute(
                "DELETE FROM error_log WHERE id NOT IN (SELECT id FROM error_log ORDER BY id DESC LIMIT ?)",
                (self._ERROR_LOG_MAX_ROWS,),
            )
            self._connection.commit()

    def get_recent_errors(self, limit: int = 5, *, since: str | None = None) -> list[dict[str, str | None]]:
        with self._lock:
            if since is not None:
                rows = self._connection.execute(
                    "SELECT timestamp, source, message, image_id FROM error_log "
                    "WHERE timestamp >= ? ORDER BY id DESC LIMIT ?",
                    (since, limit),
                ).fetchall()
            else:
                rows = self._connection.execute(
                    "SELECT timestamp, source, message, image_id FROM error_log ORDER BY id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _orientation_args(active_orientation: str | None) -> tuple[str, ...]:
        if active_orientation not in ACTIVE_ORIENTATIONS:
            return ()
        return orientation_pool(active_orientation)

    @staticmethod
    def _orientation_filter_sql(active_orientation: str | None) -> str:
        if active_orientation not in ACTIVE_ORIENTATIONS:
            return ""
        return " AND orientation_bucket IN (?, ?)"

    def _get_rotation_limit_locked(self) -> int | None:
        row = self._connection.execute(
            "SELECT value FROM settings WHERE key = ?",
            ("rotation_limit",),
        ).fetchone()
        raw_value = row["value"] if row else str(DEFAULT_ROTATION_LIMIT)
        try:
            parsed = int(str(raw_value))
        except (TypeError, ValueError):
            return DEFAULT_ROTATION_LIMIT
        if parsed <= 0:
            return None
        return parsed

    def _count_rotation_eligible_images_locked(self, active_orientation: str | None) -> int:
        orientation_args = self._orientation_args(active_orientation)
        placeholders = ", ".join("?" for _ in self._ROTATION_ELIGIBLE_STATUSES)
        row = self._connection.execute(
            f"""
            SELECT COUNT(*) AS count
            FROM images
            WHERE status IN ({placeholders})
            {self._orientation_filter_sql(active_orientation)}
            """,
            (*self._ROTATION_ELIGIBLE_STATUSES, *orientation_args),
        ).fetchone()
        return int(row["count"] if row else 0)

    def _get_rotation_pool_rows_locked(self, active_orientation: str | None) -> list[sqlite3.Row]:
        orientation_args = self._orientation_args(active_orientation)
        placeholders = ", ".join("?" for _ in self._ROTATION_ELIGIBLE_STATUSES)
        limit = self._get_rotation_limit_locked()
        if limit is None:
            rows = self._connection.execute(
                f"""
                SELECT *
                FROM images
                WHERE status IN ({placeholders})
                {self._orientation_filter_sql(active_orientation)}
                ORDER BY created_at DESC
                """,
                (*self._ROTATION_ELIGIBLE_STATUSES, *orientation_args),
            ).fetchall()
        else:
            rows = self._connection.execute(
                f"""
                SELECT *
                FROM images
                WHERE status IN ({placeholders})
                {self._orientation_filter_sql(active_orientation)}
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (*self._ROTATION_ELIGIBLE_STATUSES, *orientation_args, limit),
            ).fetchall()
        return list(rows)

    def _select_relative_row_locked(
        self,
        current_image_id: str,
        rows: list[sqlite3.Row],
        *,
        direction: str,
    ) -> sqlite3.Row | None:
        current = self._connection.execute(
            "SELECT created_at FROM images WHERE image_id = ?",
            (current_image_id,),
        ).fetchone()
        if current is None or not rows:
            return None

        current_created_at = current["created_at"]
        current_index = next(
            (index for index, row in enumerate(rows) if row["image_id"] == current_image_id),
            None,
        )
        if current_index is not None:
            if len(rows) == 1:
                return None
            if direction == "next":
                return rows[(current_index + 1) % len(rows)]
            return rows[(current_index - 1) % len(rows)]

        if direction == "next":
            for row in rows:
                if row["created_at"] < current_created_at:
                    return row
            return rows[0]

        for row in reversed(rows):
            if row["created_at"] > current_created_at:
                return row
        return rows[-1]

    @staticmethod
    def _get_rotation_start_index_locked(
        rows: list[sqlite3.Row],
        current_image_id: str,
        current_created_at: str,
        *,
        include_current: bool,
    ) -> int | None:
        current_index = next(
            (index for index, row in enumerate(rows) if row["image_id"] == current_image_id),
            None,
        )
        if current_index is not None:
            if include_current:
                return current_index
            if len(rows) == 1:
                return None
            return (current_index + 1) % len(rows)

        for index, row in enumerate(rows):
            if row["created_at"] < current_created_at:
                return index
        return 0

    def _upsert_image_locked(self, record: ImageRecord) -> None:
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
                last_error,
                orientation_bucket,
                display_attempts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                last_error = excluded.last_error,
                orientation_bucket = excluded.orientation_bucket,
                display_attempts = excluded.display_attempts
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
                record.orientation_bucket,
                record.display_attempts,
            ),
        )

    def _set_settings_locked(self, values: dict[str, str | None]) -> None:
        for key, value in values.items():
            if value is None:
                self._connection.execute("DELETE FROM settings WHERE key = ?", (key,))
                continue
            self._connection.execute(
                """
                INSERT INTO settings(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )
