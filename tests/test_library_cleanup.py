from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from app.database import Database
from app.library_cleanup import clear_non_current_images
from app.models import ImageRecord


class LibraryCleanupTests(unittest.TestCase):
    def test_clear_non_current_images_preserves_current_payload_image(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            database = Database(tmpdir_path / "frame.db")
            database.initialize()

            payload_path = tmpdir_path / "inkypi" / "current.json"
            payload_path.parent.mkdir(parents=True, exist_ok=True)
            payload_path.write_text(json.dumps({"image_id": "img-current"}), encoding="utf-8")

            current_original = tmpdir_path / "incoming" / "img-current.jpg"
            current_rendered = tmpdir_path / "rendered" / "img-current.png"
            old_original = tmpdir_path / "incoming" / "img-old.jpg"
            old_rendered = tmpdir_path / "rendered" / "img-old.png"
            for path in (current_original, current_rendered, old_original, old_rendered):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(path.name.encode("utf-8"))

            database.upsert_image(
                ImageRecord(
                    image_id="img-current",
                    telegram_file_id="file-current",
                    telegram_chat_id=1,
                    local_original_path=str(current_original),
                    local_rendered_path=str(current_rendered),
                    location="",
                    taken_at="",
                    caption="Current",
                    uploaded_by=1,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            database.upsert_image(
                ImageRecord(
                    image_id="img-old",
                    telegram_file_id="file-old",
                    telegram_chat_id=1,
                    local_original_path=str(old_original),
                    local_rendered_path=str(old_rendered),
                    location="",
                    taken_at="",
                    caption="Old",
                    uploaded_by=1,
                    created_at="2026-03-18T12:05:00+00:00",
                    status="rendered",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )

            summary = clear_non_current_images(database, payload_path)

            self.assertEqual(summary.preserved_image_id, "img-current")
            self.assertEqual(summary.deleted_images, 1)
            self.assertEqual(summary.deleted_files, 2)
            self.assertIsNotNone(database.get_image_by_id("img-current"))
            self.assertIsNone(database.get_image_by_id("img-old"))
            self.assertTrue(current_original.exists())
            self.assertTrue(current_rendered.exists())
            self.assertFalse(old_original.exists())
            self.assertFalse(old_rendered.exists())


if __name__ == "__main__":
    unittest.main()
