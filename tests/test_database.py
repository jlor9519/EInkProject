from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path

from app.database import Database
from app.models import ImageRecord


class DatabaseTests(unittest.TestCase):
    def test_database_init_seed_and_latest_image(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Database(Path(tmpdir) / "frame.db")
            database.initialize()
            database.seed_admins([111])
            database.seed_whitelist([222])

            self.assertTrue(database.is_admin(111))
            self.assertTrue(database.is_whitelisted(111))
            self.assertTrue(database.is_whitelisted(222))

            record = ImageRecord(
                image_id="img-1",
                telegram_file_id="file-1",
                telegram_chat_id=111,
                local_original_path="/tmp/original.jpg",
                local_rendered_path="/tmp/rendered.png",
                location="Berlin",
                taken_at="2026-03-18",
                caption="A caption",
                uploaded_by=111,
                created_at="2026-03-18T12:00:00+00:00",
                status="displayed",
                last_error=None,
            )
            database.upsert_image(record)
            latest = database.get_latest_image()

            self.assertIsNotNone(latest)
            assert latest is not None
            self.assertEqual(latest.image_id, "img-1")
            self.assertEqual(latest.status, "displayed")


    def test_concurrent_upsert_image(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Database(Path(tmpdir) / "frame.db")
            database.initialize()
            database.seed_admins([111])
            errors: list[Exception] = []

            def upsert_record(index: int) -> None:
                try:
                    record = ImageRecord(
                        image_id=f"img-{index}",
                        telegram_file_id=f"file-{index}",
                        telegram_chat_id=111,
                        local_original_path=f"/tmp/original-{index}.jpg",
                        local_rendered_path=f"/tmp/rendered-{index}.png",
                        location="Berlin",
                        taken_at="2026-03-18",
                        caption=f"Caption {index}",
                        uploaded_by=111,
                        created_at=f"2026-03-18T12:00:{index:02d}+00:00",
                        status="displayed",
                        last_error=None,
                    )
                    database.upsert_image(record)
                except Exception as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=upsert_record, args=(i,)) for i in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            self.assertEqual(errors, [])
            latest = database.get_latest_image()
            self.assertIsNotNone(latest)

    def test_reconcile_pending_images_requeues_processing_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Database(Path(tmpdir) / "frame.db")
            database.initialize()
            database.upsert_image(
                ImageRecord(
                    image_id="img-queued",
                    telegram_file_id="file-queued",
                    telegram_chat_id=111,
                    local_original_path="/tmp/original-queued.jpg",
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="",
                    uploaded_by=111,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="queued",
                    last_error=None,
                )
            )
            database.upsert_image(
                ImageRecord(
                    image_id="img-processing",
                    telegram_file_id="file-processing",
                    telegram_chat_id=111,
                    local_original_path="/tmp/original-processing.jpg",
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="",
                    uploaded_by=111,
                    created_at="2026-03-18T12:05:00+00:00",
                    status="processing",
                    last_error=None,
                )
            )

            pending = database.reconcile_pending_images()

            self.assertEqual([record.image_id for record in pending], ["img-queued", "img-processing"])
            self.assertEqual(database.get_image_by_id("img-processing").status, "queued")


if __name__ == "__main__":
    unittest.main()
