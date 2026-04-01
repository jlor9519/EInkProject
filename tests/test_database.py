from __future__ import annotations

import sqlite3
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
                orientation_bucket="shared",
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
                        orientation_bucket="shared",
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
                    orientation_bucket="shared",
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
                    orientation_bucket="shared",
                )
            )

            pending = database.reconcile_pending_images()

            self.assertEqual([record.image_id for record in pending], ["img-queued", "img-processing"])
            self.assertEqual(database.get_image_by_id("img-processing").status, "queued")

    def test_existing_rows_default_to_shared_orientation_bucket(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Database(Path(tmpdir) / "frame.db")
            database.initialize()
            database._connection.execute(  # noqa: SLF001 - migration coverage
                """
                INSERT INTO images (
                    image_id, telegram_file_id, telegram_chat_id, local_original_path, local_rendered_path,
                    location, taken_at, caption, uploaded_by, created_at, status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "legacy",
                    "file-legacy",
                    1,
                    "/tmp/original.jpg",
                    None,
                    "",
                    "",
                    "",
                    1,
                    "2026-03-18T12:00:00+00:00",
                    "displayed",
                    None,
                ),
            )
            database._connection.commit()  # noqa: SLF001 - migration coverage
            database.initialize()

            record = database.get_image_by_id("legacy")

            self.assertIsNotNone(record)
            self.assertEqual(record.orientation_bucket, "shared")

    def test_initialize_migrates_true_legacy_images_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "frame.db"
            connection = sqlite3.connect(db_path)
            connection.execute(
                """
                CREATE TABLE images (
                    image_id TEXT PRIMARY KEY,
                    telegram_file_id TEXT NOT NULL,
                    local_original_path TEXT NOT NULL,
                    local_rendered_path TEXT,
                    location TEXT NOT NULL,
                    taken_at TEXT NOT NULL,
                    caption TEXT NOT NULL,
                    uploaded_by INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_error TEXT
                )
                """
            )
            connection.execute(
                """
                INSERT INTO images (
                    image_id, telegram_file_id, local_original_path, local_rendered_path,
                    location, taken_at, caption, uploaded_by, created_at, status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "legacy",
                    "file-legacy",
                    "/tmp/original.jpg",
                    None,
                    "",
                    "",
                    "",
                    1,
                    "2026-03-18T12:00:00+00:00",
                    "displayed",
                    None,
                ),
            )
            connection.commit()
            connection.close()

            database = Database(db_path)
            database.initialize()

            record = database.get_image_by_id("legacy")

            self.assertIsNotNone(record)
            assert record is not None
            self.assertEqual(record.telegram_chat_id, None)
            self.assertEqual(record.orientation_bucket, "shared")

    def test_maintenance_job_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Database(Path(tmpdir) / "frame.db")
            database.initialize()

            created = database.create_maintenance_job(
                job_id="job-1",
                kind="update",
                requested_by_user_id=111,
                telegram_chat_id=222,
                log_path="/tmp/job-1.log",
                unit_name="photo-frame-update-job-1",
            )
            self.assertEqual(created.status, "queued")
            self.assertEqual(database.get_active_maintenance_job().job_id, "job-1")

            database.mark_maintenance_job_running("job-1")
            self.assertEqual(database.get_maintenance_job("job-1").status, "running")

            database.mark_maintenance_job_finished("job-1", status="succeeded")
            finished = database.get_unnotified_finished_maintenance_jobs()
            self.assertEqual([job.job_id for job in finished], ["job-1"])

            database.mark_maintenance_job_notified("job-1")
            self.assertEqual(database.get_unnotified_finished_maintenance_jobs(), [])

    def test_recover_stale_queued_update_job_marks_it_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Database(Path(tmpdir) / "frame.db")
            database.initialize()
            database.create_maintenance_job(
                job_id="job-stale-queued",
                kind="update",
                requested_by_user_id=111,
                telegram_chat_id=222,
                log_path="/tmp/job-stale-queued.log",
                unit_name="photo-frame-update-job-stale",
            )

            recovered = database.recover_stale_update_jobs()
            job = database.get_maintenance_job("job-stale-queued")

            self.assertEqual([record.job_id for record in recovered], ["job-stale-queued"])
            self.assertIsNotNone(job)
            assert job is not None
            self.assertEqual(job.status, "failed")
            self.assertEqual(job.last_error, "stale maintenance job recovered after restart")
            self.assertIsNone(database.get_active_maintenance_job())

    def test_recover_stale_running_update_job_marks_it_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Database(Path(tmpdir) / "frame.db")
            database.initialize()
            database.create_maintenance_job(
                job_id="job-stale-running",
                kind="update",
                requested_by_user_id=111,
                telegram_chat_id=222,
                log_path="/tmp/job-stale-running.log",
                unit_name="photo-frame-update-job-stale",
            )
            database.mark_maintenance_job_running("job-stale-running")

            recovered = database.recover_stale_update_jobs()
            job = database.get_maintenance_job("job-stale-running")

            self.assertEqual([record.job_id for record in recovered], ["job-stale-running"])
            self.assertIsNotNone(job)
            assert job is not None
            self.assertEqual(job.status, "failed")
            self.assertEqual(job.last_error, "stale maintenance job recovered after restart")
            self.assertIsNone(database.get_active_maintenance_job())

    def test_get_all_displayed_images_ordered_wraps_around(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Database(Path(tmpdir) / "frame.db")
            database.initialize()
            for i, (img_id, bucket) in enumerate([
                ("img-a", "shared"),
                ("img-b", "vertical"),
                ("img-c", "shared"),
                ("img-d", "horizontal"),
            ]):
                database.upsert_image(
                    ImageRecord(
                        image_id=img_id,
                        telegram_file_id=f"file-{i}",
                        telegram_chat_id=1,
                        local_original_path=f"/tmp/{img_id}.jpg",
                        local_rendered_path=None,
                        location="",
                        taken_at="",
                        caption=f"Caption {img_id}",
                        uploaded_by=1,
                        created_at=f"2026-03-18T12:{i:02d}:00+00:00",
                        status="displayed",
                        last_error=None,
                        orientation_bucket=bucket,
                    )
                )

            # Starting from img-b in vertical pool: img-b, img-c, img-a (wrap)
            ordered = database.get_all_displayed_images_ordered("img-b", "vertical")
            self.assertEqual([r.image_id for r in ordered], ["img-b", "img-c", "img-a"])

            # Starting from img-c in vertical pool: img-c, img-a (wrap), img-b (wrap)
            ordered = database.get_all_displayed_images_ordered("img-c", "vertical")
            self.assertEqual([r.image_id for r in ordered], ["img-c", "img-a", "img-b"])

            # No orientation filter: all 4 images, starting from img-c
            ordered = database.get_all_displayed_images_ordered("img-c", None)
            self.assertEqual([r.image_id for r in ordered], ["img-c", "img-d", "img-a", "img-b"])

    def test_orientation_aware_queries_filter_active_library(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Database(Path(tmpdir) / "frame.db")
            database.initialize()
            records = [
                ImageRecord(
                    image_id="shared-displayed",
                    telegram_file_id="file-1",
                    telegram_chat_id=1,
                    local_original_path="/tmp/shared.jpg",
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="",
                    uploaded_by=1,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="shared",
                ),
                ImageRecord(
                    image_id="vertical-displayed",
                    telegram_file_id="file-2",
                    telegram_chat_id=1,
                    local_original_path="/tmp/vertical.jpg",
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="",
                    uploaded_by=1,
                    created_at="2026-03-18T12:10:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="vertical",
                ),
                ImageRecord(
                    image_id="horizontal-displayed",
                    telegram_file_id="file-3",
                    telegram_chat_id=1,
                    local_original_path="/tmp/horizontal.jpg",
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="",
                    uploaded_by=1,
                    created_at="2026-03-18T12:20:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="horizontal",
                ),
                ImageRecord(
                    image_id="vertical-rendered",
                    telegram_file_id="file-4",
                    telegram_chat_id=1,
                    local_original_path="/tmp/vertical-rendered.jpg",
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="",
                    uploaded_by=1,
                    created_at="2026-03-18T12:30:00+00:00",
                    status="rendered",
                    last_error=None,
                    orientation_bucket="vertical",
                ),
            ]
            for record in records:
                database.upsert_image(record)

            self.assertEqual(database.count_displayed_images("vertical"), 2)
            self.assertEqual(database.count_displayed_images("horizontal"), 2)
            self.assertEqual(database.count_rendered_images("vertical"), 1)
            self.assertEqual(database.count_rendered_images("horizontal"), 0)
            self.assertEqual(
                database.get_adjacent_image("shared-displayed", "next", "vertical").image_id,
                "vertical-displayed",
            )
            self.assertEqual(
                [record.image_id for record in database.get_next_images("shared-displayed", 3, "horizontal")],
                ["horizontal-displayed"],
            )
            self.assertEqual(database.get_displayed_image_position("vertical-displayed", "vertical"), 2)
            self.assertEqual(database.get_displayed_image_position("horizontal-displayed", "vertical"), 0)
            self.assertEqual(database.get_oldest_rendered_image_for_orientation("vertical").image_id, "vertical-rendered")
            self.assertEqual(database.get_oldest_rendered_image_for_orientation("horizontal"), None)
            self.assertEqual(
                database.get_newest_eligible_orientation_image("vertical").image_id,
                "vertical-rendered",
            )


if __name__ == "__main__":
    unittest.main()
