from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.database import Database
from app.display_state import DISPLAY_TRANSITION_KEYS
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

    def test_initialize_enables_sqlite_pragmas(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Database(Path(tmpdir) / "frame.db")
            database.initialize()

            journal_mode = database._connection.execute("PRAGMA journal_mode").fetchone()[0]  # noqa: SLF001
            busy_timeout = database._connection.execute("PRAGMA busy_timeout").fetchone()[0]  # noqa: SLF001
            foreign_keys = database._connection.execute("PRAGMA foreign_keys").fetchone()[0]  # noqa: SLF001

            self.assertEqual(str(journal_mode).lower(), "wal")
            self.assertEqual(int(busy_timeout), 5000)
            self.assertEqual(int(foreign_keys), 1)

    def test_reconcile_runtime_state_promotes_payload_image_and_requeues_other_processing_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Database(Path(tmpdir) / "frame.db")
            database.initialize()
            current = ImageRecord(
                image_id="img-current",
                telegram_file_id="file-current",
                telegram_chat_id=111,
                local_original_path="/tmp/current.jpg",
                local_rendered_path="/tmp/current.png",
                location="",
                taken_at="",
                caption="",
                uploaded_by=111,
                created_at="2026-03-18T12:00:00+00:00",
                status="processing",
                last_error=None,
                orientation_bucket="shared",
            )
            other = ImageRecord(
                image_id="img-other",
                telegram_file_id="file-other",
                telegram_chat_id=111,
                local_original_path="/tmp/other.jpg",
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
            database.upsert_image(current)
            database.upsert_image(other)
            database.set_settings(
                {
                    "display_transition_image_id": "img-current",
                    "display_transition_started_at": "2026-03-18T12:00:00+00:00",
                    "display_transition_kind": "upload",
                }
            )

            pending = database.reconcile_runtime_state(
                "img-current",
                transition_keys=DISPLAY_TRANSITION_KEYS,
            )

            self.assertEqual(database.get_image_by_id("img-current").status, "displayed")
            self.assertEqual(database.get_image_by_id("img-other").status, "queued")
            self.assertEqual([record.image_id for record in pending], ["img-other"])
            self.assertIsNotNone(database.get_setting("current_image_displayed_at"))
            for key in DISPLAY_TRANSITION_KEYS:
                self.assertIsNone(database.get_setting(key))

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

    def test_recover_stale_queued_update_job_marks_it_failed_in_startup_mode(self) -> None:
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

    def test_recover_stale_running_update_job_marks_it_failed_in_startup_mode(self) -> None:
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

    def test_recover_stale_update_jobs_recovers_only_old_queued_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Database(Path(tmpdir) / "frame.db")
            database.initialize()
            database.create_maintenance_job(
                job_id="job-old-queued",
                kind="update",
                requested_by_user_id=111,
                telegram_chat_id=222,
                log_path="/tmp/job-old-queued.log",
                unit_name="photo-frame-update-job-old",
            )
            database.create_maintenance_job(
                job_id="job-fresh-queued",
                kind="update",
                requested_by_user_id=111,
                telegram_chat_id=222,
                log_path="/tmp/job-fresh-queued.log",
                unit_name="photo-frame-update-job-fresh",
            )
            old_created_at = (datetime.now(timezone.utc) - timedelta(seconds=61)).isoformat()
            database._connection.execute(  # noqa: SLF001 - test setup
                "UPDATE maintenance_jobs SET created_at = ? WHERE job_id = ?",
                (old_created_at, "job-old-queued"),
            )
            database._connection.commit()  # noqa: SLF001 - test setup

            recovered = database.recover_stale_update_jobs(
                max_queued_age_seconds=60,
                max_running_age_seconds=600,
                reason="stale maintenance job recovered before new update request",
            )

            self.assertEqual([job.job_id for job in recovered], ["job-old-queued"])
            self.assertEqual(database.get_maintenance_job("job-old-queued").status, "failed")
            self.assertEqual(database.get_maintenance_job("job-fresh-queued").status, "queued")

    def test_recover_stale_update_jobs_keeps_fresh_queued_jobs_active(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Database(Path(tmpdir) / "frame.db")
            database.initialize()
            database.create_maintenance_job(
                job_id="job-fresh-queued",
                kind="update",
                requested_by_user_id=111,
                telegram_chat_id=222,
                log_path="/tmp/job-fresh-queued.log",
                unit_name="photo-frame-update-job-fresh",
            )

            recovered = database.recover_stale_update_jobs(
                max_queued_age_seconds=60,
                max_running_age_seconds=600,
                reason="stale maintenance job recovered before new update request",
            )

            self.assertEqual(recovered, [])
            self.assertEqual(database.get_maintenance_job("job-fresh-queued").status, "queued")

    def test_recover_stale_update_jobs_recovers_only_old_running_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Database(Path(tmpdir) / "frame.db")
            database.initialize()
            database.create_maintenance_job(
                job_id="job-old-running",
                kind="update",
                requested_by_user_id=111,
                telegram_chat_id=222,
                log_path="/tmp/job-old-running.log",
                unit_name="photo-frame-update-job-old",
            )
            database.create_maintenance_job(
                job_id="job-fresh-running",
                kind="update",
                requested_by_user_id=111,
                telegram_chat_id=222,
                log_path="/tmp/job-fresh-running.log",
                unit_name="photo-frame-update-job-fresh",
            )
            database.mark_maintenance_job_running("job-old-running")
            database.mark_maintenance_job_running("job-fresh-running")
            old_started_at = (datetime.now(timezone.utc) - timedelta(minutes=11)).isoformat()
            database._connection.execute(  # noqa: SLF001 - test setup
                "UPDATE maintenance_jobs SET started_at = ? WHERE job_id = ?",
                (old_started_at, "job-old-running"),
            )
            database._connection.commit()  # noqa: SLF001 - test setup

            recovered = database.recover_stale_update_jobs(
                max_queued_age_seconds=60,
                max_running_age_seconds=600,
                reason="stale maintenance job recovered before new update request",
            )

            self.assertEqual([job.job_id for job in recovered], ["job-old-running"])
            self.assertEqual(database.get_maintenance_job("job-old-running").status, "failed")
            self.assertEqual(database.get_maintenance_job("job-fresh-running").status, "running")

    def test_recover_stale_update_jobs_keeps_fresh_running_jobs_active(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Database(Path(tmpdir) / "frame.db")
            database.initialize()
            database.create_maintenance_job(
                job_id="job-fresh-running",
                kind="update",
                requested_by_user_id=111,
                telegram_chat_id=222,
                log_path="/tmp/job-fresh-running.log",
                unit_name="photo-frame-update-job-fresh",
            )
            database.mark_maintenance_job_running("job-fresh-running")

            recovered = database.recover_stale_update_jobs(
                max_queued_age_seconds=60,
                max_running_age_seconds=600,
                reason="stale maintenance job recovered before new update request",
            )

            self.assertEqual(recovered, [])
            self.assertEqual(database.get_maintenance_job("job-fresh-running").status, "running")

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

    def test_rotation_limit_uses_newest_images_and_restores_hidden_images_when_increased(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Database(Path(tmpdir) / "frame.db")
            database.initialize()
            database.set_setting("rotation_limit", "2")
            records = [
                ImageRecord(
                    image_id="shared-old",
                    telegram_file_id="file-1",
                    telegram_chat_id=1,
                    local_original_path="/tmp/shared-old.jpg",
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
                    image_id="vertical-mid",
                    telegram_file_id="file-2",
                    telegram_chat_id=1,
                    local_original_path="/tmp/vertical-mid.jpg",
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
                    image_id="vertical-newest",
                    telegram_file_id="file-3",
                    telegram_chat_id=1,
                    local_original_path="/tmp/vertical-newest.jpg",
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="",
                    uploaded_by=1,
                    created_at="2026-03-18T12:20:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="vertical",
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
                ImageRecord(
                    image_id="horizontal-new",
                    telegram_file_id="file-5",
                    telegram_chat_id=1,
                    local_original_path="/tmp/horizontal-new.jpg",
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="",
                    uploaded_by=1,
                    created_at="2026-03-18T12:40:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="horizontal",
                ),
            ]
            for record in records:
                database.upsert_image(record)

            self.assertEqual(database.get_rotation_limit(), 2)
            self.assertEqual(database.count_rotation_pool_images("vertical"), 2)
            self.assertEqual(database.count_hidden_rotation_images("vertical"), 2)
            self.assertEqual(database.count_displayed_images("vertical"), 1)
            self.assertEqual(database.count_rendered_images("vertical"), 1)
            self.assertFalse(database.is_image_in_rotation_pool("shared-old", "vertical"))
            self.assertTrue(database.is_image_in_rotation_pool("vertical-rendered", "vertical"))
            self.assertEqual(
                database.get_adjacent_image("shared-old", "next", "vertical").image_id,
                "vertical-newest",
            )
            self.assertEqual(
                [record.image_id for record in database.get_next_images("shared-old", 3, "vertical")],
                ["vertical-newest"],
            )
            self.assertEqual(
                [record.image_id for record in database.get_all_displayed_images_ordered("shared-old", "vertical")],
                ["vertical-newest"],
            )
            self.assertEqual(database.count_rotation_pool_images("horizontal"), 2)
            self.assertEqual(database.count_hidden_rotation_images("horizontal"), 0)

            database.set_setting("rotation_limit", "4")

            self.assertEqual(database.count_rotation_pool_images("vertical"), 4)
            self.assertEqual(database.count_hidden_rotation_images("vertical"), 0)
            self.assertTrue(database.is_image_in_rotation_pool("shared-old", "vertical"))
            self.assertEqual(database.count_displayed_images("vertical"), 3)
            self.assertEqual(
                [record.image_id for record in database.get_all_displayed_images_ordered("shared-old", "vertical")],
                ["shared-old", "vertical-mid", "vertical-newest"],
            )


if __name__ == "__main__":
    unittest.main()
