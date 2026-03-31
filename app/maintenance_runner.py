from __future__ import annotations

import argparse
import asyncio
import logging
import shutil
import subprocess
from pathlib import Path

from telegram import Bot

from app.config import load_config
from app.database import Database, utcnow_iso

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a detached photo-frame maintenance job")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument("--job-id", required=True, help="Maintenance job id")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    database = Database(config.database.path)
    database.initialize()

    job = database.get_maintenance_job(args.job_id)
    if job is None:
        raise SystemExit(f"Maintenance job {args.job_id} was not found.")

    log_path = Path(job.log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"[{utcnow_iso()}] Starting maintenance job {job.job_id} ({job.kind})\n")
        handle.flush()
        try:
            if job.kind == "update":
                _run_update(config.telegram.bot_token, database, job.job_id, handle)
            elif job.kind == "restart":
                _run_restart(database, job.job_id, handle)
            else:
                database.mark_maintenance_job_finished(
                    job.job_id,
                    status="failed",
                    last_error=f"Unknown maintenance job kind: {job.kind}",
                )
        except Exception as exc:
            logger.exception("Maintenance job %s crashed", job.job_id)
            handle.write(f"[{utcnow_iso()}] Job crashed: {exc}\n")
            handle.flush()
            database.mark_maintenance_job_finished(job.job_id, status="failed", last_error=str(exc))


def _run_update(bot_token: str, database: Database, job_id: str, handle) -> None:
    database.mark_maintenance_job_running(job_id)
    completed = subprocess.run(
        ["bash", str(PROJECT_ROOT / "scripts" / "update.sh")],
        cwd=PROJECT_ROOT,
        stdout=handle,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    if completed.returncode == 0:
        database.mark_maintenance_job_finished(job_id, status="succeeded")
        _notify_update_result(bot_token, database, job_id)
        return
    database.mark_maintenance_job_finished(
        job_id,
        status="failed",
        last_error=f"scripts/update.sh exited with code {completed.returncode}",
    )
    _notify_update_result(bot_token, database, job_id)


def _run_restart(database: Database, job_id: str, handle) -> None:
    reboot_bin = shutil.which("reboot") or "/sbin/reboot"
    database.mark_maintenance_job_running(job_id)
    database.mark_maintenance_job_rebooting(job_id)
    handle.write(f"[{utcnow_iso()}] Triggering reboot via {reboot_bin}\n")
    handle.flush()
    completed = subprocess.run(
        ["sudo", "-n", reboot_bin],
        cwd=PROJECT_ROOT,
        stdout=handle,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    if completed.returncode == 0:
        return
    database.mark_maintenance_job_finished(
        job_id,
        status="failed",
        last_error=f"reboot exited with code {completed.returncode}",
    )


def _notify_update_result(bot_token: str, database: Database, job_id: str) -> None:
    job = database.get_maintenance_job(job_id)
    if job is None or job.notified_at is not None:
        return

    action = "Update erfolgreich abgeschlossen." if job.status == "succeeded" else "Update fehlgeschlagen."
    details = _log_tail_summary(Path(job.log_path))
    if job.last_error and job.last_error not in details:
        details = f"{job.last_error}\n{details}".strip()
    text = f"{action}\n\nLetzte Ausgaben:\n{details}" if details else action

    try:
        asyncio.run(Bot(token=bot_token).send_message(job.telegram_chat_id, text))
    except Exception:
        logger.exception("Failed to send update completion message for %s", job_id)
        return
    database.mark_maintenance_job_notified(job_id)


def _log_tail_summary(log_path: Path, *, max_lines: int = 8, max_chars: int = 1200) -> str:
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ""
    if not lines:
        return ""
    tail = "\n".join(lines[-max_lines:])
    if len(tail) > max_chars:
        tail = tail[-max_chars:]
    return tail.strip()


if __name__ == "__main__":
    main()
