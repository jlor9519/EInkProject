from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from telegram.error import TimedOut

from app.conversations import PENDING_SUBMISSION_KEY, WAITING_FOR_TEXT_CHOICE, _submit_photo, photo_entry, process_queued_upload
from app.database import Database
from app.models import DisplayResult, ImageRecord


class UploadFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_photo_entry_prefills_caption_from_telegram_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            application = _FakeApplication(services)
            update = _FakeUpdate(user_id=1, chat_id=101, photo_token="one", caption="Schon im Foto")
            context = _FakeContext(application)

            result = await photo_entry(update, context)

            self.assertEqual(result, WAITING_FOR_TEXT_CHOICE)
            self.assertEqual(context.user_data[PENDING_SUBMISSION_KEY]["caption"], "Schon im Foto")

    async def test_photo_entry_and_submit_queue_multiple_users_fifo(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            application = _FakeApplication(services)

            update_one = _FakeUpdate(user_id=1, chat_id=101, photo_token="one")
            update_two = _FakeUpdate(user_id=2, chat_id=202, photo_token="two")
            context_one = _FakeContext(application)
            context_two = _FakeContext(application)

            result_one = await photo_entry(update_one, context_one)
            result_two = await photo_entry(update_two, context_two)

            self.assertEqual(result_one, WAITING_FOR_TEXT_CHOICE)
            self.assertEqual(result_two, WAITING_FOR_TEXT_CHOICE)
            self.assertNotIn("Ein anderes Foto wird gerade verarbeitet", "".join(update_two.effective_message.replies))

            image_id_one = context_one.user_data[PENDING_SUBMISSION_KEY]["image_id"]
            image_id_two = context_two.user_data[PENDING_SUBMISSION_KEY]["image_id"]

            await _submit_photo(update_one, context_one, show_caption=False)
            await _submit_photo(update_two, context_two, show_caption=False)

            queue: asyncio.Queue[str] = application.bot_data["upload_queue"]
            self.assertEqual(await queue.get(), image_id_one)
            self.assertEqual(await queue.get(), image_id_two)

            queued_one = services.database.get_image_by_id(image_id_one)
            queued_two = services.database.get_image_by_id(image_id_two)
            self.assertEqual(queued_one.status, "queued")
            self.assertEqual(queued_two.status, "queued")
            self.assertEqual(queued_one.telegram_chat_id, 101)
            self.assertEqual(queued_two.telegram_chat_id, 202)

    async def test_process_queued_upload_keeps_image_listed_when_completion_message_times_out(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(
                tmpdir_path,
                bot=_FakeBot(send_side_effect=TimedOut("timed out")),
            )
            application = _FakeApplication(services)

            original = services.storage.original_path("img-1")
            original.parent.mkdir(parents=True, exist_ok=True)
            original.write_bytes(b"original")
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-1",
                    telegram_file_id="file-1",
                    telegram_chat_id=555,
                    local_original_path=str(original),
                    local_rendered_path=None,
                    location="Berlin",
                    taken_at="2026-03-18",
                    caption="Caption",
                    uploaded_by=1,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="queued",
                    last_error=None,
                )
            )

            await process_queued_upload(application, "img-1")

            record = services.database.get_image_by_id("img-1")
            self.assertEqual(record.status, "displayed_with_warnings")
            self.assertEqual(services.database.count_displayed_images(), 1)
            self.assertIn("Telegram-Benachrichtigung fehlgeschlagen", record.last_error)

    async def test_process_queued_upload_waits_for_display_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            display = _FakeDisplay()
            services = _build_services(tmpdir_path, display=display)
            application = _FakeApplication(services)

            original = services.storage.original_path("img-1")
            original.parent.mkdir(parents=True, exist_ok=True)
            original.write_bytes(b"original")
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-1",
                    telegram_file_id="file-1",
                    telegram_chat_id=555,
                    local_original_path=str(original),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="",
                    uploaded_by=1,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="queued",
                    last_error=None,
                )
            )

            lock: asyncio.Lock = application.bot_data["display_lock"]
            await lock.acquire()
            task = asyncio.create_task(process_queued_upload(application, "img-1"))
            await asyncio.sleep(0.05)
            self.assertEqual(display.calls, 0)

            lock.release()
            await task
            self.assertEqual(display.calls, 1)


class _FakeAuth:
    def sync_user(self, user) -> None:
        return None

    def is_whitelisted(self, user_id: int) -> bool:
        return True


class _FakeStorage:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self._counter = 0

    def generate_image_id(self) -> str:
        self._counter += 1
        return f"img-{self._counter}"

    def original_path(self, image_id: str, extension: str = ".jpg") -> Path:
        return self.base_dir / "incoming" / f"{image_id}{extension}"

    def rendered_path(self, image_id: str, extension: str = ".png") -> Path:
        return self.base_dir / "rendered" / f"{image_id}{extension}"

    def cleanup_rendered_cache(self) -> None:
        return None


class _FakeRenderer:
    def render(self, original_path: Path, output_path: Path, **_: object) -> Path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(original_path.read_bytes())
        return output_path


class _FakeDisplay:
    def __init__(self) -> None:
        self.calls = 0

    def current_orientation(self) -> str:
        return "horizontal"

    def display(self, request) -> DisplayResult:
        self.calls += 1
        return DisplayResult(True, f"displayed {request.image_id}")


class _FakeBot:
    def __init__(self, *, send_side_effect: Exception | None = None) -> None:
        self.send_side_effect = send_side_effect
        self.messages: list[tuple[int, str]] = []

    async def send_message(self, *, chat_id: int, text: str, write_timeout: int = 60) -> None:
        if self.send_side_effect is not None:
            raise self.send_side_effect
        self.messages.append((chat_id, text))


class _FakeTelegramFile:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    async def download_to_drive(self, *, custom_path: str) -> None:
        path = Path(custom_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.payload)


class _FakePhoto:
    def __init__(self, token: str) -> None:
        self.file_id = f"file-{token}"
        self._file = _FakeTelegramFile(token.encode("utf-8"))

    async def get_file(self) -> _FakeTelegramFile:
        return self._file


class _FakeMessage:
    def __init__(self, *, photo_token: str, caption: str = "") -> None:
        self.photo = [_FakePhoto(photo_token)]
        self.caption = caption
        self.replies: list[str] = []

    async def reply_text(self, text: str, reply_markup=None, write_timeout: int = 60) -> None:
        self.replies.append(text)


class _FakeUser:
    def __init__(self, user_id: int) -> None:
        self.id = user_id


class _FakeChat:
    def __init__(self, chat_id: int) -> None:
        self.id = chat_id


class _FakeUpdate:
    def __init__(self, *, user_id: int, chat_id: int, photo_token: str, caption: str = "") -> None:
        self.effective_user = _FakeUser(user_id)
        self.effective_chat = _FakeChat(chat_id)
        self.effective_message = _FakeMessage(photo_token=photo_token, caption=caption)
        self.callback_query = None


class _FakeContext:
    def __init__(self, application: "_FakeApplication") -> None:
        self.application = application
        self.user_data: dict[str, object] = {}


class _FakeApplication:
    def __init__(self, services, *, bot: _FakeBot | None = None) -> None:
        self.bot_data = {
            "services": services,
            "display_lock": asyncio.Lock(),
            "upload_queue": asyncio.Queue(),
        }
        self.bot = bot or services.bot
        self.job_queue = None


def _build_services(
    base_dir: Path,
    *,
    bot: _FakeBot | None = None,
    display: _FakeDisplay | None = None,
):
    database = Database(base_dir / "photo_frame.db")
    database.initialize()
    storage = _FakeStorage(base_dir)
    fake_bot = bot or _FakeBot()
    return SimpleNamespace(
        auth=_FakeAuth(),
        database=database,
        storage=storage,
        renderer=_FakeRenderer(),
        display=display or _FakeDisplay(),
        config=SimpleNamespace(storage=SimpleNamespace(current_payload_path=base_dir / "inkypi" / "current.json")),
        bot=fake_bot,
    )


if __name__ == "__main__":
    unittest.main()
