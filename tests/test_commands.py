from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from app.commands import delete_cancel_callback, delete_confirm_callback
from app.database import Database
from app.models import DisplayResult, ImageRecord


class DeleteCommandTests(unittest.IsolatedAsyncioTestCase):
    async def test_delete_confirm_callback_handles_text_only_confirmation_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-1"}),
                encoding="utf-8",
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-1",
                    telegram_file_id="file-1",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-1.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="",
                    uploaded_by=1,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="displayed",
                    last_error=None,
                )
            )

            update = _FakeUpdate(data="delete_confirm:img-1", has_media=False)
            context = _FakeContext(services)

            await delete_confirm_callback(update, context)

            self.assertEqual(update.callback_query.text_edits, [
                "Das letzte Bild kann nicht gelöscht werden. Lade zuerst ein neues Bild hoch."
            ])
            self.assertEqual(update.callback_query.caption_edits, [])

    async def test_delete_cancel_callback_uses_caption_edit_for_media_messages(self) -> None:
        services = _build_services(Path(tempfile.mkdtemp()))
        update = _FakeUpdate(data="delete_cancel", has_media=True)
        context = _FakeContext(services)

        await delete_cancel_callback(update, context)

        self.assertEqual(update.callback_query.caption_edits, ["Löschen abgebrochen."])
        self.assertEqual(update.callback_query.text_edits, [])


class _FakeQueryMessage:
    def __init__(self, *, has_media: bool) -> None:
        self.photo = [object()] if has_media else []
        self.document = None
        self.animation = None
        self.video = None


class _FakeCallbackQuery:
    def __init__(self, *, data: str, has_media: bool) -> None:
        self.data = data
        self.message = _FakeQueryMessage(has_media=has_media)
        self.text_edits: list[str] = []
        self.caption_edits: list[str] = []

    async def answer(self) -> None:
        return None

    async def edit_message_text(self, text: str) -> None:
        self.text_edits.append(text)

    async def edit_message_caption(self, *, caption: str) -> None:
        self.caption_edits.append(caption)


class _FakeUpdate:
    def __init__(self, *, data: str, has_media: bool) -> None:
        self.callback_query = _FakeCallbackQuery(data=data, has_media=has_media)


class _FakeContext:
    def __init__(self, services) -> None:
        self.application = SimpleNamespace(bot_data={"services": services, "display_lock": asyncio.Lock()})


class _FakeDisplay:
    def display(self, request) -> DisplayResult:
        return DisplayResult(True, f"displayed {request.image_id}")


def _build_services(base_dir: Path):
    database = Database(base_dir / "photo_frame.db")
    database.initialize()
    return SimpleNamespace(
        database=database,
        display=_FakeDisplay(),
        storage=SimpleNamespace(rendered_path=lambda image_id: base_dir / "rendered" / f"{image_id}.png"),
        renderer=SimpleNamespace(render=lambda *args, **kwargs: None),
        config=SimpleNamespace(
            storage=SimpleNamespace(
                current_payload_path=base_dir / "inkypi" / "current.json",
                current_image_path=base_dir / "inkypi" / "current.png",
            )
        ),
    )


if __name__ == "__main__":
    unittest.main()
