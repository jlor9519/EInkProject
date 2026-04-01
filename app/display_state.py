from __future__ import annotations

import json
import logging
from pathlib import Path

from app.database import utcnow_iso
from app.models import ImageRecord

logger = logging.getLogger(__name__)

DISPLAY_TRANSITION_IMAGE_ID_KEY = "display_transition_image_id"
DISPLAY_TRANSITION_STARTED_AT_KEY = "display_transition_started_at"
DISPLAY_TRANSITION_KIND_KEY = "display_transition_kind"
DISPLAY_TRANSITION_KEYS = (
    DISPLAY_TRANSITION_IMAGE_ID_KEY,
    DISPLAY_TRANSITION_STARTED_AT_KEY,
    DISPLAY_TRANSITION_KIND_KEY,
)


def read_current_payload_image_id(payload_path: Path) -> str | None:
    if not payload_path.exists():
        return None
    try:
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logger.warning("Could not read current payload image id from %s", payload_path, exc_info=True)
        return None
    image_id = payload.get("image_id")
    return str(image_id) if image_id else None


def begin_display_transition(database, image_id: str, kind: str) -> None:
    database.set_settings(
        {
            DISPLAY_TRANSITION_IMAGE_ID_KEY: image_id,
            DISPLAY_TRANSITION_STARTED_AT_KEY: utcnow_iso(),
            DISPLAY_TRANSITION_KIND_KEY: kind,
        }
    )


def clear_display_transition(database) -> None:
    database.set_settings({key: None for key in DISPLAY_TRANSITION_KEYS})


def commit_display_success(
    database,
    record: ImageRecord,
    *,
    mark_new_image: bool,
    displayed_at: str | None = None,
) -> str:
    timestamp = displayed_at or utcnow_iso()
    settings: dict[str, str | None] = {
        "current_image_displayed_at": timestamp,
    }
    if mark_new_image:
        settings["last_new_image_displayed_at"] = timestamp
    database.apply_image_and_settings(
        record,
        settings=settings,
        clear_keys=DISPLAY_TRANSITION_KEYS,
    )
    return timestamp
