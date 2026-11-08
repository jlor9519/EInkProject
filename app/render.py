from __future__ import annotations

import math
from collections.abc import Callable
from io import BytesIO
from pathlib import Path

from PIL import Image, ImageOps

from app.compose import compose_frame, frame_size
from app.models import DisplayConfig

# Upper bound for stored prepared images (~18 MB as RGB), relevant for very long panoramas.
_MAX_PREPARED_PIXELS = 6_000_000


def _open_prepared(path: Path, max_side: int | Callable[[tuple[int, int]], int]) -> Image.Image:
    """Open an image upright, in RGB, and no larger than max_side on its longest edge.

    max_side may be a function of the original (width, height). The draft call lets JPEGs decode
    at a reduced scale, so a large photo never has to be held in memory at full resolution (the
    Pi Zero only has 512 MB).
    """
    with Image.open(path) as image:
        if callable(max_side):
            max_side = max_side(image.size)
        longest = max(image.size)
        if longest > max_side:
            scale = max_side / longest
            image.draft("RGB", (max(1, round(image.width * scale)), max(1, round(image.height * scale))))
        prepared = ImageOps.exif_transpose(image).convert("RGB")
    prepared.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)
    return prepared


class RenderService:
    def __init__(self, config: DisplayConfig):
        self.config = config

    def render(
        self,
        original_path: Path,
        output_path: Path,
        *,
        location: str,
        taken_at: str,
        caption: str,
    ) -> Path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        prepared = _open_prepared(original_path, self._max_prepared_side)
        prepared.save(output_path, format="PNG")
        return output_path

    def _max_prepared_side(self, size: tuple[int, int]) -> int:
        """Longest edge to keep for an image of the given size.

        Twice the panel size is plenty for normal photos and keeps the PNG a few MB. The shorter
        edge is kept at least as long as the panel's longer edge, so "fill" never has to enlarge
        a panorama, in either orientation. A pixel cap bounds memory for extreme panoramas.
        """
        width, height = max(1, size[0]), max(1, size[1])
        panel_long = max(self.config.width, self.config.height)
        longest, shortest = max(width, height), min(width, height)
        limit = max(2 * panel_long, math.ceil(longest * panel_long / shortest))
        pixel_cap = math.floor(longest * math.sqrt(_MAX_PREPARED_PIXELS / (width * height)))
        return max(1, min(limit, pixel_cap))

    @staticmethod
    def thumbnail_jpeg(image_path: Path, max_side: int = 1280) -> BytesIO:
        """Small JPEG copy of an image, well below Telegram's 10 MB photo limit."""
        prepared = _open_prepared(image_path, max_side)
        buf = BytesIO()
        prepared.save(buf, format="JPEG", quality=85)
        buf.seek(0)
        buf.name = "preview.jpg"
        return buf

    def compose_preview(
        self,
        original_path: Path,
        *,
        location: str,
        taken_at: str,
        caption: str,
        orientation: str = "horizontal",
        fit_mode: str = "fill",
    ) -> BytesIO:
        width, height = frame_size(self.config.width, self.config.height, orientation)
        prepared = _open_prepared(original_path, self._max_prepared_side)
        final = compose_frame(
            prepared,
            width=width,
            height=height,
            caption_bar_height=self.config.caption_height,
            caption=caption,
            taken_at=taken_at,
            location=location,
            caption_font_size=self.config.caption_font_size,
            metadata_font_size=self.config.metadata_font_size,
            caption_character_limit=self.config.caption_character_limit,
            caption_margin=self.config.margin,
            font_path=self.config.font_path,
            text_color=self.config.text_color,
            fit_mode=fit_mode,
        )
        buf = BytesIO()
        final.save(buf, format="JPEG", quality=85)
        buf.seek(0)
        return buf
