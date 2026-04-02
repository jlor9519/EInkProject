from __future__ import annotations

import re
from io import BytesIO
from pathlib import Path

from PIL import Image, ImageColor, ImageDraw, ImageFilter, ImageFont, ImageOps

from app.models import DisplayConfig

# Characters DejaVuSans cannot render: emoji, pictographs, variation selectors, ZWJ
_UNSUPPORTED_RE = re.compile(
    "["
    "\U0001F100-\U0001FFFF"
    "\U00002600-\U000027BF"
    "\U0000FE00-\U0000FE0F"
    "\U0000200D"
    "\U000020E3"
    "]+",
    flags=re.UNICODE,
)

_CAPTION_BG = "#FFFFFF"
_ICON_TEXT_GAP = 6
_METADATA_LINE_GAP = 2
_BLUR_RADIUS = 18


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
        with Image.open(original_path) as original:
            prepared = ImageOps.exif_transpose(original).convert("RGB")
            prepared.save(output_path, format="PNG")
        return output_path

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
        width = self.config.width
        height = self.config.height
        if orientation == "vertical":
            width, height = height, width
        caption_bar_height = max(0, min(self.config.caption_height, height - 1))
        photo_height = max(1, height - caption_bar_height)
        margin = self.config.margin

        with Image.open(original_path) as original:
            prepared = ImageOps.exif_transpose(original).convert("RGB")

        bg_color = ImageColor.getrgb(_CAPTION_BG)
        final = Image.new("RGB", (width, height), bg_color)
        photo_area = (width, photo_height)

        if fit_mode == "fill":
            filled = ImageOps.fit(
                prepared, photo_area, method=Image.Resampling.LANCZOS, centering=(0.5, 0.5),
            )
            final.paste(filled, (0, 0))
        else:
            blurred = ImageOps.fit(
                prepared, photo_area, method=Image.Resampling.LANCZOS, centering=(0.5, 0.5),
            ).filter(ImageFilter.GaussianBlur(radius=_BLUR_RADIUS))
            final.paste(blurred, (0, 0))
            contained = ImageOps.contain(prepared, photo_area, method=Image.Resampling.LANCZOS)
            final.paste(contained, ((width - contained.width) // 2, (photo_height - contained.height) // 2))

        draw = ImageDraw.Draw(final)
        bar_top = photo_height
        draw.rectangle([(0, bar_top), (width, height)], fill=bg_color)

        caption_font = self._load_font(self.config.caption_font_size)
        metadata_font = self._load_font(self.config.metadata_font_size)
        text_color = ImageColor.getrgb(self.config.text_color)

        metadata_lines = self._prepare_metadata_lines(
            draw, metadata_font,
            taken_at=self._normalize_text(taken_at),
            location=self._normalize_text(location),
            max_block_width=self._max_metadata_block_width(width, margin),
        )
        metadata_block_width = max((line["width"] for line in metadata_lines), default=0)
        caption_available = max(
            1,
            width - (margin * 2) - (metadata_block_width + margin if metadata_block_width else 0),
        )
        text = self._truncate_line(
            draw,
            self._truncate_characters(self._normalize_text(caption), self.config.caption_character_limit),
            caption_font,
            caption_available,
        )

        if text:
            bbox = draw.textbbox((0, 0), text, font=caption_font)
            th = bbox[3] - bbox[1]
            ty = bar_top + max(0, (caption_bar_height - th) // 2) - bbox[1]
            draw.text((margin, ty), text, font=caption_font, fill=text_color)

        if metadata_lines:
            self._draw_metadata_block(
                draw, metadata_lines,
                width=width, bar_top=bar_top,
                caption_bar_height=caption_bar_height, caption_margin=margin,
                text_color=text_color, background_color=bg_color,
            )

        buf = BytesIO()
        final.save(buf, format="JPEG", quality=85)
        buf.seek(0)
        return buf

    # ------------------------------------------------------------------
    # Helpers (adapted from TelegramFrame plugin for preview parity)
    # ------------------------------------------------------------------

    def _load_font(self, size: int) -> ImageFont.ImageFont:
        if self.config.font_path:
            try:
                return ImageFont.truetype(self.config.font_path, size=size)
            except OSError:
                pass
        return ImageFont.load_default()

    @staticmethod
    def _normalize_text(text: str) -> str:
        stripped = _UNSUPPORTED_RE.sub("", str(text or ""))
        return " ".join(stripped.split())

    @staticmethod
    def _truncate_characters(text: str, limit: int) -> str:
        if not text or limit <= 0 or len(text) <= limit:
            return text
        if limit <= 3:
            return "." * limit
        return text[: limit - 3].rstrip() + "..."

    @staticmethod
    def _truncate_line(
        draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int,
    ) -> str:
        if not text:
            return ""
        candidate = " ".join(text.split())
        ellipsis = "..."
        while candidate and draw.textlength(candidate, font=font) > max_width:
            if draw.textlength(candidate + ellipsis, font=font) <= max_width:
                return candidate + ellipsis
            candidate = candidate[:-1].rstrip()
        return candidate or ellipsis

    @staticmethod
    def _icon_size(font: ImageFont.ImageFont) -> int:
        try:
            return max(10, int(getattr(font, "size", 14)) - 2)
        except (TypeError, ValueError):
            return 12

    @staticmethod
    def _max_metadata_block_width(width: int, margin: int) -> int:
        min_caption_width = max(120, width // 3)
        available = width - (margin * 3) - min_caption_width
        return max(120, min(int(width * 0.45), available))

    def _prepare_metadata_lines(
        self,
        draw: ImageDraw.ImageDraw,
        font: ImageFont.ImageFont,
        *,
        taken_at: str,
        location: str,
        max_block_width: int,
    ) -> list[dict[str, object]]:
        lines: list[dict[str, object]] = []
        for kind, text in (("date", taken_at), ("location", location)):
            if not text:
                continue
            text = self._normalize_text(text)
            if not text:
                continue
            icon_sz = self._icon_size(font)
            max_tw = max(1, max_block_width - icon_sz - _ICON_TEXT_GAP)
            truncated = self._truncate_line(draw, text, font, max_tw)
            if not truncated:
                continue
            bbox = draw.textbbox((0, 0), truncated, font=font)
            tw = max(0, bbox[2] - bbox[0])
            th = max(0, bbox[3] - bbox[1])
            lh = max(icon_sz, th)
            lines.append({
                "kind": kind, "text": truncated, "font": font,
                "icon_size": icon_sz,
                "width": icon_sz + _ICON_TEXT_GAP + tw,
                "height": lh, "text_bbox": bbox,
            })
        return lines

    @staticmethod
    def _draw_metadata_block(
        draw: ImageDraw.ImageDraw,
        metadata_lines: list[dict[str, object]],
        *,
        width: int,
        bar_top: int,
        caption_bar_height: int,
        caption_margin: int,
        text_color: tuple[int, int, int],
        background_color: tuple[int, int, int],
    ) -> None:
        total_h = sum(int(l["height"]) for l in metadata_lines)
        total_h += _METADATA_LINE_GAP * max(0, len(metadata_lines) - 1)
        cur_y = bar_top + max(0, (caption_bar_height - total_h) // 2)

        for line in metadata_lines:
            lw = int(line["width"])
            lh = int(line["height"])
            icon_sz = int(line["icon_size"])
            font = line["font"]
            bbox = line["text_bbox"]
            text = str(line["text"])

            lx = width - caption_margin - lw
            iy = cur_y + max(0, (lh - icon_sz) // 2)
            if line["kind"] == "date":
                _draw_calendar_icon(draw, lx, iy, icon_sz, text_color, background_color)
            else:
                _draw_location_icon(draw, lx, iy, icon_sz, text_color, background_color)

            tx = lx + icon_sz + _ICON_TEXT_GAP
            th = bbox[3] - bbox[1]
            ty = cur_y + max(0, (lh - th) // 2) - bbox[1]
            draw.text((tx, ty), text, font=font, fill=text_color)
            cur_y += lh + _METADATA_LINE_GAP


# ------------------------------------------------------------------
# Icon drawing (module-level, shared with _draw_metadata_block)
# ------------------------------------------------------------------

def _draw_calendar_icon(
    draw: ImageDraw.ImageDraw, x: int, y: int, size: int,
    color: tuple[int, int, int], bg: tuple[int, int, int],
) -> None:
    right, bottom = x + size, y + size
    draw.rounded_rectangle((x, y, right, bottom), radius=2, outline=color, width=1, fill=bg)
    rw = max(1, size // 7)
    top_band = y + max(2, size // 4)
    draw.rectangle((x, y, right, top_band), fill=color)
    draw.rectangle((x + rw, y - 1, x + rw * 2, y + rw + 1), fill=color)
    draw.rectangle((right - rw * 2, y - 1, right - rw, y + rw + 1), fill=color)
    gy = top_band + max(2, size // 6)
    if gy < bottom - 2:
        draw.line((x + 2, gy, right - 2, gy), fill=color, width=1)


def _draw_location_icon(
    draw: ImageDraw.ImageDraw, x: int, y: int, size: int,
    color: tuple[int, int, int], bg: tuple[int, int, int],
) -> None:
    right, bottom = x + size, y + size
    cb = bottom - max(3, size // 4)
    draw.ellipse((x + 1, y, right - 1, cb), outline=color, width=1, fill=bg)
    cx = (x + right) // 2
    draw.polygon(((x + 2, cb - 1), (right - 2, cb - 1), (cx, bottom)), outline=color, fill=bg)
    im = max(3, size // 4)
    draw.ellipse((x + im, y + im - 1, right - im, cb - im), outline=color, width=1)
