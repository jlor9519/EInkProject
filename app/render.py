from __future__ import annotations

import re
from io import BytesIO
from pathlib import Path

from PIL import Image, ImageColor, ImageDraw, ImageFilter, ImageFont, ImageOps

from app.models import DisplayConfig

_EMOJI_RUN_RE = re.compile(
    "(?:"
    "[0-9#*]\uFE0F?\u20E3"
    "|"
    "["
    "\U0001F100-\U0001FFFF"
    "\U00002600-\U000027BF"
    "\U0000FE00-\U0000FE0F"
    "\U0000200D"
    "\U000020E3"
    "]+"
    ")",
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

        caption_font = self._load_font(self.config.font_path, self.config.caption_font_size)
        metadata_font = self._load_font(self.config.font_path, self.config.metadata_font_size)
        emoji_caption_font = self._load_emoji_font(self.config.caption_font_size, caption_font)
        emoji_metadata_font = self._load_emoji_font(self.config.metadata_font_size, metadata_font)
        text_color = ImageColor.getrgb(self.config.text_color)

        metadata_lines = self._prepare_metadata_lines(
            draw, metadata_font, emoji_metadata_font,
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
            emoji_caption_font,
            caption_available,
        )

        if text:
            runs = self._split_text_runs(text, caption_font, emoji_caption_font)
            _, text_top, text_bottom = self._measure_runs(draw, runs)
            th = text_bottom - text_top
            ty = bar_top + max(0, (caption_bar_height - th) // 2) - text_top
            self._draw_text_runs(draw, margin, ty, runs, fill=text_color)

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

    def _load_font(self, font_path: str, size: int) -> ImageFont.ImageFont:
        if font_path:
            try:
                return ImageFont.truetype(font_path, size=size)
            except OSError:
                pass
        return ImageFont.load_default()

    def _load_emoji_font(
        self,
        size: int,
        fallback_font: ImageFont.ImageFont,
    ) -> ImageFont.ImageFont:
        emoji_font_path = (self.config.emoji_font_path or "").strip()
        if emoji_font_path:
            try:
                return ImageFont.truetype(emoji_font_path, size=size)
            except OSError:
                pass
        return fallback_font

    @staticmethod
    def _normalize_text(text: str) -> str:
        return " ".join(str(text or "").split())

    @staticmethod
    def _truncate_characters(text: str, limit: int) -> str:
        if not text or limit <= 0 or len(text) <= limit:
            return text
        if limit <= 3:
            return "." * limit
        return text[: limit - 3].rstrip() + "..."

    def _truncate_line(
        self,
        draw: ImageDraw.ImageDraw,
        text: str,
        font: ImageFont.ImageFont,
        emoji_font: ImageFont.ImageFont,
        max_width: int,
    ) -> str:
        if not text:
            return ""
        candidate = " ".join(text.split())
        ellipsis = "..."
        while candidate and self._measure_text(draw, candidate, font, emoji_font) > max_width:
            if self._measure_text(draw, candidate + ellipsis, font, emoji_font) <= max_width:
                return candidate + ellipsis
            candidate = candidate[:-1].rstrip()
        return candidate or ellipsis

    def _split_text_runs(
        self,
        text: str,
        font: ImageFont.ImageFont,
        emoji_font: ImageFont.ImageFont,
    ) -> list[dict[str, object]]:
        if not text:
            return []

        runs: list[dict[str, object]] = []
        cursor = 0
        use_color_emoji = emoji_font is not font
        for match in _EMOJI_RUN_RE.finditer(text):
            if match.start() > cursor:
                runs.append({"text": text[cursor:match.start()], "font": font, "embedded_color": False})
            runs.append(
                {
                    "text": match.group(0),
                    "font": emoji_font if use_color_emoji else font,
                    "embedded_color": use_color_emoji,
                }
            )
            cursor = match.end()
        if cursor < len(text):
            runs.append({"text": text[cursor:], "font": font, "embedded_color": False})
        return [run for run in runs if run["text"]]

    def _measure_text(
        self,
        draw: ImageDraw.ImageDraw,
        text: str,
        font: ImageFont.ImageFont,
        emoji_font: ImageFont.ImageFont,
    ) -> float:
        runs = self._split_text_runs(text, font, emoji_font)
        width, _, _ = self._measure_runs(draw, runs)
        return width

    @staticmethod
    def _measure_runs(
        draw: ImageDraw.ImageDraw,
        runs: list[dict[str, object]],
    ) -> tuple[float, int, int]:
        total_width = 0.0
        top = 0
        bottom = 0
        seen = False
        for run in runs:
            bbox = draw.textbbox(
                (0, 0),
                str(run["text"]),
                font=run["font"],
                embedded_color=bool(run["embedded_color"]),
            )
            total_width += float(
                draw.textlength(
                    str(run["text"]),
                    font=run["font"],
                    embedded_color=bool(run["embedded_color"]),
                )
            )
            if not seen:
                top = bbox[1]
                bottom = bbox[3]
                seen = True
            else:
                top = min(top, bbox[1])
                bottom = max(bottom, bbox[3])
        return total_width, top, bottom

    @staticmethod
    def _draw_text_runs(
        draw: ImageDraw.ImageDraw,
        x: int,
        y: int,
        runs: list[dict[str, object]],
        *,
        fill: tuple[int, int, int],
    ) -> None:
        cur_x = float(x)
        for run in runs:
            draw.text(
                (cur_x, y),
                str(run["text"]),
                font=run["font"],
                fill=fill,
                embedded_color=bool(run["embedded_color"]),
            )
            cur_x += float(
                draw.textlength(
                    str(run["text"]),
                    font=run["font"],
                    embedded_color=bool(run["embedded_color"]),
                )
            )

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
        emoji_font: ImageFont.ImageFont,
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
            truncated = self._truncate_line(draw, text, font, emoji_font, max_tw)
            if not truncated:
                continue
            runs = self._split_text_runs(truncated, font, emoji_font)
            tw, text_top, text_bottom = self._measure_runs(draw, runs)
            th = max(0, text_bottom - text_top)
            lh = max(icon_sz, th)
            lines.append({
                "kind": kind,
                "runs": runs,
                "icon_size": icon_sz,
                "width": icon_sz + _ICON_TEXT_GAP + int(round(tw)),
                "height": lh,
                "text_top": text_top,
                "text_bottom": text_bottom,
            })
        return lines

    def _draw_metadata_block(
        self,
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
            runs = list(line["runs"])

            lx = width - caption_margin - lw
            iy = cur_y + max(0, (lh - icon_sz) // 2)
            if line["kind"] == "date":
                _draw_calendar_icon(draw, lx, iy, icon_sz, text_color, background_color)
            else:
                _draw_location_icon(draw, lx, iy, icon_sz, text_color, background_color)

            tx = lx + icon_sz + _ICON_TEXT_GAP
            th = int(line["text_bottom"]) - int(line["text_top"])
            ty = cur_y + max(0, (lh - th) // 2) - int(line["text_top"])
            self._draw_text_runs(draw, tx, ty, runs, fill=text_color)
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
