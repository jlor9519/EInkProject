from __future__ import annotations

import json
import re
from pathlib import Path

from PIL import Image, ImageColor, ImageDraw, ImageFilter, ImageFont, ImageOps
from plugins.base_plugin.base_plugin import BasePlugin

# Characters DejaVuSans cannot render: emoji, pictographs, variation selectors, ZWJ
_UNSUPPORTED_RE = re.compile(
    "["
    "\U0001F100-\U0001FFFF"  # All SMP emoji / symbols (emoticons, transport, nature, …)
    "\U00002600-\U000027BF"  # BMP misc symbols & dingbats
    "\U0000FE00-\U0000FE0F"  # Variation selectors (modify preceding emoji)
    "\U0000200D"             # Zero-width joiner (emoji sequence connector)
    "\U000020E3"             # Combining enclosing keycap
    "]+",
    flags=re.UNICODE,
)

DEFAULT_CAPTION_BAR_HEIGHT = 44
DEFAULT_CAPTION_FONT_SIZE = 20
DEFAULT_METADATA_FONT_SIZE = 14
DEFAULT_CAPTION_CHARACTER_LIMIT = 72
DEFAULT_CAPTION_MARGIN = 12
DEFAULT_CAPTION_TEXT_COLOR = "#111111"
DEFAULT_CAPTION_BACKGROUND_COLOR = "#FFFFFF"
DEFAULT_ICON_TEXT_GAP = 6
DEFAULT_METADATA_LINE_GAP = 2


class TelegramFrame(BasePlugin):
    def generate_image(self, settings, device_config):  # noqa: D401 - InkyPi API method
        payload_path = settings.get("payload_path")
        if not payload_path:
            raise RuntimeError("Missing payload_path in plugin settings.")

        payload_file = Path(payload_path)
        if not payload_file.exists():
            raise RuntimeError(f"Payload file not found: {payload_file}")

        try:
            payload = json.loads(payload_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Payload file is not valid JSON: {exc}") from exc

        image_path = Path(
            payload.get("prepared_image_path")
            or payload.get("bridge_image_path")
            or payload.get("composed_path", "")
        )
        if not image_path.exists():
            raise RuntimeError(f"Prepared image not found: {image_path}")

        orientation = str(device_config.get_config("orientation") or "horizontal")
        width, height = self._resolve_dimensions(device_config, orientation)
        caption_bar_height = self._safe_int(payload.get("caption_bar_height"), DEFAULT_CAPTION_BAR_HEIGHT)
        caption_bar_height = max(0, min(caption_bar_height, height - 1))
        photo_height = max(1, height - caption_bar_height)

        with Image.open(image_path) as prepared_image:
            prepared_rgb = prepared_image.convert("RGB")
            final_image = self._compose_final_image(
                prepared_rgb,
                width=width,
                height=height,
                photo_height=photo_height,
                caption=payload.get("caption", ""),
                taken_at=payload.get("taken_at", ""),
                location=payload.get("location", ""),
                caption_bar_height=caption_bar_height,
                caption_font_size=self._safe_int(payload.get("caption_font_size"), DEFAULT_CAPTION_FONT_SIZE),
                metadata_font_size=self._safe_int(payload.get("metadata_font_size"), DEFAULT_METADATA_FONT_SIZE),
                caption_character_limit=self._safe_int(
                    payload.get("caption_character_limit"),
                    DEFAULT_CAPTION_CHARACTER_LIMIT,
                ),
                caption_margin=self._safe_int(payload.get("caption_margin"), DEFAULT_CAPTION_MARGIN),
                font_path=str(payload.get("font_path") or ""),
                caption_text_color=str(payload.get("caption_text_color") or DEFAULT_CAPTION_TEXT_COLOR),
                caption_background_color=str(payload.get("caption_background_color") or DEFAULT_CAPTION_BACKGROUND_COLOR),
                fit_mode=str(payload.get("image_fit_mode") or "fill"),
            )

        return final_image

    def _compose_final_image(
        self,
        prepared_image: Image.Image,
        *,
        width: int,
        height: int,
        photo_height: int,
        caption: str,
        taken_at: str,
        location: str,
        caption_bar_height: int,
        caption_font_size: int,
        metadata_font_size: int,
        caption_character_limit: int,
        caption_margin: int,
        font_path: str,
        caption_text_color: str,
        caption_background_color: str,
        fit_mode: str = "fill",
    ) -> Image.Image:
        final_image = Image.new("RGB", (width, height), ImageColor.getrgb(caption_background_color))
        photo_area_size = (width, photo_height)

        if fit_mode == "fill":
            filled = ImageOps.fit(
                prepared_image,
                photo_area_size,
                method=Image.Resampling.LANCZOS,
                centering=(0.5, 0.5),
            )
            final_image.paste(filled, (0, 0))
        else:
            blurred_background = ImageOps.fit(
                prepared_image,
                photo_area_size,
                method=Image.Resampling.LANCZOS,
                centering=(0.5, 0.5),
            ).filter(ImageFilter.GaussianBlur(radius=18))
            final_image.paste(blurred_background, (0, 0))

            contained = ImageOps.contain(
                prepared_image,
                photo_area_size,
                method=Image.Resampling.LANCZOS,
            )
            paste_x = (width - contained.width) // 2
            paste_y = (photo_height - contained.height) // 2
            final_image.paste(contained, (paste_x, paste_y))

        draw = ImageDraw.Draw(final_image)
        bar_top = photo_height
        draw.rectangle(
            [(0, bar_top), (width, height)],
            fill=ImageColor.getrgb(caption_background_color),
        )

        caption_font = self._load_font(font_path, caption_font_size)
        metadata_font = self._load_font(font_path, metadata_font_size)
        text_color = ImageColor.getrgb(caption_text_color)
        metadata_lines = self._prepare_metadata_lines(
            draw,
            metadata_font,
            taken_at=self._normalize_text(taken_at),
            location=self._normalize_text(location),
            max_block_width=self._max_metadata_block_width(width, caption_margin),
        )
        metadata_block_width = max((line["width"] for line in metadata_lines), default=0)
        caption_available_width = max(
            1,
            width - (caption_margin * 2) - (metadata_block_width + caption_margin if metadata_block_width else 0),
        )
        text = self._truncate_line(
            draw,
            self._truncate_characters(self._normalize_text(caption), caption_character_limit),
            caption_font,
            caption_available_width,
        )

        if text:
            text_bbox = draw.textbbox((0, 0), text, font=caption_font)
            text_height = text_bbox[3] - text_bbox[1]
            text_y = bar_top + max(0, (caption_bar_height - text_height) // 2) - text_bbox[1]
            draw.text(
                (caption_margin, text_y),
                text,
                font=caption_font,
                fill=text_color,
            )

        if metadata_lines:
            self._draw_metadata_block(
                draw,
                metadata_lines,
                width=width,
                bar_top=bar_top,
                caption_bar_height=caption_bar_height,
                caption_margin=caption_margin,
                text_color=text_color,
                background_color=ImageColor.getrgb(caption_background_color),
            )

        return final_image

    def _resolve_dimensions(self, device_config, orientation: str) -> tuple[int, int]:
        width, height = device_config.get_resolution()
        if orientation == "vertical":
            return height, width
        return width, height

    def _load_font(self, font_path: str, size: int) -> ImageFont.ImageFont:
        if font_path:
            try:
                return ImageFont.truetype(font_path, size=size)
            except OSError:
                pass
        return ImageFont.load_default()

    def _truncate_line(
        self,
        draw: ImageDraw.ImageDraw,
        text: str,
        font: ImageFont.ImageFont,
        max_width: int,
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

    def _normalize_text(self, text: str) -> str:
        stripped = _UNSUPPORTED_RE.sub("", str(text or ""))
        return " ".join(stripped.split())

    def _truncate_characters(self, text: str, limit: int) -> str:
        if not text or limit <= 0 or len(text) <= limit:
            return text
        if limit <= 3:
            return "." * limit
        return text[: limit - 3].rstrip() + "..."

    def _max_metadata_block_width(self, width: int, caption_margin: int) -> int:
        min_caption_width = max(120, width // 3)
        available = width - (caption_margin * 3) - min_caption_width
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
            icon_size = self._icon_size(font)
            max_text_width = max(1, max_block_width - icon_size - DEFAULT_ICON_TEXT_GAP)
            truncated_text = self._truncate_line(draw, text, font, max_text_width)
            if not truncated_text:
                continue
            text_bbox = draw.textbbox((0, 0), truncated_text, font=font)
            text_width = max(0, text_bbox[2] - text_bbox[0])
            text_height = max(0, text_bbox[3] - text_bbox[1])
            line_height = max(icon_size, text_height)
            lines.append(
                {
                    "kind": kind,
                    "text": truncated_text,
                    "font": font,
                    "icon_size": icon_size,
                    "width": icon_size + DEFAULT_ICON_TEXT_GAP + text_width,
                    "height": line_height,
                    "text_bbox": text_bbox,
                }
            )
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
        total_height = sum(int(line["height"]) for line in metadata_lines)
        total_height += DEFAULT_METADATA_LINE_GAP * max(0, len(metadata_lines) - 1)
        current_y = bar_top + max(0, (caption_bar_height - total_height) // 2)

        for line in metadata_lines:
            line_width = int(line["width"])
            line_height = int(line["height"])
            icon_size = int(line["icon_size"])
            font = line["font"]
            text_bbox = line["text_bbox"]
            text = str(line["text"])

            line_x = width - caption_margin - line_width
            icon_y = current_y + max(0, (line_height - icon_size) // 2)
            if line["kind"] == "date":
                self._draw_calendar_icon(draw, line_x, icon_y, icon_size, text_color, background_color)
            else:
                self._draw_location_icon(draw, line_x, icon_y, icon_size, text_color, background_color)

            text_x = line_x + icon_size + DEFAULT_ICON_TEXT_GAP
            text_height = text_bbox[3] - text_bbox[1]
            text_y = current_y + max(0, (line_height - text_height) // 2) - text_bbox[1]
            draw.text((text_x, text_y), text, font=font, fill=text_color)
            current_y += line_height + DEFAULT_METADATA_LINE_GAP

    def _draw_calendar_icon(
        self,
        draw: ImageDraw.ImageDraw,
        x: int,
        y: int,
        size: int,
        color: tuple[int, int, int],
        background_color: tuple[int, int, int],
    ) -> None:
        right = x + size
        bottom = y + size
        draw.rounded_rectangle((x, y, right, bottom), radius=2, outline=color, width=1, fill=background_color)
        ring_width = max(1, size // 7)
        top_band = y + max(2, size // 4)
        draw.rectangle((x, y, right, top_band), fill=color)
        draw.rectangle((x + ring_width, y - 1, x + (ring_width * 2), y + ring_width + 1), fill=color)
        draw.rectangle((right - (ring_width * 2), y - 1, right - ring_width, y + ring_width + 1), fill=color)
        grid_y = top_band + max(2, size // 6)
        if grid_y < bottom - 2:
            draw.line((x + 2, grid_y, right - 2, grid_y), fill=color, width=1)

    def _draw_location_icon(
        self,
        draw: ImageDraw.ImageDraw,
        x: int,
        y: int,
        size: int,
        color: tuple[int, int, int],
        background_color: tuple[int, int, int],
    ) -> None:
        right = x + size
        bottom = y + size
        circle_bottom = bottom - max(3, size // 4)
        draw.ellipse((x + 1, y, right - 1, circle_bottom), outline=color, width=1, fill=background_color)
        center_x = (x + right) // 2
        draw.polygon(
            ((x + 2, circle_bottom - 1), (right - 2, circle_bottom - 1), (center_x, bottom)),
            outline=color,
            fill=background_color,
        )
        inner_margin = max(3, size // 4)
        draw.ellipse(
            (x + inner_margin, y + inner_margin - 1, right - inner_margin, circle_bottom - inner_margin),
            outline=color,
            width=1,
        )

    def _icon_size(self, font: ImageFont.ImageFont) -> int:
        try:
            return max(10, int(getattr(font, "size", DEFAULT_METADATA_FONT_SIZE)) - 2)
        except (TypeError, ValueError):
            return max(10, DEFAULT_METADATA_FONT_SIZE - 2)

    def _safe_int(self, value, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
