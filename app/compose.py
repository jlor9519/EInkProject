"""Draws the final frame image: the photo with its caption bar, or the "no images yet" screen.

The panel output and the Telegram preview both use these functions, so the preview always
matches what the frame shows.
"""

from __future__ import annotations

import re

from PIL import Image, ImageColor, ImageDraw, ImageFilter, ImageFont, ImageOps

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

CAPTION_BACKGROUND_COLOR = "#FFFFFF"
BLUR_RADIUS = 18
ICON_TEXT_GAP = 6
METADATA_LINE_GAP = 2
PLACEHOLDER_TITLE = "Noch keine Bilder"
PLACEHOLDER_HINT = "Sende ein Foto an den Telegram-Bot."


def frame_size(width: int, height: int, orientation: str) -> tuple[int, int]:
    """Size of the upright frame image; the panel's width and height swap in portrait."""
    if orientation == "vertical":
        return height, width
    return width, height


def compose_frame(
    prepared_image: Image.Image,
    *,
    width: int,
    height: int,
    caption_bar_height: int,
    caption: str,
    taken_at: str,
    location: str,
    caption_font_size: int,
    metadata_font_size: int,
    caption_character_limit: int,
    caption_margin: int,
    font_path: str,
    text_color: str,
    background_color: str = CAPTION_BACKGROUND_COLOR,
    fit_mode: str = "fill",
) -> Image.Image:
    caption_bar_height = max(0, min(caption_bar_height, height - 1))
    photo_height = max(1, height - caption_bar_height)
    background = ImageColor.getrgb(background_color)
    final_image = Image.new("RGB", (width, height), background)
    photo_area_size = (width, photo_height)
    prepared_image = prepared_image.convert("RGB")

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
        ).filter(ImageFilter.GaussianBlur(radius=BLUR_RADIUS))
        final_image.paste(blurred_background, (0, 0))

        contained = ImageOps.contain(
            prepared_image,
            photo_area_size,
            method=Image.Resampling.LANCZOS,
        )
        paste_x = (width - contained.width) // 2
        paste_y = (photo_height - contained.height) // 2
        final_image.paste(contained, (paste_x, paste_y))

    if caption_bar_height <= 0:
        return final_image

    draw = ImageDraw.Draw(final_image)
    bar_top = photo_height
    draw.rectangle([(0, bar_top), (width, height)], fill=background)

    caption_font = load_font(font_path, caption_font_size)
    metadata_font = load_font(font_path, metadata_font_size)
    color = ImageColor.getrgb(text_color)
    metadata_lines = _prepare_metadata_lines(
        draw,
        metadata_font,
        taken_at=normalize_text(taken_at),
        location=normalize_text(location),
        max_block_width=_max_metadata_block_width(width, caption_margin),
    )
    metadata_block_width = max((line["width"] for line in metadata_lines), default=0)
    caption_available_width = max(
        1,
        width - (caption_margin * 2) - (metadata_block_width + caption_margin if metadata_block_width else 0),
    )
    text = _truncate_line(
        draw,
        _truncate_characters(normalize_text(caption), caption_character_limit),
        caption_font,
        caption_available_width,
    )

    if text:
        text_bbox = draw.textbbox((0, 0), text, font=caption_font)
        text_height = text_bbox[3] - text_bbox[1]
        text_y = bar_top + max(0, (caption_bar_height - text_height) // 2) - text_bbox[1]
        draw.text((caption_margin, text_y), text, font=caption_font, fill=color)

    if metadata_lines:
        _draw_metadata_block(
            draw,
            metadata_lines,
            width=width,
            bar_top=bar_top,
            caption_bar_height=caption_bar_height,
            caption_margin=caption_margin,
            text_color=color,
            background_color=background,
        )

    return final_image


def compose_placeholder(width: int, height: int, font_path: str) -> Image.Image:
    """The "no images yet" screen, shown before the first upload and after the last delete."""
    image = Image.new("RGB", (width, height), (255, 255, 255))
    draw = ImageDraw.Draw(image)
    short_side = min(width, height)
    max_text_width = int(width * 0.85)
    lines = [
        (PLACEHOLDER_TITLE, _fitting_font(draw, PLACEHOLDER_TITLE, font_path, short_side // 10, max_text_width)),
        (PLACEHOLDER_HINT, _fitting_font(draw, PLACEHOLDER_HINT, font_path, short_side // 22, max_text_width)),
    ]
    boxes = [draw.textbbox((0, 0), text, font=font) for text, font in lines]
    gap = max(10, short_side // 16)
    total_height = sum(box[3] - box[1] for box in boxes) + gap
    y = (height - total_height) // 2
    for (text, font), box in zip(lines, boxes):
        x = (width - (box[2] - box[0])) // 2 - box[0]
        draw.text((x, y - box[1]), text, font=font, fill=(0, 0, 0))
        y += (box[3] - box[1]) + gap
    return image


def load_font(font_path: str, size: int) -> ImageFont.ImageFont:
    if font_path:
        try:
            return ImageFont.truetype(font_path, size=size)
        except OSError:
            pass
    try:
        # Pillow >= 10.1: scalable built-in font, so text keeps its intended size.
        return ImageFont.load_default(size=size)
    except TypeError:
        return ImageFont.load_default()


def normalize_text(text: str) -> str:
    stripped = _UNSUPPORTED_RE.sub("", str(text or ""))
    return " ".join(stripped.split())


def _fitting_font(draw: ImageDraw.ImageDraw, text: str, font_path: str, size: int, max_width: int) -> ImageFont.ImageFont:
    """The font at the given size, made smaller until the text fits into max_width."""
    size = max(12, size)
    font = load_font(font_path, size)
    while size > 12 and draw.textlength(text, font=font) > max_width:
        size -= 2
        font = load_font(font_path, size)
    return font


def _truncate_line(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> str:
    if not text:
        return ""
    candidate = " ".join(text.split())
    ellipsis = "..."
    while candidate and draw.textlength(candidate, font=font) > max_width:
        if draw.textlength(candidate + ellipsis, font=font) <= max_width:
            return candidate + ellipsis
        candidate = candidate[:-1].rstrip()
    return candidate or ellipsis


def _truncate_characters(text: str, limit: int) -> str:
    if not text or limit <= 0 or len(text) <= limit:
        return text
    if limit <= 3:
        return "." * limit
    return text[: limit - 3].rstrip() + "..."


def _max_metadata_block_width(width: int, caption_margin: int) -> int:
    min_caption_width = max(120, width // 3)
    available = width - (caption_margin * 3) - min_caption_width
    return max(120, min(int(width * 0.45), available))


def _icon_size(font: ImageFont.ImageFont) -> int:
    try:
        return max(10, int(getattr(font, "size", 14)) - 2)
    except (TypeError, ValueError):
        return 12


def _prepare_metadata_lines(
    draw: ImageDraw.ImageDraw,
    font: ImageFont.ImageFont,
    *,
    taken_at: str,
    location: str,
    max_block_width: int,
) -> list[dict[str, object]]:
    lines: list[dict[str, object]] = []
    for kind, text in (("date", taken_at), ("location", location)):
        text = normalize_text(text)
        if not text:
            continue
        icon_size = _icon_size(font)
        max_text_width = max(1, max_block_width - icon_size - ICON_TEXT_GAP)
        truncated_text = _truncate_line(draw, text, font, max_text_width)
        if not truncated_text:
            continue
        text_bbox = draw.textbbox((0, 0), truncated_text, font=font)
        text_width = max(0, text_bbox[2] - text_bbox[0])
        text_height = max(0, text_bbox[3] - text_bbox[1])
        lines.append(
            {
                "kind": kind,
                "text": truncated_text,
                "font": font,
                "icon_size": icon_size,
                "width": icon_size + ICON_TEXT_GAP + text_width,
                "height": max(icon_size, text_height),
                "text_bbox": text_bbox,
            }
        )
    return lines


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
    total_height = sum(int(line["height"]) for line in metadata_lines)
    total_height += METADATA_LINE_GAP * max(0, len(metadata_lines) - 1)
    current_y = bar_top + max(0, (caption_bar_height - total_height) // 2)

    for line in metadata_lines:
        line_width = int(line["width"])
        line_height = int(line["height"])
        icon_size = int(line["icon_size"])
        text_bbox = line["text_bbox"]

        line_x = width - caption_margin - line_width
        icon_y = current_y + max(0, (line_height - icon_size) // 2)
        if line["kind"] == "date":
            _draw_calendar_icon(draw, line_x, icon_y, icon_size, text_color, background_color)
        else:
            _draw_location_icon(draw, line_x, icon_y, icon_size, text_color, background_color)

        text_x = line_x + icon_size + ICON_TEXT_GAP
        text_height = text_bbox[3] - text_bbox[1]
        text_y = current_y + max(0, (line_height - text_height) // 2) - text_bbox[1]
        draw.text((text_x, text_y), str(line["text"]), font=line["font"], fill=text_color)
        current_y += line_height + METADATA_LINE_GAP


def _draw_calendar_icon(
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
