from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from PIL import Image

from app.models import DisplayConfig
from app.render import RenderService


class _RecordingDraw:
    def __init__(self, widths: dict[tuple[str, object, bool], float], bboxes: dict[tuple[str, object, bool], tuple[int, int, int, int]]) -> None:
        self.widths = widths
        self.bboxes = bboxes
        self.text_calls: list[dict[str, object]] = []

    def textlength(self, text: str, *, font=None, embedded_color: bool = False):
        return self.widths[(text, font, embedded_color)]

    def textbbox(self, xy, text: str, *, font=None, embedded_color: bool = False):
        return self.bboxes[(text, font, embedded_color)]

    def text(self, xy, text: str, *, font=None, fill=None, embedded_color: bool = False):
        self.text_calls.append(
            {
                "xy": xy,
                "text": text,
                "font": font,
                "fill": fill,
                "embedded_color": embedded_color,
            }
        )


class RenderTests(unittest.TestCase):
    def test_render_creates_normalized_png_without_cropping(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.jpg"
            output = Path(tmpdir) / "output.png"
            Image.new("RGB", (1600, 1200), (200, 120, 80)).save(source)

            renderer = RenderService(
                DisplayConfig(
                    width=800,
                    height=480,
                    caption_height=132,
                    margin=18,
                    metadata_font_size=22,
                    caption_font_size=28,
                    caption_character_limit=72,
                    max_caption_lines=2,
                    font_path="/tmp/does-not-exist.ttf",
                    background_color="#F7F3EA",
                    text_color="#111111",
                    divider_color="#3A3A3A",
                )
            )
            renderer.render(
                source,
                output,
                location="Berlin",
                taken_at="2026-03-18",
                caption="A test caption that should wrap cleanly on the rendered output.",
            )

            self.assertTrue(output.exists())
            with Image.open(output) as image:
                self.assertEqual(image.size, (1600, 1200))
                self.assertEqual(image.mode, "RGB")

    def test_render_preserves_portrait_orientation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "portrait.jpg"
            output = Path(tmpdir) / "output.png"
            Image.new("RGB", (900, 1600), (12, 140, 220)).save(source)

            renderer = RenderService(
                DisplayConfig(
                    width=800,
                    height=480,
                    caption_height=132,
                    margin=18,
                    metadata_font_size=22,
                    caption_font_size=28,
                    caption_character_limit=72,
                    max_caption_lines=2,
                    font_path="/tmp/does-not-exist.ttf",
                    background_color="#F7F3EA",
                    text_color="#111111",
                    divider_color="#3A3A3A",
                )
            )
            renderer.render(
                source,
                output,
                location="",
                taken_at="",
                caption="",
            )

            with Image.open(output) as image:
                self.assertEqual(image.size, (900, 1600))
                self.assertEqual(image.mode, "RGB")

    def test_normalize_text_preserves_emoji(self) -> None:
        renderer = RenderService(
            DisplayConfig(
                width=800,
                height=480,
                caption_height=44,
                margin=18,
                metadata_font_size=14,
                caption_font_size=20,
                caption_character_limit=72,
                max_caption_lines=1,
                font_path="/tmp/does-not-exist.ttf",
                background_color="#F7F3EA",
                text_color="#111111",
                divider_color="#3A3A3A",
            )
        )

        self.assertEqual(renderer._normalize_text(" Urlaub   😊  in  Rom "), "Urlaub 😊 in Rom")

    def test_split_text_runs_separates_emoji_from_text(self) -> None:
        renderer = RenderService(
            DisplayConfig(
                width=800,
                height=480,
                caption_height=44,
                margin=18,
                metadata_font_size=14,
                caption_font_size=20,
                caption_character_limit=72,
                max_caption_lines=1,
                font_path="/tmp/does-not-exist.ttf",
                background_color="#F7F3EA",
                text_color="#111111",
                divider_color="#3A3A3A",
            )
        )
        text_font = object()
        emoji_font = object()

        runs = renderer._split_text_runs("Urlaub 😊 in Rom", text_font, emoji_font)

        self.assertEqual([run["text"] for run in runs], ["Urlaub ", "😊", " in Rom"])
        self.assertEqual([run["font"] for run in runs], [text_font, emoji_font, text_font])
        self.assertEqual([run["embedded_color"] for run in runs], [False, True, False])

    def test_draw_text_runs_uses_embedded_color_for_emoji_runs(self) -> None:
        renderer = RenderService(
            DisplayConfig(
                width=800,
                height=480,
                caption_height=44,
                margin=18,
                metadata_font_size=14,
                caption_font_size=20,
                caption_character_limit=72,
                max_caption_lines=1,
                font_path="/tmp/does-not-exist.ttf",
                background_color="#F7F3EA",
                text_color="#111111",
                divider_color="#3A3A3A",
            )
        )
        text_font = object()
        emoji_font = object()
        runs = renderer._split_text_runs("Hi 😊", text_font, emoji_font)
        draw = _RecordingDraw(
            widths={
                ("Hi ", text_font, False): 12.0,
                ("😊", emoji_font, True): 8.0,
            },
            bboxes={
                ("Hi ", text_font, False): (0, -2, 12, 8),
                ("😊", emoji_font, True): (0, -1, 8, 9),
            },
        )

        renderer._draw_text_runs(draw, 10, 20, runs, fill=(17, 17, 17))

        self.assertEqual(len(draw.text_calls), 2)
        self.assertEqual(draw.text_calls[0]["text"], "Hi ")
        self.assertFalse(draw.text_calls[0]["embedded_color"])
        self.assertEqual(draw.text_calls[1]["text"], "😊")
        self.assertTrue(draw.text_calls[1]["embedded_color"])


if __name__ == "__main__":
    unittest.main()
