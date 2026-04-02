from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from PIL import Image

from app.models import DisplayConfig
from app.render import RenderService


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


if __name__ == "__main__":
    unittest.main()
