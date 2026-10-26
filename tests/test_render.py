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
                    font_path="/tmp/does-not-exist.ttf",
                    text_color="#111111",
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
                    font_path="/tmp/does-not-exist.ttf",
                    text_color="#111111",
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


    def test_render_caps_large_images_and_keeps_exif_rotation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "large.jpg"
            output = Path(tmpdir) / "output.png"
            exif = Image.Exif()
            exif[0x0112] = 6  # Orientation: rotate 90° clockwise to display upright
            Image.new("RGB", (4000, 3000), (10, 200, 30)).save(source, exif=exif)

            renderer = RenderService(
                DisplayConfig(
                    width=800,
                    height=480,
                    caption_height=44,
                    margin=18,
                    metadata_font_size=14,
                    caption_font_size=20,
                    caption_character_limit=72,
                    font_path="/tmp/does-not-exist.ttf",
                    text_color="#111111",
                )
            )
            renderer.render(source, output, location="", taken_at="", caption="")

            with Image.open(output) as image:
                # Twice the panel's long side, upright (portrait) and with the aspect ratio kept.
                self.assertEqual(image.size, (1200, 1600))


    def test_render_keeps_panoramas_sharp_enough_to_fill_the_panel(self) -> None:
        # A 4:1 panorama must keep its short edge at the panel's long edge (800 px), so "fill"
        # on a portrait frame (480x800) never has to enlarge it.
        for size, expected in (((8000, 2000), (3200, 800)), ((2000, 8000), (800, 3200))):
            with self.subTest(size=size), tempfile.TemporaryDirectory() as tmpdir:
                source = Path(tmpdir) / "panorama.jpg"
                output = Path(tmpdir) / "output.png"
                Image.new("RGB", size, (10, 200, 30)).save(source)

                _renderer().render(source, output, location="", taken_at="", caption="")

                with Image.open(output) as image:
                    self.assertEqual(image.size, expected)

    def test_render_caps_pixels_of_extreme_panoramas(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "panorama.jpg"
            output = Path(tmpdir) / "output.png"
            Image.new("RGB", (24000, 1200), (10, 200, 30)).save(source)

            _renderer().render(source, output, location="", taken_at="", caption="")

            with Image.open(output) as image:
                # About 6 MP; rounding of the short edge may add a fraction of a percent.
                self.assertLessEqual(image.width * image.height, 6_060_000)
                self.assertGreater(image.width, 10000)


def _renderer() -> RenderService:
    return RenderService(
        DisplayConfig(
            width=800,
            height=480,
            caption_height=44,
            margin=18,
            metadata_font_size=14,
            caption_font_size=20,
            caption_character_limit=72,
            font_path="/tmp/does-not-exist.ttf",
            text_color="#111111",
        )
    )


if __name__ == "__main__":
    unittest.main()
