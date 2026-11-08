from __future__ import annotations

import unittest

from PIL import Image, ImageDraw

from app.compose import _fitting_font, compose_frame, compose_placeholder, frame_size


def _compose(prepared: Image.Image, *, width: int = 800, height: int = 480, caption_bar_height: int = 44, **overrides):
    kwargs = dict(
        width=width,
        height=height,
        caption_bar_height=caption_bar_height,
        caption="Ein Ausflug ans Meer",
        taken_at="2026-03-18",
        location="Berlin",
        caption_font_size=20,
        metadata_font_size=14,
        caption_character_limit=72,
        caption_margin=18,
        font_path="",
        text_color="#111111",
    )
    kwargs.update(overrides)
    return compose_frame(prepared, **kwargs)


class FrameSizeTests(unittest.TestCase):
    def test_vertical_swaps_width_and_height(self) -> None:
        self.assertEqual(frame_size(800, 480, "horizontal"), (800, 480))
        self.assertEqual(frame_size(800, 480, "vertical"), (480, 800))


class ComposeFrameTests(unittest.TestCase):
    def test_fill_covers_photo_area_and_draws_caption_bar(self) -> None:
        prepared = Image.new("RGB", (1600, 1200), (200, 30, 30))

        image = _compose(prepared)

        self.assertEqual(image.size, (800, 480))
        self.assertEqual(image.getpixel((5, 5)), (200, 30, 30))
        bar = image.crop((0, 436, 800, 480)).convert("L")
        self.assertEqual(bar.getpixel((799, 479 - 436)), 255)  # white bar background
        self.assertLess(min(bar.getdata()), 100)  # caption text was drawn

    def test_contain_keeps_whole_photo_with_blurred_sides(self) -> None:
        prepared = Image.new("RGB", (400, 1200), (10, 120, 10))

        image = _compose(prepared, fit_mode="contain")

        self.assertEqual(image.size, (800, 480))
        self.assertEqual(image.getpixel((400, 200)), (10, 120, 10))

    def test_without_caption_bar_the_photo_fills_the_frame(self) -> None:
        prepared = Image.new("RGB", (1600, 1200), (30, 30, 200))

        image = _compose(prepared, caption_bar_height=0)

        self.assertEqual(image.getpixel((400, 479)), (30, 30, 200))


class PlaceholderTests(unittest.TestCase):
    def test_placeholder_in_both_orientations(self) -> None:
        for size in ((800, 480), (480, 800)):
            with self.subTest(size=size):
                image = compose_placeholder(*size, "")

                self.assertEqual(image.size, size)
                self._assert_has_dark_text(image)

    def test_placeholder_text_is_shrunk_to_fit_the_width(self) -> None:
        draw = ImageDraw.Draw(Image.new("RGB", (10, 10)))
        font = _fitting_font(draw, "Noch keine Bilder", "", 200, 300)

        self.assertLessEqual(draw.textlength("Noch keine Bilder", font=font), 300)
        self.assertLess(font.size, 200)

    def _assert_has_dark_text(self, image) -> None:
        darkest = min(image.convert("L").getdata())
        self.assertLess(darkest, 100)
        # The text must fit: the left and right edge columns stay white (no clipped letters).
        gray = image.convert("L")
        for x in (0, 1, image.width - 2, image.width - 1):
            self.assertEqual(min(gray.getpixel((x, y)) for y in range(image.height)), 255)


if __name__ == "__main__":
    unittest.main()
