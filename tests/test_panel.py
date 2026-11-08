from __future__ import annotations

import os
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

from PIL import Image

from app.panel import FilePanel, PanelError, WaveshareFramePanel, prepare_panel_image

# A stand-in for a Waveshare driver package: same layout (`from . import epdconfig`) and the
# methods the panel uses. It records every call into the file named by FAKE_EPD_LOG.
FAKE_DRIVER = textwrap.dedent(
    """
    import os
    import time

    from . import epdconfig


    def _log(entry):
        with open(os.environ["FAKE_EPD_LOG"], "a", encoding="utf-8") as handle:
            handle.write(entry + "\\n")


    class EPD:
        width = 800
        height = 480

        def init(self):
            _log("init")
            return epdconfig.INIT_RESULT

        def Clear(self):
            _log("Clear")

        def getbuffer(self, image):
            _log(f"getbuffer {image.size[0]}x{image.size[1]}")
            return b"buffer"

        def display(self, buffer):
            delay = float(os.environ.get("FAKE_EPD_DELAY", "0"))
            if delay:
                time.sleep(delay)
            _log("display")

        def sleep(self):
            _log("sleep")
    """
)


class PreparePanelImageTests(unittest.TestCase):
    def _marked_upright(self, size: tuple[int, int]) -> Image.Image:
        image = Image.new("RGB", size, (255, 255, 255))
        image.putpixel((0, 0), (255, 0, 0))  # top-left marker
        return image

    def _prepare(self, image: Image.Image, orientation: str, inverted: bool) -> Image.Image:
        return prepare_panel_image(
            image,
            panel_size=(800, 480),
            orientation=orientation,
            inverted=inverted,
            image_settings={},
        )

    def test_rotation_matches_inkypi_for_each_orientation(self) -> None:
        # (orientation, inverted, upright size, where the top-left marker ends up)
        cases = [
            ("horizontal", False, (800, 480), (0, 0)),
            ("horizontal", True, (800, 480), (799, 479)),
            ("vertical", False, (480, 800), (0, 479)),
            ("vertical", True, (480, 800), (799, 0)),
        ]
        for orientation, inverted, upright_size, marker in cases:
            with self.subTest(orientation=orientation, inverted=inverted):
                result = self._prepare(self._marked_upright(upright_size), orientation, inverted)

                self.assertEqual(result.size, (800, 480))
                self.assertEqual(result.getpixel(marker), (255, 0, 0))

    def test_enhancements_are_skipped_at_1_and_applied_otherwise(self) -> None:
        image = Image.new("RGB", (800, 480), (120, 90, 60))
        neutral = {"saturation": 1.0, "contrast": 1.0, "sharpness": 1.0, "brightness": 1.0}

        unchanged = prepare_panel_image(
            image, panel_size=(800, 480), orientation="horizontal", inverted=False, image_settings=neutral
        )
        brighter = prepare_panel_image(
            image, panel_size=(800, 480), orientation="horizontal", inverted=False, image_settings={"brightness": 1.5}
        )

        self.assertEqual(unchanged.getpixel((10, 10)), (120, 90, 60))
        self.assertEqual(brighter.getpixel((10, 10)), (180, 135, 90))


class WaveshareFramePanelTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.driver_dir = self.tmp / "vendor" / "waveshare_epd"
        self.driver_dir.mkdir(parents=True)
        (self.driver_dir / "epdfake.py").write_text(FAKE_DRIVER, encoding="utf-8")
        (self.driver_dir / "epdconfig.py").write_text("INIT_RESULT = 0\n", encoding="utf-8")
        self.log = self.tmp / "calls.log"
        env = patch.dict(os.environ, {"FAKE_EPD_LOG": str(self.log), "FAKE_EPD_DELAY": "0"})
        env.start()
        self.addCleanup(env.stop)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _panel(self, **kwargs) -> WaveshareFramePanel:
        return WaveshareFramePanel("epdfake", self.driver_dir, work_dir=self.tmp / "work", **kwargs)

    def _calls(self) -> list[str]:
        return self.log.read_text(encoding="utf-8").splitlines()

    def test_show_wipes_then_draws_then_sleeps(self) -> None:
        self._panel().show(Image.new("RGB", (800, 480), (0, 0, 0)))

        self.assertEqual(self._calls(), ["init", "Clear", "getbuffer 800x480", "display", "sleep"])
        self.assertEqual(list((self.tmp / "work").iterdir()), [])  # temporary image removed

    def test_show_can_skip_the_wipe(self) -> None:
        self._panel(clear_before_display=False).show(Image.new("RGB", (800, 480)))

        self.assertEqual(self._calls(), ["init", "getbuffer 800x480", "display", "sleep"])

    def test_failed_init_is_reported(self) -> None:
        (self.driver_dir / "epdconfig.py").write_text("INIT_RESULT = -1\n", encoding="utf-8")

        with self.assertRaises(PanelError) as caught:
            self._panel().show(Image.new("RGB", (800, 480)))

        self.assertIn("nicht initialisiert", str(caught.exception))

    def test_a_hanging_panel_is_stopped_after_the_timeout(self) -> None:
        with patch.dict(os.environ, {"FAKE_EPD_DELAY": "30"}):
            with self.assertRaises(PanelError) as caught:
                self._panel(timeout_seconds=2).show(Image.new("RGB", (800, 480)))

        self.assertIn("nach 2 Sekunden nicht reagiert", str(caught.exception))

    def test_missing_driver_is_reported_by_status_and_show(self) -> None:
        panel = WaveshareFramePanel("epd7in3e", self.driver_dir, work_dir=self.tmp / "work")

        ok, description = panel.status()
        self.assertFalse(ok)
        self.assertIn("Treiber fehlt", description)
        with self.assertRaises(PanelError) as caught:
            panel.show(Image.new("RGB", (800, 480)))
        self.assertIn("epd7in3e.py fehlt", str(caught.exception))


class FilePanelTests(unittest.TestCase):
    def test_writes_the_panel_image(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "out" / "panel.png"

            FilePanel(output).show(Image.new("RGB", (800, 480), (1, 2, 3)))

            with Image.open(output) as written:
                self.assertEqual(written.size, (800, 480))
                self.assertEqual(written.convert("RGB").getpixel((0, 0)), (1, 2, 3))


if __name__ == "__main__":
    unittest.main()
