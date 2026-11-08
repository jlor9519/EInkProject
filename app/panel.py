"""Sends finished frame images to the e-paper panel.

The Waveshare driver waits for the panel's BUSY line without a time limit, so a stuck panel would
block forever. Each refresh therefore runs in a short-lived child process (`python -m app.panel`)
that is killed after `display.refresh_timeout_seconds`. That also releases the GPIO and SPI
handles after every refresh.
"""

from __future__ import annotations

import argparse
import importlib
import inspect
import logging
import os
import subprocess
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any, Protocol

from PIL import Image, ImageEnhance, ImageOps

from app.models import DisplayConfig

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Applied in this order, each only when its factor is not 1.0 (same as InkyPi did).
_ENHANCERS = (
    ("brightness", ImageEnhance.Brightness),
    ("contrast", ImageEnhance.Contrast),
    ("saturation", ImageEnhance.Color),
    ("sharpness", ImageEnhance.Sharpness),
)


class PanelError(RuntimeError):
    """The panel could not show the image."""


class Panel(Protocol):
    def show(self, image: Image.Image, *, clear: bool | None = None) -> None: ...

    def status(self) -> tuple[bool, str]: ...


def prepare_panel_image(
    image: Image.Image,
    *,
    panel_size: tuple[int, int],
    orientation: str,
    inverted: bool,
    image_settings: dict[str, Any] | None,
) -> Image.Image:
    """Turn the upright frame image into what the panel expects: rotated, sized and tuned."""
    angle = 90 if orientation == "vertical" else 0
    if inverted:
        angle = (angle + 180) % 360
    if angle:
        image = image.rotate(angle, expand=True)
    if image.mode not in ("RGB", "L"):
        image = image.convert("RGB")
    if image.size != panel_size:
        image = ImageOps.fit(image, panel_size, method=Image.Resampling.LANCZOS, centering=(0.5, 0.5))

    settings = image_settings or {}
    for key, enhancer in _ENHANCERS:
        factor = _factor(settings.get(key))
        if factor != 1.0:
            image = enhancer(image).enhance(factor)
    return image


def _factor(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 1.0


class WaveshareFramePanel:
    def __init__(
        self,
        model: str,
        driver_dir: Path,
        *,
        clear_before_display: bool = True,
        timeout_seconds: int = 300,
        work_dir: Path | None = None,
    ):
        self.model = model
        self.driver_dir = driver_dir
        self.clear_before_display = clear_before_display
        self.timeout_seconds = timeout_seconds
        self.work_dir = work_dir or Path(tempfile.gettempdir())
        self._lock = threading.Lock()

    def status(self) -> tuple[bool, str]:
        if not (self.driver_dir / f"{self.model}.py").exists():
            return False, f"Waveshare {self.model}: Treiber fehlt in {self.driver_dir}"
        return True, f"Waveshare {self.model}"

    def show(self, image: Image.Image, *, clear: bool | None = None) -> None:
        """Show the image. `clear` overrides clear_before_display for this refresh."""
        if clear is None:
            clear = self.clear_before_display
        with self._lock:
            self.work_dir.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=self.work_dir, prefix=".panel-", suffix=".png", delete=False) as handle:
                image_path = Path(handle.name)
            try:
                image.save(image_path, format="PNG")
                command = [
                    sys.executable,
                    "-m",
                    "app.panel",
                    "--model",
                    self.model,
                    "--driver-dir",
                    str(self.driver_dir),
                    "--clear" if clear else "--no-clear",
                    str(image_path),
                ]
                logger.info("Refreshing Waveshare %s panel", self.model)
                try:
                    completed = subprocess.run(
                        command,
                        cwd=PROJECT_ROOT,
                        capture_output=True,
                        text=True,
                        timeout=self.timeout_seconds,
                        check=False,
                    )
                except subprocess.TimeoutExpired as exc:
                    raise PanelError(
                        f"Das Display hat nach {self.timeout_seconds} Sekunden nicht reagiert."
                    ) from exc
                if completed.returncode != 0:
                    lines = [line for line in (completed.stderr or completed.stdout).splitlines() if line.strip()]
                    raise PanelError(lines[-1] if lines else f"Panel-Prozess endete mit Code {completed.returncode}.")
            finally:
                try:
                    image_path.unlink()
                except FileNotFoundError:
                    pass


class FilePanel:
    """Writes the panel image to a PNG file instead of a real panel (development and tests)."""

    def __init__(self, output_path: Path):
        self.output_path = output_path

    def status(self) -> tuple[bool, str]:
        return True, f"Datei {self.output_path}"

    def show(self, image: Image.Image, *, clear: bool | None = None) -> None:
        # A file has no previous image to wipe; `clear` only matters for a real panel.
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=self.output_path.parent, suffix=".png", delete=False) as handle:
            temp_path = Path(handle.name)
        try:
            image.save(temp_path, format="PNG")
            temp_path.replace(self.output_path)
        except OSError as exc:
            temp_path.unlink(missing_ok=True)
            raise PanelError(f"Panel-Datei konnte nicht geschrieben werden: {exc}") from exc


def make_panel(config: DisplayConfig, work_dir: Path) -> Panel:
    if config.panel == "file":
        return FilePanel(config.panel_output_path)
    return WaveshareFramePanel(
        config.waveshare_model,
        config.driver_dir,
        clear_before_display=config.clear_before_display,
        timeout_seconds=config.refresh_timeout_seconds,
        work_dir=work_dir,
    )


# ----------------------------------------------------------------------
# Child process: talks to the panel hardware.
# ----------------------------------------------------------------------


def push_to_waveshare(image_path: Path, model: str, driver_dir: Path, *, clear: bool) -> None:
    driver_file = driver_dir / f"{model}.py"
    if not driver_file.exists():
        raise PanelError(f"Waveshare-Treiber {driver_file} fehlt. Bitte scripts/update.sh ausführen.")

    # The drivers use `from . import epdconfig`, so import them as part of their package.
    for path in (str(driver_dir.parent), str(driver_dir)):
        if path not in sys.path:
            sys.path.insert(0, path)
    module = importlib.import_module(f"{driver_dir.name}.{model}")
    epd = module.EPD()
    init = getattr(epd, "init", None) or getattr(epd, "Init", None)
    if not callable(init):
        raise PanelError(f"Waveshare-Treiber {model} hat keine init-Methode.")
    if len(inspect.signature(epd.display).parameters) > 1:
        raise PanelError(f"Zweifarbige Panels wie {model} werden nicht unterstützt.")

    with Image.open(image_path) as opened:
        image = opened.convert("RGB")

    if init() == -1:
        raise PanelError("Das Display konnte nicht initialisiert werden (SPI aktiviert?).")
    if clear:
        epd.Clear()
    epd.display(epd.getbuffer(image))
    epd.sleep()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Show one image on the Waveshare panel")
    parser.add_argument("--model", required=True)
    parser.add_argument("--driver-dir", required=True)
    parser.add_argument("--clear", dest="clear", action="store_true", default=True)
    parser.add_argument("--no-clear", dest="clear", action="store_false")
    parser.add_argument("image")
    args = parser.parse_args(argv)
    logging.basicConfig(level=os.getenv("PANEL_LOG_LEVEL", "WARNING"))
    try:
        push_to_waveshare(Path(args.image), args.model, Path(args.driver_dir), clear=args.clear)
    except Exception as exc:  # reported to the parent process via stderr
        print(f"{exc}" if isinstance(exc, PanelError) else f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
