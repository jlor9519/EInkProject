# Telegram E-Paper Photo Frame

This project turns a Raspberry Pi Zero 2 W or Pi Zero W and a Waveshare 7.3 inch Spectra 6 display into a remotely controlled photo frame.

The app in this repository handles:

- Telegram bot polling and conversations
- whitelist and admin access control
- SQLite persistence for users, image history and frame settings
- local storage for originals, prepared images and the current image
- drawing the frame (photo plus caption bar) and sending it to the panel through Waveshare's own driver
- setup automation and systemd integration

Earlier versions used [InkyPi](https://github.com/fatihak/InkyPi) as the display backend. That is no longer needed; see [Migrating a frame that still uses InkyPi](#migrating-a-frame-that-still-uses-inkypi).

## High-level flow

1. A whitelisted user sends a photo to the Telegram bot.
2. The bot asks for location, date, and caption.
3. The app saves the original photo and metadata locally, plus a prepared copy sized for the panel.
4. The app draws the frame image, rotates it for the frame's orientation and applies the colour tuning.
5. A short-lived child process sends the image to the panel. It is stopped after
   `display.refresh_timeout_seconds` if the panel hangs, so the bot never gets stuck.
6. Only once the panel accepted the image does it become the current image. A failed refresh leaves the previous image in place.

## Layout

```text
app/        Python app (bot, frame drawing, panel output)
config/     Example config and systemd unit
scripts/    Install, update, and debug helpers
tests/      Unit tests
vendor/     Waveshare driver, downloaded by the install/update scripts (not in git)
```

## Prerequisites

- A Raspberry Pi Zero 2 W or Pi Zero W with **Raspberry Pi OS Lite**. The Pi Zero W only runs the **32-bit** version; Raspberry Pi Imager offers the right one when you pick the board.
- A **Telegram bot token** from [@BotFather](https://t.me/BotFather). Each frame needs **its own bot**: Telegram delivers a bot's messages to only one device, so two frames sharing a token keep interrupting each other.
- Your **Telegram user ID** (send `/start` to [@userinfobot](https://t.me/userinfobot) to find it)

## Setup

1. Clone this repository and run the installer:
   ```bash
   git clone https://github.com/jlor9519/EInkProject.git && cd ~/EInkProject
   bash scripts/install.sh
   ```
   Run the installer as your normal user, not with `sudo`. The script uses `sudo` internally
   for apt and systemd when needed.
2. The installer installs the apt packages, downloads the Waveshare driver, enables SPI and adds
   your user to the `spi` and `gpio` groups. If SPI was off, it asks you to **reboot** once; the
   photo frame starts by itself after the reboot.

On a Pi Zero W the first install takes a while, but nothing has to be compiled: Pillow, PyYAML
and the GPIO/SPI libraries come prebuilt from apt.

### Validate the install

1. Check the service:
   ```bash
   systemctl status photo-frame.service
   ```
2. In Telegram, run `/myid` and `/status`, then send one test photo. `/status` shows a
   `Display:` line; `✗ … Treiber fehlt` means the Waveshare driver was not downloaded (rerun `scripts/update.sh`).
3. Confirm your Telegram user ID is both an admin and whitelisted. If those values were entered
   incorrectly, the bot may start but reject your uploads.

### Updating

Run `bash scripts/update.sh` to pull the latest changes and restart the service. `--quick` skips
the apt packages; `--clear-images` removes all stored images.

### Migrating a frame that still uses InkyPi

On a frame set up with an older version, update in this order:

```bash
cd ~/EInkProject
git pull
bash scripts/update.sh
```

Run `git pull` by itself first. The old `update.sh` would otherwise keep running its own outdated
steps after pulling the new version.

The update then:

- installs the panel libraries from apt and downloads the Waveshare driver into `vendor/`,
- turns off `inkypi.service`, so only the photo frame uses the panel,
- restarts the photo frame, which imports orientation, colour tuning, slideshow interval and quiet
  hours once from InkyPi's `device.json`.

InkyPi's files are left in place, so you can go back by checking out the previous version and running
`sudo systemctl enable --now inkypi.service`. Once everything works, remove InkyPi with:

```bash
sudo rm -rf ~/InkyPi /usr/local/inkypi
sudo rm -f /etc/systemd/system/inkypi.service /etc/sudoers.d/photo-frame-inkypi
sudo systemctl daemon-reload
```

### Uninstalling

To remove everything the installer set up:

1. Stop and disable the systemd service:
   ```bash
   sudo systemctl stop photo-frame.service
   sudo systemctl disable photo-frame.service
   sudo rm /etc/systemd/system/photo-frame.service
   sudo systemctl daemon-reload
   ```
2. Remove the sudoers file older versions created, if it is still there:
   ```bash
   sudo rm -f /etc/sudoers.d/photo-frame-maintenance
   ```
3. Delete the project directory (contains the app, config, database, and photos):
   ```bash
   rm -rf ~/EInkProject
   ```
4. Optionally remove the apt packages that were installed exclusively for this project:
   ```bash
   sudo apt-get remove --autoremove python3-venv python3-pip fonts-dejavu-core \
     python3-pil python3-yaml python3-spidev python3-gpiozero python3-lgpio
   ```
   Skip this step if you use those packages for other things.

## Development

If you want to rehearse the shell prompt flow on a development machine without touching system services, run:

```bash
bash scripts/mock_install.sh
```

That mock flow writes its state under `mock-installation/` and skips privileged system changes.

If you want to test the Telegram bot flow on a development machine in the foreground, run:

```bash
bash scripts/test_telegram_bot.sh
```

That runner uses isolated state under `telegram-bot-test/` and a simulated panel: every refresh
writes `telegram-bot-test/data/panel.png`, rotated and colour-tuned exactly as the real panel
would receive it. Stop the bot with `Ctrl-C` or by closing the terminal.

Run the tests with `.venv/bin/python -m pytest`.

## Notes

- Panel settings live in the `display` section of `config/config.yaml`:
  - `waveshare_model` (default `epd7in3e`, the Waveshare 7.3 inch E6) picks the driver that is downloaded.
  - `clear_before_display` (default `true`) wipes the panel to white before each image. That avoids
    ghosting but doubles the refresh time; set it to `false` to try faster refreshes.
  - `refresh_timeout_seconds` (default `300`) is how long one refresh may take before it is stopped.
  - `panel: file` writes the panel image to `panel_output_path` instead of driving a panel.
- Orientation, colour tuning, slideshow interval and quiet hours are changed in the bot's `/settings` menu and stored in the database.
- The default render size is `800x480`.
