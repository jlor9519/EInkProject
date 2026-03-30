# Telegram-to-InkyPi Photo Frame

This project turns a Raspberry Pi Zero 2 W and a Waveshare 7.3 inch Spectra 6 display into a remotely controlled photo frame.

The companion app in this repository handles:

- Telegram bot polling and conversations
- whitelist and admin access control
- SQLite persistence for users and image history
- local storage for originals, rendered bridge images, and current InkyPi payload
- setup automation and systemd integration

InkyPi remains the display backend. During setup, this project injects a custom InkyPi plugin into a cloned InkyPi checkout so that panel-specific image handling stays on the InkyPi side.

By default, the setup expects the standard upstream InkyPi layout:

- source checkout: `~/InkyPi`
- runtime install path: `/usr/local/inkypi`
- active source tree: `/usr/local/inkypi/src`, which resolves back to the checkout `src/` directory

## High-level flow

1. A whitelisted user sends a photo to the Telegram bot.
2. The bot asks for location, date, and caption.
3. The app saves the original photo and metadata locally.
4. The app renders a captioned RGB bridge image and writes a canonical bridge payload.
5. The app triggers the configured InkyPi refresh command.
6. The custom InkyPi plugin reads the bridge payload and returns a `PIL.Image` for display.
7. The app stores the result in SQLite.

## Layout

```text
app/                         Python companion app
config/                      Example config and systemd unit
integrations/inkypi_plugin/  Injected InkyPi plugin source
scripts/                     Install, update, and debug helpers
tests/                       Unit tests for core behavior
```

## Prerequisites

Before setting up, you will need:

- A **Telegram bot token** from [@BotFather](https://t.me/BotFather)
- Your **Telegram user ID** (send `/start` to [@userinfobot](https://t.me/userinfobot) to find it)

## Setup

1. Enable SPI (required for the e-ink display):
   ```bash
   sudo raspi-config nonint do_spi 0
   ```
2. Clone this repository and run the installer:
   ```bash
   git clone https://github.com/jlor9519/EInkProject.git ~/EInkProject && cd ~/EInkProject
   bash scripts/install.sh
   ```
   Run the installer as your normal user, not with `sudo`. The script uses `sudo` internally
   for apt and systemd when needed.
3. Have these ready before you start:
   - Telegram bot token
   - your Telegram user ID

### Validate the install

After the installer finishes:

1. Check the service:
   ```bash
   systemctl status photo-frame.service
   ```
2. In Telegram, run `/myid` and `/status`, then send one test photo.
3. Confirm your Telegram user ID is both an admin and whitelisted. If those values were entered
   incorrectly, the bot may start but reject your uploads.

### Updating

Run `bash scripts/update.sh` to pull the latest changes and restart the service.

### Uninstalling

To remove everything the installer set up:

1. Stop and disable the systemd service:
   ```bash
   sudo systemctl stop photo-frame.service
   sudo systemctl disable photo-frame.service
   sudo rm /etc/systemd/system/photo-frame.service
   sudo systemctl daemon-reload
   ```
2. Remove the sudoers file the installer created:
   ```bash
   sudo rm -f /etc/sudoers.d/photo-frame-inkypi
   ```
3. Delete the project directory (contains the app, config, database, and photos):
   ```bash
   rm -rf ~/EInkProject
   ```
4. Optionally remove InkyPi if you no longer need it:
   ```bash
   rm -rf ~/InkyPi
   sudo rm -rf /usr/local/inkypi
   ```
5. Optionally remove the apt packages that were installed exclusively for this project:
   ```bash
   sudo apt-get remove --autoremove python3-venv python3-pip fonts-dejavu-core
   ```
   Skip this step if you use those packages for other things.

## Development

If you want to rehearse the shell prompt flow on a development machine without touching system services, run:

```bash
bash scripts/mock_install.sh
```

That mock flow writes its state under `mock-installation/`, injects the plugin into a fake InkyPi checkout, and skips privileged system changes.

If you want to test only the Telegram bot flow on a development machine in the foreground, run:

```bash
bash scripts/test_telegram_bot.sh
```

That runner uses isolated state under `telegram-bot-test/`, mocks display refresh with `echo`, and stops the bot as soon as you end the script with `Ctrl-C` or close the terminal.

## Notes

- The default Waveshare model is set to `epd7in3e`, which matches the Waveshare 7.3 inch E6 documentation.
- The default render size is `800x480`.
- The default InkyPi source checkout path is `~/InkyPi`, and the default runtime install path is `/usr/local/inkypi`.
- Exact InkyPi refresh behavior is intentionally configurable because the validated local command may differ between installations.
