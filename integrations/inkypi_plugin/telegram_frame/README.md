# Telegram Frame Plugin

This plugin is injected into an InkyPi checkout by `scripts/setup_inkypi.sh`.

It reads the canonical bridge payload produced by the companion app, opens the current bridge image, and returns that image to InkyPi so InkyPi can continue to own the panel-specific display pipeline.

