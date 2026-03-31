from __future__ import annotations

ORIENTATION_VERTICAL = "vertical"
ORIENTATION_HORIZONTAL = "horizontal"
ORIENTATION_SHARED = "shared"

ACTIVE_ORIENTATIONS = {ORIENTATION_VERTICAL, ORIENTATION_HORIZONTAL}

ORIENTATION_LABELS = {
    ORIENTATION_VERTICAL: "Hochformat",
    ORIENTATION_HORIZONTAL: "Querformat",
    ORIENTATION_SHARED: "Beide Ausrichtungen",
}


def normalize_orientation_value(text: str) -> str | None:
    normalized = " ".join(text.strip().lower().split())
    mapping = {
        "horizontal": ORIENTATION_HORIZONTAL,
        "landscape": ORIENTATION_HORIZONTAL,
        "waagerecht": ORIENTATION_HORIZONTAL,
        "querformat": ORIENTATION_HORIZONTAL,
        "vertical": ORIENTATION_VERTICAL,
        "vertikal": ORIENTATION_VERTICAL,
        "hochformat": ORIENTATION_VERTICAL,
        "portrait": ORIENTATION_VERTICAL,
        "porträt": ORIENTATION_VERTICAL,
    }
    return mapping.get(normalized)


def format_orientation_label(orientation: str | None) -> str:
    if not orientation:
        return "Unbekannt"
    return ORIENTATION_LABELS.get(orientation, orientation)


def orientation_pool(active_orientation: str | None) -> tuple[str, ...]:
    if active_orientation in ACTIVE_ORIENTATIONS:
        return (active_orientation, ORIENTATION_SHARED)
    return (ORIENTATION_SHARED,)


def orientation_matches(bucket: str | None, active_orientation: str | None) -> bool:
    return (bucket or ORIENTATION_SHARED) in orientation_pool(active_orientation)
