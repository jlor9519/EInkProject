from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_INSTALL_PATH = Path("/usr/local/inkypi")
STALE_REPO_PATH = Path("/opt/InkyPi")


@dataclass(slots=True)
class ResolvedInkyPiLayout:
    repo_path: Path
    install_path: Path
    source_root: Path
    git_sync_path: Path | None
    source_origin: str
    replaced_stale_repo_path: bool
    install_src_exists: bool

    @property
    def device_config_path(self) -> Path:
        return self.source_root / "config" / "device.json"

    def plugin_dir(self, plugin_id: str) -> Path:
        return self.source_root / "plugins" / plugin_id


def resolve_inkypi_layout(
    configured_repo_path: str | os.PathLike[str] | None,
    configured_install_path: str | os.PathLike[str] | None,
    *,
    home_dir: str | os.PathLike[str] | None = None,
    cwd: str | os.PathLike[str] | None = None,
) -> ResolvedInkyPiLayout:
    home_path = _resolve_home(home_dir)
    cwd_path = Path(cwd).resolve() if cwd is not None else Path.cwd()
    default_repo_path = home_path / "InkyPi"

    repo_candidate = _expand_path(configured_repo_path, default_repo_path, home_path, cwd_path)
    install_path = _expand_path(configured_install_path, DEFAULT_INSTALL_PATH, home_path, cwd_path)
    install_src = install_path / "src"
    replaced_stale_repo_path = False

    if install_src.exists():
        source_root = install_src.resolve()
        source_repo = source_root.parent
        git_sync_path = source_repo if (source_repo / ".git").exists() else None
        repo_path = source_repo
        if git_sync_path is None and (repo_candidate / ".git").exists():
            git_sync_path = repo_candidate
            repo_path = repo_candidate
        if repo_candidate == STALE_REPO_PATH and not repo_candidate.exists() and repo_path != repo_candidate:
            replaced_stale_repo_path = True
        return ResolvedInkyPiLayout(
            repo_path=repo_path,
            install_path=install_path,
            source_root=source_root,
            git_sync_path=git_sync_path,
            source_origin="install_path",
            replaced_stale_repo_path=replaced_stale_repo_path,
            install_src_exists=True,
        )

    if repo_candidate == STALE_REPO_PATH and not repo_candidate.exists():
        repo_candidate = default_repo_path
        replaced_stale_repo_path = True

    repo_src = repo_candidate / "src"
    if repo_src.exists():
        repo_path = repo_candidate.resolve() if repo_candidate.exists() else repo_candidate
        source_root = repo_src.resolve()
        git_sync_path = repo_path if (repo_path / ".git").exists() else None
        source_origin = "repo_path"
    else:
        repo_path = repo_candidate
        source_root = repo_candidate / "src"
        git_sync_path = None
        source_origin = "planned_clone"

    return ResolvedInkyPiLayout(
        repo_path=repo_path,
        install_path=install_path,
        source_root=source_root,
        git_sync_path=git_sync_path,
        source_origin=source_origin,
        replaced_stale_repo_path=replaced_stale_repo_path,
        install_src_exists=False,
    )


def _resolve_home(home_dir: str | os.PathLike[str] | None) -> Path:
    if home_dir is not None:
        return Path(home_dir).resolve()
    return Path.home().resolve()


def _expand_path(
    value: str | os.PathLike[str] | None,
    default: Path,
    home_path: Path,
    cwd_path: Path,
) -> Path:
    if value is None or str(value).strip() == "":
        path = default
    else:
        text = str(value).strip()
        if text == "~":
            text = str(home_path)
        elif text.startswith("~/"):
            text = str(home_path / text[2:])
        text = os.path.expandvars(text)
        path = Path(text)
        if not path.is_absolute():
            path = cwd_path / path
    return path.resolve(strict=False)
