from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.inkypi_paths import resolve_inkypi_layout


class InkyPiPathResolutionTests(unittest.TestCase):
    def test_prefers_runtime_src_symlink_and_uses_checkout_parent_for_repo(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            home_dir = tmpdir_path / "home" / "inky"
            repo_path = home_dir / "InkyPi"
            source_root = repo_path / "src"
            install_path = tmpdir_path / "usr" / "local" / "inkypi"

            (repo_path / ".git").mkdir(parents=True)
            source_root.mkdir(parents=True)
            install_path.mkdir(parents=True)
            (install_path / "src").symlink_to(source_root, target_is_directory=True)

            layout = resolve_inkypi_layout(
                str(repo_path),
                str(install_path),
                home_dir=home_dir,
                cwd=tmpdir_path,
            )

            self.assertEqual(layout.repo_path, repo_path.resolve())
            self.assertEqual(layout.install_path, install_path.resolve())
            self.assertEqual(layout.source_root, source_root.resolve())
            self.assertEqual(layout.git_sync_path, repo_path.resolve())
            self.assertEqual(layout.source_origin, "install_path")
            self.assertTrue(layout.install_src_exists)

    def test_replaces_stale_opt_repo_with_home_checkout_when_runtime_install_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            home_dir = tmpdir_path / "home" / "inky"
            repo_path = home_dir / "InkyPi"
            (repo_path / ".git").mkdir(parents=True)
            (repo_path / "src").mkdir(parents=True)

            layout = resolve_inkypi_layout(
                "/opt/InkyPi",
                str(tmpdir_path / "usr" / "local" / "inkypi"),
                home_dir=home_dir,
                cwd=tmpdir_path,
            )

            self.assertEqual(layout.repo_path, repo_path.resolve())
            self.assertEqual(layout.source_root, (repo_path / "src").resolve())
            self.assertEqual(layout.git_sync_path, repo_path.resolve())
            self.assertEqual(layout.source_origin, "repo_path")
            self.assertTrue(layout.replaced_stale_repo_path)
            self.assertFalse(layout.install_src_exists)

    def test_falls_back_to_planned_clone_when_no_runtime_install_or_checkout_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            home_dir = tmpdir_path / "home" / "inky"

            layout = resolve_inkypi_layout(
                None,
                None,
                home_dir=home_dir,
                cwd=tmpdir_path,
            )

            self.assertEqual(layout.repo_path, (home_dir / "InkyPi").resolve())
            self.assertEqual(layout.install_path, Path("/usr/local/inkypi"))
            self.assertEqual(layout.source_root, (home_dir / "InkyPi" / "src").resolve(strict=False))
            self.assertIsNone(layout.git_sync_path)
            self.assertEqual(layout.source_origin, "planned_clone")
            self.assertFalse(layout.install_src_exists)


if __name__ == "__main__":
    unittest.main()
