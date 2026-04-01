from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from paper_agent.config import _iter_local_env_files


class ConfigEnvLoadingTests(unittest.TestCase):
    def test_iter_local_env_files_excludes_env_example_templates(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            cwd = root / "cwd"
            project_root = root / "project"
            cwd.mkdir(parents=True, exist_ok=True)
            project_root.mkdir(parents=True, exist_ok=True)

            cwd_env = cwd / ".env"
            project_env = project_root / ".env"
            cwd_env.write_text("A=1\n", encoding="utf-8")
            project_env.write_text("B=1\n", encoding="utf-8")
            (cwd / ".env.example").write_text("A=example\n", encoding="utf-8")
            (project_root / ".env.example").write_text("B=example\n", encoding="utf-8")

            self.assertEqual(
                _iter_local_env_files(cwd=cwd, project_root=project_root),
                (cwd_env.resolve(), project_env.resolve()),
            )


if __name__ == "__main__":
    unittest.main()
