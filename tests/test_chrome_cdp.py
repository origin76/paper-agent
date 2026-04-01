from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from paper_agent.chrome_cdp import ChromeCDPSession
from paper_agent.chrome_cdp import cleanup_session_artifacts
from paper_agent.chrome_cdp import chrome_copy_ignore
from paper_agent.chrome_cdp import prepare_chrome_clone
from paper_agent.chrome_cdp import render_shell_exports


class ChromeCDPTests(unittest.TestCase):
    def test_render_shell_exports_includes_reusable_env_vars(self) -> None:
        session = ChromeCDPSession(
            cdp_url="http://127.0.0.1:9222",
            browser_executable_path="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            source_user_data_dir="/Users/example/Library/Application Support/Google/Chrome",
            clone_user_data_dir="/tmp/paper-agent-chrome-cdp-123/chrome-user-data",
            profile_directory="Default",
            remote_debugging_port=9222,
            pid=12345,
            process_group_id=12345,
            log_path="/tmp/paper-agent-chrome-cdp-123/chrome.log",
            session_file="/tmp/paper-agent-chrome-cdp-123/session.json",
            launched_at="2026-03-31T13:40:00+0800",
            version_payload={"Browser": "Chrome/146.0.7680.165"},
        )
        rendered = render_shell_exports(session)
        self.assertIn("PAPER_AGENT_PLAYWRIGHT_CDP_URL", rendered)
        self.assertIn("PAPER_AGENT_CHROME_CDP_SESSION_FILE", rendered)
        self.assertIn("http://127.0.0.1:9222", rendered)

    def test_chrome_copy_ignore_skips_cache_and_singleton_files(self) -> None:
        ignored = chrome_copy_ignore(
            "/tmp/Profile/Service Worker",
            ["CacheStorage", "ScriptCache", "SingletonLock", "Preferences"],
        )
        self.assertIn("CacheStorage", ignored)
        self.assertIn("ScriptCache", ignored)
        self.assertIn("SingletonLock", ignored)
        self.assertNotIn("Preferences", ignored)

    def test_prepare_chrome_clone_copies_local_state_and_profile_without_cache_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source_root = Path(tmp_dir) / "source"
            profile_dir = source_root / "Default"
            profile_dir.mkdir(parents=True)
            (source_root / "Local State").write_text(json.dumps({"profile": "ok"}), encoding="utf-8")
            (profile_dir / "Preferences").write_text("prefs", encoding="utf-8")
            (profile_dir / "Cache").mkdir()
            (profile_dir / "Cache" / "cache.bin").write_text("cache", encoding="utf-8")
            (profile_dir / "SingletonLock").write_text("lock", encoding="utf-8")

            clone_dir = prepare_chrome_clone(
                source_user_data_dir=source_root,
                profile_directory="Default",
            )

            self.assertTrue((clone_dir / "Local State").exists())
            self.assertTrue((clone_dir / "Default" / "Preferences").exists())
            self.assertFalse((clone_dir / "Default" / "Cache").exists())
            self.assertFalse((clone_dir / "Default" / "SingletonLock").exists())

    def test_cleanup_session_artifacts_removes_expected_session_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            session_root = Path(tmp_dir) / "paper-agent-chrome-cdp-123"
            clone_user_data_dir = session_root / "chrome-user-data"
            clone_user_data_dir.mkdir(parents=True)
            (session_root / "session.json").write_text("{}", encoding="utf-8")

            session = ChromeCDPSession(
                cdp_url="http://127.0.0.1:9222",
                browser_executable_path="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                source_user_data_dir="/Users/example/Library/Application Support/Google/Chrome",
                clone_user_data_dir=str(clone_user_data_dir),
                profile_directory="Default",
                remote_debugging_port=9222,
                pid=12345,
                process_group_id=12345,
                log_path=str(session_root / "chrome.log"),
                session_file=str(session_root / "session.json"),
                launched_at="2026-03-31T13:40:00+0800",
                version_payload={"Browser": "Chrome/146.0.7680.165"},
            )

            self.assertTrue(cleanup_session_artifacts(session))
            self.assertFalse(session_root.exists())


if __name__ == "__main__":
    unittest.main()
