from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.request import urlopen

from paper_agent.playwright_download import default_chrome_executable_path
from paper_agent.runtime import log_event
from paper_agent.utils import write_json


DEFAULT_CHROME_USER_DATA_DIR_ENV = "PAPER_AGENT_CHROME_SOURCE_USER_DATA_DIR"
DEFAULT_CHROME_PROFILE_DIRECTORY_ENV = "PAPER_AGENT_PLAYWRIGHT_PROFILE_DIRECTORY"
DEFAULT_CHROME_CDP_URL_ENV = "PAPER_AGENT_PLAYWRIGHT_CDP_URL"
DEFAULT_CHROME_CDP_PORT_ENV = "PAPER_AGENT_CHROME_CDP_PORT"
DEFAULT_CHROME_CDP_SESSION_FILE_ENV = "PAPER_AGENT_CHROME_CDP_SESSION_FILE"

CHROME_CACHE_DIR_NAMES = {
    "Cache",
    "Code Cache",
    "GPUCache",
    "GrShaderCache",
    "GraphiteDawnCache",
}
CHROME_EPHEMERAL_DIR_NAMES = {
    "Crashpad",
}
CHROME_SERVICE_WORKER_SKIP_DIR_NAMES = {
    "CacheStorage",
    "ScriptCache",
}


@dataclass
class ChromeCDPSession:
    cdp_url: str
    browser_executable_path: str
    source_user_data_dir: str
    clone_user_data_dir: str
    profile_directory: str
    remote_debugging_port: int
    pid: int
    process_group_id: int
    log_path: str
    session_file: str
    launched_at: str
    version_payload: dict[str, Any]


def default_chrome_user_data_dir() -> Path | None:
    candidates = [
        Path.home() / "Library/Application Support/Google/Chrome",
        Path.home() / ".config/google-chrome",
        Path.home() / ".config/chromium",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def parse_int_env(raw_value: str | None, default: int) -> int:
    if raw_value is None or not raw_value.strip():
        return default
    try:
        return int(raw_value.strip())
    except ValueError:
        return default


def default_cdp_port() -> int:
    return parse_int_env(os.getenv(DEFAULT_CHROME_CDP_PORT_ENV), default=9222)


def chrome_copy_ignore(dir_path: str, names: list[str]) -> set[str]:
    ignored: set[str] = set()
    path = Path(dir_path)
    for name in names:
        if name.startswith("Singleton"):
            ignored.add(name)
            continue
        if path.name == "Service Worker" and name in CHROME_SERVICE_WORKER_SKIP_DIR_NAMES:
            ignored.add(name)
            continue
        if name in CHROME_CACHE_DIR_NAMES or name in CHROME_EPHEMERAL_DIR_NAMES:
            ignored.add(name)
    return ignored


def prepare_chrome_clone(
    *,
    source_user_data_dir: Path,
    profile_directory: str,
    clone_root: Path | None = None,
) -> Path:
    if clone_root is not None:
        clone_root = clone_root.expanduser().resolve()
        clone_root.mkdir(parents=True, exist_ok=True)
    session_root = Path(
        tempfile.mkdtemp(
            prefix="paper-agent-chrome-cdp-",
            dir=str(clone_root) if clone_root else None,
        )
    )
    clone_user_data_dir = session_root / "chrome-user-data"
    clone_user_data_dir.mkdir(parents=True, exist_ok=True)

    local_state = source_user_data_dir / "Local State"
    if local_state.exists():
        shutil.copy2(local_state, clone_user_data_dir / "Local State")

    source_profile_dir = source_user_data_dir / profile_directory
    if not source_profile_dir.exists():
        raise FileNotFoundError(f"Chrome profile directory does not exist: {source_profile_dir}")

    shutil.copytree(
        source_profile_dir,
        clone_user_data_dir / profile_directory,
        ignore=chrome_copy_ignore,
        dirs_exist_ok=True,
    )
    return clone_user_data_dir


def wait_for_cdp_ready(cdp_url: str, timeout_seconds: float = 30.0) -> dict[str, Any]:
    version_url = cdp_url.rstrip("/") + "/json/version"
    deadline = time.perf_counter() + max(1.0, timeout_seconds)
    last_error: Exception | None = None
    while time.perf_counter() < deadline:
        try:
            with urlopen(version_url, timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8"))
                if isinstance(payload, dict):
                    return payload
        except Exception as exc:  # pragma: no cover - exercised by retry loop behaviour
            last_error = exc
            time.sleep(1.0)
    if last_error is not None:
        raise RuntimeError(f"Chrome CDP endpoint did not become ready: {last_error}") from last_error
    raise RuntimeError("Chrome CDP endpoint did not become ready before timeout")


def launch_chrome_with_cdp(
    *,
    browser_executable_path: str,
    clone_user_data_dir: Path,
    profile_directory: str,
    remote_debugging_port: int,
    log_path: Path,
) -> subprocess.Popen[Any]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_handle = log_path.open("w", encoding="utf-8")
    args = [
        browser_executable_path,
        f"--remote-debugging-port={remote_debugging_port}",
        f"--user-data-dir={clone_user_data_dir}",
        f"--profile-directory={profile_directory}",
        "about:blank",
    ]
    process = subprocess.Popen(
        args,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        close_fds=True,
    )
    log_handle.close()
    return process


def session_root_from_clone_user_data_dir(clone_user_data_dir: str | Path) -> Path:
    return Path(clone_user_data_dir).expanduser().resolve().parent


def cleanup_session_artifacts(session: ChromeCDPSession) -> bool:
    session_root = session_root_from_clone_user_data_dir(session.clone_user_data_dir)
    if not session_root.name.startswith("paper-agent-chrome-cdp-"):
        log_event(
            "warning",
            "Skipping Chrome CDP session cleanup because the session root does not match the expected prefix",
            session_root=session_root,
        )
        return False
    if not session_root.exists():
        return True
    shutil.rmtree(session_root, ignore_errors=True)
    removed = not session_root.exists()
    log_event(
        "info",
        "Chrome CDP session artifacts cleanup finished",
        session_root=session_root,
        removed=removed,
    )
    return removed


def prepare_cdp_session(
    *,
    browser_executable_path: str,
    source_user_data_dir: Path,
    profile_directory: str,
    remote_debugging_port: int,
    clone_root: Path | None = None,
    ready_timeout_seconds: float = 30.0,
) -> ChromeCDPSession:
    clone_user_data_dir = prepare_chrome_clone(
        source_user_data_dir=source_user_data_dir,
        profile_directory=profile_directory,
        clone_root=clone_root,
    )
    log_path = clone_user_data_dir.parent / "chrome.log"
    session_file = clone_user_data_dir.parent / "session.json"
    cdp_url = f"http://127.0.0.1:{remote_debugging_port}"

    log_event(
        "info",
        "Preparing Chrome CDP session",
        browser_executable_path=browser_executable_path,
        source_user_data_dir=source_user_data_dir,
        clone_user_data_dir=clone_user_data_dir,
        profile_directory=profile_directory,
        remote_debugging_port=remote_debugging_port,
    )
    process = launch_chrome_with_cdp(
        browser_executable_path=browser_executable_path,
        clone_user_data_dir=clone_user_data_dir,
        profile_directory=profile_directory,
        remote_debugging_port=remote_debugging_port,
        log_path=log_path,
    )

    try:
        version_payload = wait_for_cdp_ready(cdp_url, timeout_seconds=ready_timeout_seconds)
    except Exception:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except Exception:
            pass
        shutil.rmtree(clone_user_data_dir.parent, ignore_errors=True)
        raise

    session = ChromeCDPSession(
        cdp_url=cdp_url,
        browser_executable_path=browser_executable_path,
        source_user_data_dir=str(source_user_data_dir),
        clone_user_data_dir=str(clone_user_data_dir),
        profile_directory=profile_directory,
        remote_debugging_port=remote_debugging_port,
        pid=process.pid,
        process_group_id=process.pid,
        log_path=str(log_path),
        session_file=str(session_file),
        launched_at=time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        version_payload=version_payload,
    )
    write_json(session_file, asdict(session))
    log_event(
        "info",
        "Chrome CDP session ready",
        cdp_url=session.cdp_url,
        pid=session.pid,
        clone_user_data_dir=session.clone_user_data_dir,
        session_file=session.session_file,
    )
    return session


def load_session(session_file: Path) -> ChromeCDPSession:
    payload = json.loads(session_file.read_text(encoding="utf-8"))
    return ChromeCDPSession(**payload)


def stop_cdp_session(session: ChromeCDPSession, timeout_seconds: float = 10.0) -> bool:
    try:
        os.killpg(session.process_group_id, signal.SIGTERM)
    except ProcessLookupError:
        return True

    deadline = time.perf_counter() + max(1.0, timeout_seconds)
    while time.perf_counter() < deadline:
        try:
            os.kill(session.pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.5)

    try:
        os.killpg(session.process_group_id, signal.SIGKILL)
    except ProcessLookupError:
        return True
    return False


def render_shell_exports(session: ChromeCDPSession) -> str:
    lines = [
        f"export {DEFAULT_CHROME_CDP_URL_ENV}={shlex_quote(session.cdp_url)}",
        f"export {DEFAULT_CHROME_CDP_SESSION_FILE_ENV}={shlex_quote(session.session_file)}",
    ]
    return "\n".join(lines)


def shlex_quote(value: str) -> str:
    escaped = value.replace("'", "'\"'\"'")
    return f"'{escaped}'"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare and manage a reusable Chrome CDP session for ACM Playwright downloads.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare", help="Clone a Chrome profile and launch a CDP-enabled browser session.")
    prepare.add_argument("--browser-executable", help="Chrome executable path. Defaults to the detected local Chrome binary.")
    prepare.add_argument(
        "--source-user-data-dir",
        help=f"Chrome user-data root to clone from. Defaults to env {DEFAULT_CHROME_USER_DATA_DIR_ENV} or the local Chrome profile root.",
    )
    prepare.add_argument(
        "--profile-directory",
        default=os.getenv(DEFAULT_CHROME_PROFILE_DIRECTORY_ENV) or "Default",
        help=f"Chrome profile directory to clone. Defaults to env {DEFAULT_CHROME_PROFILE_DIRECTORY_ENV} or Default.",
    )
    prepare.add_argument("--remote-debugging-port", type=int, default=default_cdp_port(), help="Local CDP port to expose.")
    prepare.add_argument("--clone-root", help="Optional parent directory for the temporary cloned Chrome profile.")
    prepare.add_argument("--ready-timeout-seconds", type=float, default=30.0, help="How long to wait for the CDP endpoint to become ready.")
    prepare.add_argument(
        "--output",
        choices=("json", "shell"),
        default="json",
        help="Whether to print the prepared session as JSON metadata or shell export lines.",
    )

    status = subparsers.add_parser("status", help="Check whether a CDP session is reachable.")
    status.add_argument("--cdp-url", help=f"CDP endpoint URL. Defaults to env {DEFAULT_CHROME_CDP_URL_ENV}.")
    status.add_argument("--session-file", help=f"Session metadata file. Defaults to env {DEFAULT_CHROME_CDP_SESSION_FILE_ENV}.")

    stop = subparsers.add_parser("stop", help="Stop a previously prepared CDP session.")
    stop.add_argument("--session-file", help=f"Session metadata file. Defaults to env {DEFAULT_CHROME_CDP_SESSION_FILE_ENV}.")
    stop.add_argument(
        "--cleanup-artifacts",
        action="store_true",
        help="Also remove the cloned Chrome profile directory and the generated session metadata/log files.",
    )
    return parser


def resolve_prepare_inputs(args: argparse.Namespace) -> tuple[str, Path, str, int, Path | None, float]:
    browser_executable_path = args.browser_executable or default_chrome_executable_path()
    if not browser_executable_path:
        raise RuntimeError("Unable to locate Google Chrome. Pass --browser-executable explicitly.")

    source_user_data_dir = (
        Path(args.source_user_data_dir).expanduser().resolve()
        if args.source_user_data_dir
        else (
            Path(os.getenv(DEFAULT_CHROME_USER_DATA_DIR_ENV)).expanduser().resolve()
            if os.getenv(DEFAULT_CHROME_USER_DATA_DIR_ENV)
            else default_chrome_user_data_dir()
        )
    )
    if source_user_data_dir is None:
        raise RuntimeError("Unable to locate a Chrome user-data directory. Pass --source-user-data-dir explicitly.")

    clone_root = Path(args.clone_root).expanduser().resolve() if args.clone_root else None
    return (
        browser_executable_path,
        source_user_data_dir,
        args.profile_directory,
        int(args.remote_debugging_port),
        clone_root,
        float(args.ready_timeout_seconds),
    )


def command_prepare(args: argparse.Namespace) -> int:
    (
        browser_executable_path,
        source_user_data_dir,
        profile_directory,
        remote_debugging_port,
        clone_root,
        ready_timeout_seconds,
    ) = resolve_prepare_inputs(args)
    session = prepare_cdp_session(
        browser_executable_path=browser_executable_path,
        source_user_data_dir=source_user_data_dir,
        profile_directory=profile_directory,
        remote_debugging_port=remote_debugging_port,
        clone_root=clone_root,
        ready_timeout_seconds=ready_timeout_seconds,
    )
    if args.output == "shell":
        print(render_shell_exports(session))
    else:
        print(json.dumps(asdict(session), indent=2, ensure_ascii=False))
    return 0


def command_status(args: argparse.Namespace) -> int:
    session_file = args.session_file or os.getenv(DEFAULT_CHROME_CDP_SESSION_FILE_ENV)
    cdp_url = args.cdp_url or os.getenv(DEFAULT_CHROME_CDP_URL_ENV)
    session: ChromeCDPSession | None = None
    if session_file:
        session = load_session(Path(session_file))
        cdp_url = cdp_url or session.cdp_url
    if not cdp_url:
        raise RuntimeError("Provide --cdp-url or --session-file.")

    try:
        version_payload = wait_for_cdp_ready(cdp_url, timeout_seconds=2.0)
        payload: dict[str, Any] = {
            "reachable": True,
            "cdp_url": cdp_url,
            "version_payload": version_payload,
        }
        exit_code = 0
    except Exception as exc:
        payload = {
            "reachable": False,
            "cdp_url": cdp_url,
            "error": str(exc),
        }
        exit_code = 1
    if session is not None:
        payload["session_file"] = session.session_file
        payload["pid"] = session.pid
        payload["clone_user_data_dir"] = session.clone_user_data_dir
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return exit_code


def command_stop(args: argparse.Namespace) -> int:
    session_file = args.session_file or os.getenv(DEFAULT_CHROME_CDP_SESSION_FILE_ENV)
    if not session_file:
        raise RuntimeError("Provide --session-file or set PAPER_AGENT_CHROME_CDP_SESSION_FILE.")
    session = load_session(Path(session_file))
    stopped = stop_cdp_session(session)
    payload = {
        "stopped": stopped,
        "session_file": session.session_file,
        "pid": session.pid,
        "process_group_id": session.process_group_id,
    }
    if args.cleanup_artifacts:
        payload["artifacts_removed"] = cleanup_session_artifacts(session)
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "prepare":
        return command_prepare(args)
    if args.command == "status":
        return command_status(args)
    if args.command == "stop":
        return command_stop(args)
    parser.error(f"Unsupported command: {args.command}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
