"""Start and monitor the single shared Cloak browser and its restricted CDP relay."""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import time
from typing import Any

from policy import (TABLE, parse_controllers, private_ipv4, readback_comment_matches,
                    readback_has_drop_hooks, render_policy, stable_fingerprint)

STATE_DIR = Path("/run/news-browser")
STATE_FILE = STATE_DIR / "runtime.json"
RELAY_HEARTBEAT_DIR = Path("/run/news-browser-relay")
RELAY_HEARTBEAT_FILE = RELAY_HEARTBEAT_DIR / "heartbeat.json"
INFO_DIR = Path("/tmp/news-browser-info")
CONTROL_DIR = Path("/browser-control")
MARKER_NAME = "restart.request"
POLICY_POLL_SECONDS = 5
STARTUP_TIMEOUT_SECONDS = 90
STOP_GRACE_SECONDS = 10
RELAY_HEARTBEAT_MAX_AGE_SECONDS = 10
RELAY_UID = 65534
RELAY_HEARTBEAT_DIRECTORY_UID = 0
SET_PRIV = "/usr/bin/setpriv"
NFT = "/usr/sbin/nft"
IP = "/usr/sbin/ip"


class StartupError(RuntimeError):
    """A safe finite startup/health failure."""


def _run(args: list[str], *, input_text: str | None = None, check: bool = True,
         timeout: float = 10, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(args, input=input_text, text=True, capture_output=True,
                                timeout=timeout, check=False, env=env)
    except (OSError, subprocess.TimeoutExpired):
        raise StartupError("required_runtime_command_failed") from None
    if check and result.returncode:
        raise StartupError("required_runtime_command_failed")
    return result


def _runtime_env() -> tuple[str, dict[str, str], str, int]:
    self_ip = private_ipv4(os.environ.get("CLOAK_SELF_IP"), "CLOAK_SELF_IP")
    controllers = parse_controllers(os.environ.get("CLOAK_CONTROLLER_IPS", ""))
    proxy_ip = private_ipv4(os.environ.get("CLOAK_PROXY_IP"), "CLOAK_PROXY_IP")
    try:
        threshold = int(os.environ.get("CHROME_HEALTHCHECK_FD_THRESHOLD", "800"))
    except ValueError:
        raise StartupError("invalid_fd_threshold") from None
    if not 1 <= threshold <= 100000:
        raise StartupError("invalid_fd_threshold")
    return self_ip, controllers, proxy_ip, threshold


def _check_single_interface(expected: str, *, timeout: float = 10) -> None:
    result = _run([IP, "-j", "-4", "address", "show"], timeout=timeout)
    try:
        interfaces = json.loads(result.stdout)
    except json.JSONDecodeError:
        raise StartupError("interface_inventory_invalid") from None
    addresses = []
    for interface in interfaces:
        if interface.get("ifname") == "lo":
            continue
        for item in interface.get("addr_info", []):
            if item.get("family") == "inet":
                try:
                    addresses.append(str(ipaddress.IPv4Address(item.get("local"))))
                except (ipaddress.AddressValueError, TypeError):
                    raise StartupError("interface_inventory_invalid") from None
    if addresses != [expected]:
        raise StartupError("single_expected_ipv4_interface_required")


def _policy_text(self_ip: str, controllers: dict[str, str], proxy_ip: str) -> str:
    return render_policy(self_ip, controllers, proxy_ip)


def _install_policy(policy_text: str) -> tuple[str, dict[str, Any]]:
    tables_result = _run([NFT, "-j", "list", "tables"])
    try:
        inventory = json.loads(tables_result.stdout)
    except json.JSONDecodeError:
        raise StartupError("nft_table_inventory_invalid") from None
    existing = [item["table"] for item in inventory.get("nftables", [])
                if isinstance(item, dict) and isinstance(item.get("table"), dict)
                and item["table"].get("family") == "inet" and item["table"].get("name") == TABLE]
    if len(existing) > 1:
        raise StartupError("nft_owned_table_inventory_invalid")
    # Keep delete plus recreate in one nft batch; older releases lack `destroy`.
    delete = f"delete table inet {TABLE}\n" if existing else ""
    replacement = delete + policy_text
    _run([NFT, "-f", "-"], input_text=replacement)
    value, text = _read_policy()
    expected = re.search(r'comment "(policy-sha256:[0-9a-f]{64})"', policy_text)
    if (expected is None or not readback_has_drop_hooks(value)
            or not readback_comment_matches(value, text, expected.group(1))):
        raise StartupError("nft_policy_readback_invalid")
    return stable_fingerprint(value), value


def _read_policy(*, timeout: float = 10) -> tuple[dict[str, Any], str]:
    value_result = _run([NFT, "-j", "list", "table", "inet", TABLE], timeout=timeout)
    text_result = _run([NFT, "list", "table", "inet", TABLE], timeout=timeout)
    try:
        value = json.loads(value_result.stdout)
    except json.JSONDecodeError:
        raise StartupError("nft_policy_readback_invalid") from None
    return value, text_result.stdout


def _verify_policy(expected_fingerprint: str, policy_text: str, *, command_timeout: float = 10) -> None:
    value, text = _read_policy(timeout=command_timeout)
    expected = re.search(r'comment "(policy-sha256:[0-9a-f]{64})"', policy_text)
    if (expected is None or not readback_has_drop_hooks(value)
            or not readback_comment_matches(value, text, expected.group(1))
            or stable_fingerprint(value) != expected_fingerprint):
        raise StartupError("nft_policy_changed")


def _clear_stale_launch_info() -> None:
    if INFO_DIR.is_symlink():
        raise StartupError("launch_info_directory_invalid")
    INFO_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = INFO_DIR.stat(follow_symlinks=False)
    if not INFO_DIR.is_dir() or info.st_uid != 0 or info.st_mode & 0o022:
        raise StartupError("launch_info_directory_invalid")
    for name in ("launch-info.json", "launch-info.json.tmp"):
        path = INFO_DIR / name
        try:
            item = path.stat(follow_symlinks=False)
        except FileNotFoundError:
            continue
        if (path.is_symlink() or not path.is_file() or item.st_uid != 0
                or item.st_mode & 0o077):
            raise StartupError("launch_info_file_invalid")
        path.unlink()


def _drop_privilege_args(uid: int, gid: int) -> list[str]:
    return [SET_PRIV, "--bounding-set=-all", "--inh-caps=-all", "--ambient-caps=-all",
            "--securebits=+noroot", "--no-new-privs", f"--reuid={uid}", f"--regid={gid}",
            "--clear-groups"]


def _child_environment(*, relay: bool = False) -> dict[str, str]:
    environment = dict(os.environ)
    environment["CLOAKBROWSER_AUTO_UPDATE"] = "false"
    environment["CLOAKBROWSER_FETCH_WIDEVINE"] = "false"
    environment["DISPLAY"] = "unix/:99"
    environment["PATH"] = "/shadow-bin:" + environment.get("PATH", "/usr/local/bin:/usr/bin:/bin")
    if relay:
        environment.pop("DISPLAY", None)
    return environment


def _start_children(controllers: dict[str, str]) -> tuple[subprocess.Popen[str], subprocess.Popen[str]]:
    chrome_command = _drop_privilege_args(0, 0) + ["--", "/entrypoint.sh", "python",
                        "/opt/news-browser/launch.py", "--info-dir", str(INFO_DIR)]
    relay_command = _drop_privilege_args(65534, 65534) + ["--", sys.executable,
                       "/opt/news-browser/relay.py", "--persistent", "--heartbeat-path",
                       str(RELAY_HEARTBEAT_FILE)]
    for address in controllers.values():
        relay_command += ["--controller-ip", address]
    try:
        chrome = subprocess.Popen(chrome_command, env=_child_environment(), start_new_session=True,
                                  stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                  stderr=subprocess.DEVNULL, text=True)
    except OSError:
        raise StartupError("cloak_start_failed") from None
    try:
        relay = subprocess.Popen(relay_command, env=_child_environment(relay=True), start_new_session=True,
                                 stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL, text=True)
    except OSError:
        _terminate(chrome)
        raise StartupError("relay_start_failed") from None
    return chrome, relay


def _terminate(process: subprocess.Popen[str] | None) -> None:
    if process is None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    if process.poll() is None:
        try:
            process.wait(timeout=STOP_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            pass
    # The process leader can exit before Xvfb or another group member.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    if process.poll() is None:
        process.wait(timeout=3)


def _process_identity(pid: int, expected_uid: int) -> bool:
    try:
        status = Path(f"/proc/{pid}/status").read_text()
    except OSError:
        return False
    fields = dict(line.split(":", 1) for line in status.splitlines() if ":" in line)
    try:
        uid = int(fields["Uid"].split()[0])
        capabilities = [int(fields[key].strip(), 16) for key in ("CapEff", "CapPrm", "CapBnd")]
    except (KeyError, ValueError, IndexError):
        return False
    return uid == expected_uid and capabilities == [0, 0, 0]


def _read_capability_sets(pid: int) -> tuple[int, int, int]:
    try:
        status = Path(f"/proc/{pid}/status").read_text()
    except OSError:
        raise StartupError("runtime_capabilities_unavailable") from None
    fields = dict(line.split(":", 1) for line in status.splitlines() if ":" in line)
    try:
        return tuple(int(fields[key].strip(), 16) for key in ("CapEff", "CapPrm", "CapBnd"))
    except (KeyError, ValueError):
        raise StartupError("runtime_capabilities_unavailable") from None


def _assert_runtime_capabilities() -> None:
    effective, permitted, bounding = _read_capability_sets(os.getpid())
    net_admin = 1 << 12
    required = (1 << 5) | (1 << 6) | (1 << 7) | (1 << 8)  # KILL, SETGID, SETUID, SETPCAP
    if any(mask & net_admin for mask in (effective, permitted, bounding)):
        raise StartupError("runtime_retained_net_admin")
    if (effective & required) != required or (permitted & required) != required:
        raise StartupError("runtime_lifecycle_capabilities_missing")


def _process_commandline(pid: int) -> list[str]:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return []
    return [part.decode(errors="replace") for part in raw.split(b"\0") if part]


def _process_children(pid: int) -> set[int]:
    try:
        raw = Path(f"/proc/{pid}/task/{pid}/children").read_text()
        return {int(value) for value in raw.split()}
    except (OSError, ValueError):
        return set()


def _child_commands_match(chrome_pid: int, relay_pid: int) -> bool:
    chrome = _process_commandline(chrome_pid)
    relay = _process_commandline(relay_pid)
    return (any(arg == "--remote-debugging-port=5100" for arg in chrome)
            and any(arg.startswith("--user-data-dir=") for arg in chrome)
            and "/opt/news-browser/relay.py" in relay and "--persistent" in relay)


def _children_ready(chrome_pid: int, relay_pid: int) -> bool:
    return (_process_identity(chrome_pid, 0) and _process_identity(relay_pid, 65534)
            and _child_commands_match(chrome_pid, relay_pid)
            and _listener_ready(relay_pid) and _relay_heartbeat_fresh(relay_pid)
            and _probe_cdp_as_relay_user())


def _fd_count(pid: int) -> int:
    try:
        return len(list(Path(f"/proc/{pid}/fd").iterdir()))
    except OSError:
        raise StartupError("chrome_fd_inventory_unavailable") from None


def _listener_ready(pid: int) -> bool:
    try:
        rows = Path(f"/proc/{pid}/net/tcp").read_text().splitlines()[1:]
    except OSError:
        return False
    for row in rows:
        fields = row.split()
        if len(fields) >= 8 and fields[1] == "00000000:2406" and fields[3] == "0A" and fields[7] == "65534":
            return True
    return False


def _probe_cdp_as_relay_user() -> bool:
    script = (
        "import json,sys,urllib.request; "
        "opener=urllib.request.build_opener(urllib.request.ProxyHandler({})); "
        "v=json.load(opener.open('http://127.0.0.1:5100/json/version',timeout=2)); "
        "sys.exit(0 if isinstance(v,dict) and isinstance(v.get('webSocketDebuggerUrl'),str) else 1)"
    )
    result = _run(_drop_privilege_args(65534, 65534) + ["--", sys.executable, "-c", script],
                  check=False, timeout=4, env=_child_environment(relay=True))
    return result.returncode == 0


def _prepare_relay_heartbeat_dir() -> None:
    if RELAY_HEARTBEAT_DIR.is_symlink():
        raise StartupError("relay_heartbeat_directory_invalid")
    RELAY_HEARTBEAT_DIR.mkdir(mode=0o733, exist_ok=True)
    try:
        RELAY_HEARTBEAT_DIR.chmod(0o733)
    except OSError:
        raise StartupError("relay_heartbeat_directory_invalid") from None
    directory = RELAY_HEARTBEAT_DIR.stat(follow_symlinks=False)
    if (not RELAY_HEARTBEAT_DIR.is_dir() or directory.st_uid != RELAY_HEARTBEAT_DIRECTORY_UID
            or stat.S_IMODE(directory.st_mode) != 0o733):
        raise StartupError("relay_heartbeat_directory_invalid")
    try:
        descriptor = os.open(RELAY_HEARTBEAT_DIR, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            try:
                fd = os.open(RELAY_HEARTBEAT_FILE.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                             dir_fd=descriptor)
            except FileNotFoundError:
                return
            except OSError:
                raise StartupError("relay_heartbeat_file_invalid") from None
            try:
                info = os.fstat(fd)
                if (not stat.S_ISREG(info.st_mode) or info.st_uid != RELAY_UID
                        or info.st_mode & 0o022 or info.st_size > 256):
                    raise StartupError("relay_heartbeat_file_invalid")
            finally:
                os.close(fd)
            os.unlink(RELAY_HEARTBEAT_FILE.name, dir_fd=descriptor)
        finally:
            os.close(descriptor)
    except OSError:
        raise StartupError("relay_heartbeat_directory_invalid") from None


def _relay_heartbeat_fresh(expected_pid: int) -> bool:
    if RELAY_HEARTBEAT_DIR.is_symlink() or RELAY_HEARTBEAT_FILE.is_symlink():
        return False
    try:
        directory = RELAY_HEARTBEAT_DIR.stat(follow_symlinks=False)
        if (not stat.S_ISDIR(directory.st_mode) or directory.st_uid != RELAY_HEARTBEAT_DIRECTORY_UID
                or stat.S_IMODE(directory.st_mode) != 0o733):
            return False
        descriptor = os.open(RELAY_HEARTBEAT_DIR, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            fd = os.open("heartbeat.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                         dir_fd=descriptor)
            try:
                info = os.fstat(fd)
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                        or info.st_uid != RELAY_UID or info.st_mode & 0o022
                        or info.st_size > 256):
                    return False
                payload = json.loads(os.read(fd, 257))
            finally:
                os.close(fd)
        finally:
            os.close(descriptor)
    except (OSError, json.JSONDecodeError):
        return False
    if (not isinstance(payload, dict) or type(payload.get("pid")) is not int
            or type(payload.get("monotonic_ns")) is not int or payload["pid"] != expected_pid):
        return False
    age = time.monotonic() - payload["monotonic_ns"] / 1_000_000_000
    return -1 <= age <= RELAY_HEARTBEAT_MAX_AGE_SECONDS


def _proc_starttime(pid: int) -> int:
    try:
        content = Path(f"/proc/{pid}/stat").read_text()
        end = content.rfind(")")
        fields = content[end + 2:].split()
        return int(fields[19])
    except (OSError, ValueError, IndexError):
        raise StartupError("supervisor_identity_unavailable") from None


def _state(chrome: subprocess.Popen[str], relay: subprocess.Popen[str], fingerprint: str) -> dict[str, Any]:
    return {"version": 1, "chrome_pid": chrome.pid, "relay_pid": relay.pid,
            "supervisor_pid": os.getpid(), "supervisor_starttime": _proc_starttime(os.getpid()),
            "policy_fingerprint": fingerprint}


def _write_state(value: dict[str, Any]) -> None:
    STATE_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = STATE_DIR / "runtime.json.tmp"
    final = STATE_FILE
    if STATE_DIR.is_symlink() or final.is_symlink() or temporary.is_symlink():
        raise StartupError("runtime_state_invalid")
    directory_info = STATE_DIR.stat(follow_symlinks=False)
    if (not STATE_DIR.is_dir() or directory_info.st_uid != 0
            or directory_info.st_mode & 0o066 or directory_info.st_mode & 0o011 != 0o011):
        raise StartupError("runtime_state_invalid")
    try:
        old_final = final.stat(follow_symlinks=False)
        if not final.is_file() or old_final.st_uid != 0 or old_final.st_mode & 0o077:
            raise StartupError("runtime_state_invalid")
    except FileNotFoundError:
        pass
    try:
        temporary.unlink()
    except FileNotFoundError:
        pass
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(value, stream, separators=(",", ":"))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, final)


def _read_state() -> dict[str, Any]:
    try:
        info = STATE_FILE.stat(follow_symlinks=False)
        if (STATE_FILE.is_symlink() or not STATE_FILE.is_file() or info.st_uid != 0
                or info.st_mode & 0o077 or info.st_size > 4096):
            raise StartupError("runtime_state_invalid")
        value = json.loads(STATE_FILE.read_text())
    except (OSError, json.JSONDecodeError):
        raise StartupError("runtime_state_unavailable") from None
    if (not isinstance(value, dict) or value.get("version") != 1
            or any(type(value.get(key)) is not int or value[key] <= 1
                   for key in ("chrome_pid", "relay_pid", "supervisor_pid"))
            or value["supervisor_pid"] == 1
            or type(value.get("supervisor_starttime")) is not int or value["supervisor_starttime"] <= 0
            or not isinstance(value.get("policy_fingerprint"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", value["policy_fingerprint"])):
        raise StartupError("runtime_state_invalid")
    return value


def _marker_present_and_consume() -> bool:
    if CONTROL_DIR.is_symlink() or not CONTROL_DIR.is_dir():
        return False
    try:
        directory_info = CONTROL_DIR.stat(follow_symlinks=False)
        if directory_info.st_uid != 0 or directory_info.st_mode & 0o022:
            raise StartupError("restart_marker_directory_invalid")
        directory = os.open(CONTROL_DIR, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError:
        return False
    try:
        try:
            fd = os.open(MARKER_NAME, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        except FileNotFoundError:
            return False
        except OSError:
            raise StartupError("restart_marker_invalid") from None
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022
                    or info.st_size > 4096):
                raise StartupError("restart_marker_invalid")
        finally:
            os.close(fd)
        os.unlink(MARKER_NAME, dir_fd=directory)
        return True
    finally:
        os.close(directory)


def _verify_runtime(policy_text: str, state: dict[str, Any], threshold: int,
                    *, verify_policy: bool = True) -> None:
    if verify_policy:
        _verify_policy(state["policy_fingerprint"], policy_text)
    chrome_pid, relay_pid = state["chrome_pid"], state["relay_pid"]
    if not _process_identity(chrome_pid, 0) or not _process_identity(relay_pid, 65534):
        raise StartupError("browser_child_identity_invalid")
    if not _child_commands_match(chrome_pid, relay_pid):
        raise StartupError("browser_child_command_invalid")
    if _fd_count(chrome_pid) >= threshold:
        raise StartupError("chrome_fd_threshold_exceeded")
    if (not _listener_ready(relay_pid) or not _relay_heartbeat_fresh(relay_pid)
            or not _probe_cdp_as_relay_user()):
        raise StartupError("browser_cdp_unavailable")


def _verified_supervisor_pid(state: dict[str, Any]) -> int:
    pid = state.get("supervisor_pid")
    if type(pid) is not int or pid <= 1 or pid == os.getpid():
        raise StartupError("supervisor_identity_invalid")
    if _proc_starttime(pid) != state.get("supervisor_starttime"):
        raise StartupError("supervisor_identity_invalid")
    try:
        status = Path(f"/proc/{pid}/status").read_text()
    except OSError:
        raise StartupError("supervisor_identity_invalid") from None
    fields = dict(line.split(":", 1) for line in status.splitlines() if ":" in line)
    command = _process_commandline(pid)
    try:
        uid = int(fields["Uid"].split()[0])
        masks = [int(fields[key].strip(), 16) for key in ("CapEff", "CapPrm", "CapBnd")]
    except (KeyError, ValueError, IndexError):
        raise StartupError("supervisor_identity_invalid") from None
    if (uid != 0 or any(mask & (1 << 12) for mask in masks)
            or str(Path(__file__).resolve()) not in command or "--runtime" not in command
            or not {state.get("chrome_pid"), state.get("relay_pid")}.issubset(_process_children(pid))):
        raise StartupError("supervisor_identity_invalid")
    return pid


def _request_supervisor_restart(state: dict[str, Any]) -> None:
    pid = _verified_supervisor_pid(state)
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        raise StartupError("supervisor_stop_race") from None


def healthcheck() -> int:
    self_ip, controllers, proxy_ip, threshold = _runtime_env()
    # Keep the policy-failure path well inside Compose's three-second health timeout,
    # so it can stop the verified runtime parent before Docker kills this exec.
    _check_single_interface(self_ip, timeout=0.5)
    policy_text = _policy_text(self_ip, controllers, proxy_ip)
    state = _read_state()
    expected_fingerprint = _expected_fingerprint()
    try:
        if state.get("policy_fingerprint") != expected_fingerprint:
            raise StartupError("policy_fingerprint_mismatch")
        _verify_policy(state["policy_fingerprint"], policy_text, command_timeout=0.5)
    except StartupError:
        _request_supervisor_restart(state)
        raise StartupError("nft_policy_readback_failed") from None
    _verify_runtime(policy_text, state, threshold, verify_policy=False)
    return 0


def supervise() -> int:
    if os.geteuid() != 0:
        raise StartupError("root_required")
    _assert_runtime_capabilities()
    self_ip, controllers, proxy_ip, threshold = _runtime_env()
    _check_single_interface(self_ip)
    policy_text = _policy_text(self_ip, controllers, proxy_ip)
    expected = re.search(r'comment "(policy-sha256:[0-9a-f]{64})"', policy_text)
    if expected is None:
        raise StartupError("nft_policy_render_invalid")
    fingerprint = _expected_fingerprint()
    _clear_stale_launch_info()
    _prepare_relay_heartbeat_dir()
    stopping = False

    def request_stop(_signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True

    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, request_stop)
    chrome, relay = _start_children(controllers)
    state = _state(chrome, relay, fingerprint)
    _write_state(state)
    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    ready = False
    try:
        while True:
            if stopping:
                return 0
            for role, process in (("cloak", chrome), ("relay", relay)):
                exit_code = process.poll()
                if exit_code is not None:
                    detail = (f"signal_{min(-exit_code, 255)}" if exit_code < 0
                              else f"code_{min(exit_code, 255)}")
                    raise StartupError(f"{role}_child_exited_{detail}")
            if _marker_present_and_consume():
                return 0
            if not ready:
                if _children_ready(chrome.pid, relay.pid):
                    ready = True
                elif time.monotonic() >= deadline:
                    raise StartupError("browser_startup_timeout")
            else:
                if not _process_identity(chrome.pid, 0) or not _process_identity(relay.pid, 65534):
                    raise StartupError("browser_child_identity_invalid")
                if not _child_commands_match(chrome.pid, relay.pid):
                    raise StartupError("browser_child_command_invalid")
                if _fd_count(chrome.pid) >= threshold:
                    raise StartupError("chrome_fd_threshold_exceeded")
                if not _listener_ready(relay.pid) or not _probe_cdp_as_relay_user():
                    raise StartupError("browser_cdp_unavailable")
                if not _relay_heartbeat_fresh(relay.pid):
                    raise StartupError("relay_heartbeat_stale")
            time.sleep(POLICY_POLL_SECONDS)
    finally:
        _terminate(relay)
        _terminate(chrome)
        STATE_FILE.unlink(missing_ok=True)


def bootstrap() -> int:
    if os.geteuid() != 0:
        raise StartupError("root_required")
    self_ip, controllers, proxy_ip, _threshold = _runtime_env()
    _check_single_interface(self_ip)
    policy_text = _policy_text(self_ip, controllers, proxy_ip)
    fingerprint, _ = _install_policy(policy_text)
    STATE_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    fingerprint_path = STATE_DIR / "expected-policy-fingerprint"
    if STATE_DIR.is_symlink() or fingerprint_path.is_symlink():
        raise StartupError("runtime_state_invalid")
    directory_info = STATE_DIR.stat(follow_symlinks=False)
    if not STATE_DIR.is_dir() or directory_info.st_uid != 0 or directory_info.st_mode & 0o077:
        raise StartupError("runtime_state_invalid")
    try:
        old_info = fingerprint_path.stat(follow_symlinks=False)
        if not fingerprint_path.is_file() or old_info.st_uid != 0 or old_info.st_mode & 0o077:
            raise StartupError("runtime_state_invalid")
    except FileNotFoundError:
        pass
    try:
        fingerprint_path.unlink()
    except FileNotFoundError:
        pass
    descriptor = os.open(fingerprint_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(fingerprint + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    runtime = [SET_PRIV, "--bounding-set=-net_admin", "--inh-caps=-all", "--ambient-caps=-all",
               "--", sys.executable, str(Path(__file__).resolve()), "--runtime"]
    try:
        os.execv(runtime[0], runtime)
    except OSError:
        raise StartupError("capability_drop_exec_failed") from None
    return 1


def _expected_fingerprint() -> str:
    path = STATE_DIR / "expected-policy-fingerprint"
    try:
        info = path.stat(follow_symlinks=False)
        if (path.is_symlink() or not path.is_file() or info.st_uid != 0
                or info.st_mode & 0o077 or info.st_size > 80):
            raise StartupError("policy_fingerprint_invalid")
        value = path.read_text().strip()
    except OSError:
        raise StartupError("policy_fingerprint_unavailable") from None
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise StartupError("policy_fingerprint_invalid")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--healthcheck", action="store_true")
    parser.add_argument("--runtime", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        if args.healthcheck:
            return healthcheck()
        return supervise() if args.runtime else bootstrap()
    except (StartupError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
