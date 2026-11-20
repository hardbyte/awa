import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

RAW_CAP = 512 * 1024 * 1024
LOG_CAP = 64 * 1024 * 1024
STOP = False


def request_stop(_signal, _frame):
    global STOP
    STOP = True


def cap_reason(raw_size, log_size):
    if raw_size >= RAW_CAP:
        return "raw_file_cap"
    if log_size >= LOG_CAP:
        return "collector_log_cap"
    return None


def main():
    directory = Path(os.environ["AWA_SOAK_METRICS_DIR"])
    if not directory.is_absolute():
        raise RuntimeError("metrics directory must be absolute")
    project = os.environ["AWA_SOAK_METRICS_PROJECT"]
    if not project.startswith("awa-gate-") or not all(c.isascii() and (c.islower() or c.isdigit() or c == "-") for c in project):
        raise RuntimeError("unique owned awa-gate-* project required")
    raw_directory = directory / "collector"
    raw_directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    os.environ["AWA_SOAK_METRICS_UID"] = str(os.getuid())
    os.environ["AWA_SOAK_METRICS_GID"] = str(os.getgid())
    raw = raw_directory / "metrics.jsonl"
    log_path = directory / "collector.log"
    receipt = directory / "collector-coverage.json"
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    command = ["docker", "compose", "--project-name", project, "--file", str(Path(__file__).with_name("compose.yaml")), "up", "--pull", "never", "--abort-on-container-exit"]
    started_ns = time.time_ns()
    reason = "collector_exit"
    raw_size = 0
    log_size = 0
    with log_path.open("xb") as log:
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            while child.poll() is None and not STOP:
                raw_size = raw.stat().st_size if raw.exists() else 0
                log_size = log_path.stat().st_size
                over_cap = cap_reason(raw_size, log_size)
                if over_cap:
                    reason = over_cap
                    break
                time.sleep(1)
            if STOP:
                reason = "stop_requested"
        finally:
            if child.poll() is None:
                child.send_signal(signal.SIGINT)
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
                    reason += ":compose_stop_timeout_container_state_unverified"
            raw_size = raw.stat().st_size if raw.exists() else 0
            log_size = log_path.stat().st_size
            over_cap = cap_reason(raw_size, log_size)
            if over_cap:
                reason = over_cap
            receipt.write_text(json.dumps({
                "project": project, "image": "otel/opentelemetry-collector-contrib:0.115.1",
                "image_digest": None, "image_digest_status": "must_be_recorded_before_gate",
                "started_ns": started_ns, "stopped_ns": time.time_ns(),
                "reason": reason, "compose_exit_code": child.returncode,
                "raw_bytes": raw_size, "raw_cap_bytes": RAW_CAP,
                "log_bytes": log_size, "log_cap_bytes": LOG_CAP,
                "coverage_complete": False, "coverage_status": "raw_transport_and_tail_validation_required",
                "cap_status": "exceeded" if over_cap else "not_observed_exceeded",
                "cap_overshoot": "one_poll_interval_plus_inflight_exports; no evidence rotated or deleted",
            }, indent=2) + "\n")
    return 1 if over_cap or reason != "stop_requested" else 0


if __name__ == "__main__":
    sys.exit(main())
