"""Run a pinned wheel probe, preserving native failure evidence without retrying.

CI configures core_pattern to compat-crash-artifacts/core.%p. Local runs never
change host settings. The child runs outside a debugger to preserve race timing.
"""

import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import subprocess
import sys


def main() -> int:
    interpreter, script = sys.argv[1:]
    artifacts = Path(os.environ.get("COMPAT_CRASH_ARTIFACTS", "compat-crash-artifacts"))
    artifacts.mkdir(parents=True, exist_ok=True)
    version = os.environ.get("COMPAT_VERSION", "unknown")
    metadata = subprocess.check_output(
        [interpreter, "-c", "import importlib.metadata,json,platform,sys; "
         "d=importlib.metadata.distribution('awa-pg'); "
         "print(json.dumps({'python':sys.version,'executable':sys.executable,"
         "'platform':platform.platform(),'wheel_version':d.version,"
         "'native_files':[str(d.locate_file(f)) for f in d.files if str(f).endswith('.so')]}))"],
        text=True,
    )
    manifest = json.loads(metadata)
    manifest["native_sha256"] = {
        name: hashlib.sha256(Path(name).read_bytes()).hexdigest()
        for name in manifest["native_files"]
    }
    _, hard = resource.getrlimit(resource.RLIMIT_CORE)
    resource.setrlimit(resource.RLIMIT_CORE, (hard, hard))
    child = subprocess.Popen([interpreter, script])
    code = child.wait()
    manifest.update(pid=child.pid, returncode=code, host=platform.platform())
    (artifacts / f"probe-{version}-{child.pid}.json").write_text(json.dumps(manifest, indent=2) + "\n")
    if code < 0:
        print(f"native probe died from signal {-code}; pid={child.pid}", file=sys.stderr)
        core = artifacts / f"core.{child.pid}"
        if core.exists():
            with (artifacts / f"backtrace-{child.pid}.txt").open("w") as output:
                subprocess.run(
                    ["gdb", "--batch", "-ex", "set pagination off", "-ex", "info sharedlibrary",
                     "-ex", "thread apply all bt full", str(Path(interpreter).resolve()), str(core)],
                    stdout=output, stderr=subprocess.STDOUT, check=False,
                )
    return 128 - code if code < 0 else code


if __name__ == "__main__":
    raise SystemExit(main())
