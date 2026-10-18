from pathlib import Path
import csv
from datetime import datetime, timezone
import time

root = Path("/tmp")
tag = root / "awa-521-524-host-tag"
stop = root / "awa-521-524-host-stop"


def pressure(name, kind):
    for line in Path(f"/proc/pressure/{name}").read_text().splitlines():
        fields = line.split()
        if fields[0] == kind:
            return float(dict(field.split("=") for field in fields[1:])["avg10"])
    return 0.0


with (root / "awa-521-524-host-load.csv").open("w", newline="") as output:
    writer = csv.writer(output)
    writer.writerow(
        [
            "utc",
            "matrix",
            "load1",
            "load5",
            "load15",
            "cpu_some_avg10_pct",
            "io_some_avg10_pct",
            "io_full_avg10_pct",
            "memory_full_avg10_pct",
        ]
    )
    while not stop.exists():
        load = Path("/proc/loadavg").read_text().split()[:3]
        writer.writerow(
            [
                datetime.now(timezone.utc).isoformat(),
                tag.read_text().strip(),
                *load,
                pressure("cpu", "some"),
                pressure("io", "some"),
                pressure("io", "full"),
                pressure("memory", "full"),
            ]
        )
        output.flush()
        time.sleep(5)
