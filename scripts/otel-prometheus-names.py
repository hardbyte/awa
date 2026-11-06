#!/usr/bin/env python3
"""Print the Prometheus series and label names awa's OTel metrics export as.

Reads the instrument definitions in awa-metrics (kind, name and unit) and
applies the OpenTelemetry specification's OTLP-to-Prometheus translation,
the one the collector's Prometheus exporter performs with metric suffixes
on (its default):

- invalid characters, such as the dots in `awa.job.duration`, become `_`,
  and runs of `_` collapse to one;
- the unit is appended in its Prometheus form (`s` -> `seconds`,
  `By` -> `bytes`, `1` -> `ratio` on gauges), unless the name already ends
  with it; annotations such as `{job}` are not units and are dropped;
- monotonic counters end in `_total`; up-down counters export as gauges;
- histograms export `_bucket`, `_sum` and `_count` series;
- attribute keys become label names by the same character rules.

Spec: https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_openmetrics/

Usage: otel-prometheus-names.py [--metrics|--labels] [awa-root]
Prints one name per line; the default prints both sets.
"""

import pathlib
import re
import sys

# UCUM units with a Prometheus name, from the specification's mapping.
UNITS = {
    "d": "days",
    "h": "hours",
    "min": "minutes",
    "s": "seconds",
    "ms": "milliseconds",
    "us": "microseconds",
    "ns": "nanoseconds",
    "By": "bytes",
    "KiBy": "kibibytes",
    "MiBy": "mebibytes",
    "GiBy": "gibibytes",
    "TiBy": "tibibytes",
    "KBy": "kilobytes",
    "MBy": "megabytes",
    "GBy": "gigabytes",
    "TBy": "terabytes",
    "m": "meters",
    "V": "volts",
    "A": "amperes",
    "J": "joules",
    "W": "watts",
    "g": "grams",
    "Cel": "celsius",
    "Hz": "hertz",
    "%": "percent",
}
# The denominator of a rate unit such as `By/s`.
PER_UNITS = {
    "s": "second",
    "m": "minute",
    "h": "hour",
    "d": "day",
    "w": "week",
    "mo": "month",
    "y": "year",
}

KINDS = {
    "counter": "counter",
    "up_down_counter": "gauge",
    "gauge": "gauge",
    "histogram": "histogram",
}


def sanitize(name: str) -> str:
    name = re.sub(r"[^a-zA-Z0-9_:]", "_", name)
    name = re.sub(r"__+", "_", name)
    if name[:1].isdigit():
        name = "_" + name
    return name


def unit_suffix(unit: str, kind: str) -> str:
    unit = re.sub(r"\{[^}]*\}", "", unit).strip()
    if not unit:
        return ""
    if unit == "1":
        return "ratio" if kind == "gauge" else ""
    if "/" in unit:
        numerator, denominator = unit.split("/", 1)
        per = PER_UNITS.get(denominator, UNITS.get(denominator, denominator))
        if not numerator:
            return f"per_{per}"
        return f"{UNITS.get(numerator, numerator)}_per_{per}"
    return UNITS.get(unit, unit)


def series_names(name: str, unit: str, kind: str) -> list[str]:
    base = sanitize(name)
    suffix = sanitize(unit_suffix(unit, kind)).strip("_")
    if suffix and not base.endswith("_" + suffix):
        base = f"{base}_{suffix}"
    if kind == "counter":
        return [base if base.endswith("_total") else base + "_total"]
    if kind == "histogram":
        return [base + "_bucket", base + "_sum", base + "_count"]
    return [base]


def instruments(source: str) -> list[tuple[str, str, str]]:
    """(kind, OTel name, unit) for every instrument awa-metrics builds."""
    names = dict(re.findall(r'pub const ([A-Z0-9_]+): &str = "([^"]+)";', source))
    found = []
    builder = re.compile(
        r"\.(?:u64|f64|i64)_(counter|up_down_counter|histogram|gauge)\(\s*names::([A-Z0-9_]+)\s*\)"
    )
    for match in builder.finditer(source):
        kind = KINDS[match.group(1)]
        end = source.find(".build()", match.end())
        if end == -1:
            sys.exit(f"instrument {match.group(2)} has no .build()")
        unit = re.search(r'\.with_unit\(\s*"([^"]*)"\s*\)', source[match.end() : end])
        found.append((kind, names[match.group(2)], unit.group(1) if unit else ""))
    # Every name constant is built exactly once; a builder this parser
    # misses would otherwise drop out of the check silently.
    built = sorted(name for _, name, _ in found)
    if sorted(names.values()) != built:
        unbuilt = set(names.values()) - set(built)
        sys.exit(f"cannot match metric names to builders: {sorted(unbuilt) or 'duplicates'}")
    return found


def labels(root: pathlib.Path, metric_names: set[str]) -> set[str]:
    keys = set()
    for crate in ("awa-metrics", "awa-worker", "awa-model", "awa"):
        for path in (root / crate / "src").rglob("*.rs"):
            keys.update(re.findall(r'KeyValue::new\(\s*"([^"]+)"', path.read_text()))
    return {sanitize(key) for key in keys if key not in metric_names}


def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    root = pathlib.Path(args[0] if args else ".")
    source = (root / "awa-metrics" / "src" / "lib.rs").read_text()
    found = instruments(source)
    metrics = sorted({s for kind, name, unit in found for s in series_names(name, unit, kind)})
    label_names = sorted(labels(root, {name for _, name, _ in found}))
    if "--labels" not in sys.argv:
        print("\n".join(metrics))
    if "--metrics" not in sys.argv:
        print("\n".join(label_names))


if __name__ == "__main__":
    main()
