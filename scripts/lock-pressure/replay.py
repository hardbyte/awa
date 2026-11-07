"""Replay a caller-supplied list of closed PostgreSQL log sources."""
import argparse
import importlib.util
import json
from pathlib import Path

if not __debug__:
    raise RuntimeError('lock-pressure evidence verification requires Python assertions')

HERE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sources', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--event-cap-bytes', type=int, default=256*1024**2)
    args = parser.parse_args()
    if args.event_cap_bytes <= 0:
        parser.error('--event-cap-bytes must be positive')
    sources = json.loads(args.sources.read_text())
    if not isinstance(sources, list):
        parser.error('--sources must contain a JSON list')
    for source in sources:
        for key in ('path', 'original_path', 'source_key'):
            if not isinstance(source.get(key), str):
                parser.error('each source requires string ' + key)
        path = Path(source['path'])
        source['path'] = str(path if path.is_absolute() else args.sources.resolve().parent / path)
    args.out.mkdir(parents=True, exist_ok=False)
    spec = importlib.util.spec_from_file_location('log_evidence', HERE / 'evidence.py')
    evidence = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(evidence)
    result = evidence.replay(sources, args.out, args.event_cap_bytes)
    print(json.dumps({'counts': result['counts'], 'invalid_lines': len(result['invalid'])}, indent=2))


if __name__ == '__main__':
    main()
