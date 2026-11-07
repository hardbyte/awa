"""Pure synthetic replay, provenance and negative controls; no database required."""
import gzip
import importlib.util
import json
from pathlib import Path
import tempfile

HERE = Path(__file__).resolve().parent


def load(name):
    spec = importlib.util.spec_from_file_location('check_' + name, HERE / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    evidence, fast, archive = (load(name) for name in ('evidence', 'offline_core', 'archive'))
    fixture = HERE / 'fixtures/synthetic.json'
    rows = fixture.read_text().splitlines()
    assert len(rows) == 41
    for row in map(json.loads, rows):
        message = row['message']
        assert fast.split_sql(message) == evidence.core.split_sql(message)
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        source = {'path': str(fixture), 'original_path': 'synthetic.json', 'source_key': 'synthetic:stable', 'gzip': False}
        reference = root / 'reference'
        reference.mkdir()
        old = evidence.Utilities(reference, classify_statement=evidence.core.classify)
        for row in map(json.loads, rows):
            old.record(row)
        old_summary = old.finish()
        result = evidence.replay([source], root / 'fast')
        assert all(result[key] == value for key, value in old_summary.items())
        for name in ('utility-events.jsonl', 'utility-summary.json'):
            assert (reference / name).read_bytes() == (root / 'fast' / name).read_bytes()
        keys = result['selected_keys']
        assert keys and evidence.compare_feed_replay(keys, keys)['match']
        assert not evidence.compare_feed_replay(keys + keys[:1], keys)['match']
        assert not evidence.compare_feed_replay([], keys)['match']
        raw = root / 'raw'
        raw.mkdir()
        original = raw / 'server.json'
        original.write_bytes(fixture.read_bytes())
        compressed = root / 'source.json.gz'
        compressed.write_bytes(gzip.compress(fixture.read_bytes()))
        receipt = {'phase': 'verified_before_unlink', 'source_key': 'synthetic:stable', 'archive_path': str(compressed), 'compressed_sha256': archive.sha(compressed)[0], 'compressed_bytes': compressed.stat().st_size, 'source_sha256': archive.sha(original)[0], 'original_bytes': original.stat().st_size, 'ordinal': 0, 'original_path': str(original)}
        receipts = root / 'receipts.jsonl'
        receipts.write_text(json.dumps(receipt) + '\n')
        stat = original.stat()
        index = [{'path': str(original), 'device': stat.st_dev, 'inode': stat.st_ino, 'source_key': 'synthetic:stable'}]
        assert len(archive.replay_sources(raw, root, receipts, index)) == 1
        compressed.write_bytes(b'corrupt')
        try:
            archive.replay_sources(raw, root, receipts, index)
        except AssertionError:
            pass
        else:
            raise AssertionError('corrupt archive accepted')
        partial = root / 'partial.json'
        partial.write_bytes(b'{invalid}\n{}')
        invalid = evidence.replay([dict(source, path=str(partial))], root / 'invalid')
        assert any(row['reason'] == 'invalid JSON' for row in invalid['invalid'])
        assert any(row['reason'] == 'partial EOF' for row in invalid['invalid'])
    print('41 synthetic records: parser equivalence, event bytes, provenance multiplicity, archive corruption, partial/invalid JSON controls passed')


if __name__ == '__main__':
    main()
