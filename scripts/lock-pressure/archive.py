import gzip,hashlib,json,pathlib

if not __debug__:
 raise RuntimeError('lock-pressure evidence verification requires Python assertions')
CHUNK=1024*1024

def sha(path):
    h=hashlib.sha256();size=0
    with pathlib.Path(path).open('rb') as f:
        for b in iter(lambda:f.read(CHUNK),b''):h.update(b);size+=len(b)
    return h.hexdigest(),size

def receipt_rows(path):
    with pathlib.Path(path).open() as f:
        for line in f:yield json.loads(line)

def replay_sources(raw,archive_dir,receipts,stream_index=None):
    index={s['source_key']:s for s in (stream_index or [])}
    verified={};unlinked=set()
    if pathlib.Path(receipts).exists():
        for row in receipt_rows(receipts):
            if row['phase']=='verified_before_unlink':verified[row['source_key']]=row
            elif row['phase']=='unlinked':unlinked.add(row['source_key'])
    sources=[];covered=set()
    for key,row in verified.items():
        archive=pathlib.Path(row['archive_path']);assert sha(archive)==(row['compressed_sha256'],row['compressed_bytes'])
        with gzip.open(archive,'rb') as f:
            h=hashlib.sha256();size=0
            for b in iter(lambda:f.read(CHUNK),b''):h.update(b);size+=len(b)
        assert (h.hexdigest(),size)==(row['source_sha256'],row['original_bytes'])
        sources.append({**row,'path':str(archive),'gzip':True});covered.add(key)
    # A crash after durable receipt but before unlink leaves both copies; replay only the verified archive.
    for path in pathlib.Path(raw).rglob('*'):
        if not path.is_file() or path.suffix not in ('.json','.internal','.log'):continue
        st=path.stat()
        matches=[s for s in (stream_index or []) if s['path']==str(path) and s['device']==st.st_dev and s['inode']==st.st_ino]
        key=matches[-1]['source_key'] if matches else f'{st.st_dev}:{st.st_ino}'
        if key in covered:continue
        sources.append({**index.get(key,{}),'source_key':key,'original_path':index.get(key,{}).get('original_path',str(path)),'path':str(path),'gzip':False,'ordinal':index.get(key,{}).get('ordinal',10**12+st.st_mtime_ns)})
    return sorted(sources,key=lambda s:s['ordinal'])
