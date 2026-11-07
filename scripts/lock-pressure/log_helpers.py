import collections, hashlib, importlib.util, json, pathlib, re, time
ROOT=pathlib.Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('core_parser',ROOT/'core_parser.py')
core=importlib.util.module_from_spec(spec);spec.loader.exec_module(core)
CRASH=('terminated by signal','TRAP:','server process (PID','PANIC:','all server processes terminated','database system was not properly shut down','restart_after_crash','buffer control','signal 9','signal 11')
SEVERE={'WARNING','ERROR','FATAL','PANIC'}
def digest(path):
    result=hashlib.sha256()
    with pathlib.Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):result.update(block)
    return result.hexdigest()

def jsonlines(path):
    with pathlib.Path(path).open() as stream:
        for line in stream:yield json.loads(line)

def family(query,state='active',truncated=False):
    if state!='active':return {'family':'idle_retained' if state.startswith('idle in transaction') else 'inactive','last_query_family':family(query)['family']}
    if truncated or not query or query=='<insufficient privilege>':return {'family':'unknown','reason':'truncated/missing/visibility'}
    if re.search(r'\$(?:[A-Za-z_]\w*)?\$|\b[Ee]\'',query):return {'family':'unknown','reason':'unsupported literal syntax'}
    query_without_identifiers=re.sub(r'"(?:""|[^"])*"',' quoted_identifier ',query)
    # Positive fragments are evaluated only outside SQL string literals and comments.
    text=re.sub(r"'(?:''|[^'])*'",' ? ',query_without_identifiers)
    text=re.sub(r'/\*.*?\*/|--[^\n]*',' ',text,flags=re.S)
    text=' '.join(text.lower().split())
    matches=set()
    if re.search(r'\bfrom\s+[a-z_][\w]*\.claim_ready_runtime\(',text):matches.add('claim')
    if re.search(r'\bwith completed\s*\(lease_slot, queue, priority, enqueue_shard, lane_seq\)',text):matches.add('complete')
    if re.fullmatch(r'select \* from awa\.jobs where id = \$1\s*;?',text):matches.add('get_job')
    if 'select * from awa.jobs where ($1::awa.job_state is null or state = $1)' in text and 'order by id desc limit $6' in text:matches.add('list_jobs')
    if re.fullmatch(r'select distinct ready_slot from [a-z_][\w]*\.ready_entries where job_id = \$1\s*;?',text):matches.add('point_or_list_hydration')
    if core.classify(query)['kind'] in core.TARGETS:matches.add('maintenance')
    return {'family':next(iter(matches)) if len(matches)==1 else 'unknown','matches':sorted(matches),'sql_sha256':hashlib.sha256(query.encode()).hexdigest()}

def issuer(app):
    if app=='awa_gate_observer':return 'observer'
    if 'proof' in app:return 'proof_python' if 'python' in app else 'proof_child' if 'child' in app else 'proof_parent'
    if app=='soak_ui_server':return 'ui_shared'
    return app or 'unassigned'

