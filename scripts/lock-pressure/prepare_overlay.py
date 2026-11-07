import argparse, difflib, hashlib, json, pathlib, re, subprocess

if not __debug__:
 raise RuntimeError('prune overlay verification requires Python assertions')

PACKET=pathlib.Path(__file__).resolve().parent
FUNCTIONS={'prune_oldest':'queue','prune_oldest_leases':'lease','prune_oldest_claims':'claim','rollup_terminal_count_delta_slot':'terminal_delta'}
def sha(text):return hashlib.sha256(text.encode()).hexdigest()
def transform(original, helper):
 source=original;proof=[]
 for name,ring in FUNCTIONS.items():
  starts=list(re.finditer(rf'^    (?:pub )?async fn {name}\(',source,re.M))
  assert len(starts)==1,(name,'signature count')
  start=starts[0].start();opening=source.index(' {\n',start)+2
  closing=re.search(r'^    }\n',source[opening:],re.M)
  assert closing,(name,'closing brace')
  end=opening+closing.start();body=source[opening:end]
  # Source pins and function-level anchor counts make this fail closed on drift.
  locks=list(re.finditer(r'sqlx::query\(&format!\(\s*"LOCK TABLE [^"\n]+"\s*\)\)\s*\.execute\(tx\.as_mut\(\)\)\s*\.await',body))
  assert len(locks)==1,(name,'LOCK expression count',len(locks))
  lock=locks[0];expression=lock.group()
  generation_binding='_generation' if name in ('prune_oldest_leases','prune_oldest_claims') else 'generation'
  if generation_binding=='_generation':assert re.search(r'let Some\(\(slot, _generation\)\)',body),(name,'generation binding')
  modified=body[:lock.start()]+'prune_trace_lock(slot, '+generation_binding+', async { '+expression+' }).await'+body[lock.end():]
  stage_replacements=[]
  truncates=list(re.finditer(r'sqlx::query\(&format!\(\s*"TRUNCATE TABLE [^"\n]+"\s*\)\)\s*\.execute\(tx\.as_mut\(\)\)\s*\.await',modified))
  assert len(truncates)==1,(name,'TRUNCATE expression count',len(truncates))
  for truncate in reversed(truncates):
   expression_truncate=truncate.group()
   replacement='prune_trace_stage("truncate", async { '+expression_truncate+' }).await'
   modified=modified[:truncate.start()]+replacement+modified[truncate.end():]
   stage_replacements.append((replacement,expression_truncate))
  commit_count=modified.count('tx.commit().await')
  modified=modified.replace('tx.commit().await','prune_trace_stage("commit", async { tx.commit().await }).await')
  wrapped='\n        prune_trace_call(pool, "'+ring+'", async {'+modified+'        }).await\n'
  # Removal of the two wrappers must recover every original byte of this function body.
  recovered=modified.replace('prune_trace_stage("commit", async { tx.commit().await }).await','tx.commit().await')
  for replacement,expression_truncate in stage_replacements:recovered=recovered.replace(replacement,expression_truncate,1)
  recovered=recovered.replace('prune_trace_lock(slot, '+generation_binding+', async { '+expression+' }).await',expression,1)
  assert recovered==body
  source=source[:opening]+wrapped+source[end:]
  proof.append({'function':name,'ring':ring,'original_body_sha256':sha(body),'original_lock_expression_sha256':sha(expression),'initial_lock_expressions':1,'truncate_expressions':len(truncates),'direct_commit_expressions':commit_count,'recovered_body_byte_identical':True})
 if 'async fn settle_prune_tx<T>(' in source:
  start=source.index('async fn settle_prune_tx<T>(')
  closing=re.search(r'^}\n',source[start:],re.M);assert closing
  end=start+closing.end();body=source[start:end]
  assert body.count('tx.commit().await')==1
  after=body.replace('tx.commit().await','prune_trace_stage("commit", async { tx.commit().await }).await')
  assert after.replace('prune_trace_stage("commit", async { tx.commit().await }).await','tx.commit().await')==body
  source=source[:start]+after+source[end:]
  proof.append({'function':'settle_prune_tx','direct_commit_expressions':1,'recovered_body_byte_identical':True,'original_body_sha256':sha(body)})
 anchor='const DEFAULT_SCHEMA: &str = "awa";'
 assert source.count(anchor)==1
 return source.replace(anchor,helper+'\n'+anchor,1),proof

def main():
 parser=argparse.ArgumentParser(description='Generate source-only prune tracing overlays without changing the repository.')
 parser.add_argument('--repository',type=pathlib.Path,required=True)
 parser.add_argument('--variant',action='append',required=True,metavar='LABEL=REF')
 parser.add_argument('--out',type=pathlib.Path,required=True)
 args=parser.parse_args();versions={}
 for variant in args.variant:
  label,separator,ref=variant.partition('=')
  if not separator or not re.fullmatch(r'[A-Za-z0-9_-]+',label) or not ref or label in versions:parser.error('variants require unique safe LABEL=REF values')
  versions[label]=subprocess.check_output(['git','-C',str(args.repository),'rev-parse','--verify','--end-of-options',ref+'^{commit}'],text=True).strip()
 args.out.mkdir(parents=True,exist_ok=False)
 helper=(PACKET/'prune_trace.rs').read_text();manifest={'status':'SOURCE_ONLY_UNCOMPILED_UNRUN','helper_sha256':sha(helper),'versions':{}}
 for label,commit in versions.items():
  original=subprocess.check_output(['git','-C',str(args.repository),'show',commit+':awa-model/src/queue_storage.rs']).decode()
  after,proof=transform(original,helper)
  directory=args.out/label;directory.mkdir()
  (directory/'overlay.diff').write_text(''.join(difflib.unified_diff(original.splitlines(True),after.splitlines(True),fromfile='a/awa-model/src/queue_storage.rs',tofile='b/awa-model/src/queue_storage.rs')))
  manifest['versions'][label]={'commit':commit,'before_sha256':sha(original),'after_sha256':sha(after),'proof':proof}
 manifest['prepare_sha256']=sha(pathlib.Path(__file__).read_text())
 (args.out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
 print(json.dumps(manifest,indent=2))
if __name__=='__main__':main()
