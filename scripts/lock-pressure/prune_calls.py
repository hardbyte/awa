"""Bounded post-close derived trace ledger; missing pairs remain incomplete."""
import collections,datetime,json,pathlib
MARKER='AWA_PRUNE_CALL '
def analyze(out,cap=64*1024**2):
    out=pathlib.Path(out);calls={};malformed=[];read=0
    paths=[out/'run.log']+sorted(out.glob('*.stderr'))+sorted(out.glob('*.stdout'))
    for path in paths:
        if not path.exists():continue
        with path.open()as stream:
            for number,line in enumerate(stream,1):
                read+=len(line.encode())
                if read>cap:raise RuntimeError('prune trace analysis input bound; raw retained')
                offset=line.find(MARKER)
                if offset<0:continue
                try:row=json.loads(line[offset+len(MARKER):])
                except ValueError:
                    malformed.append({'path':str(path),'line':number});continue
                key=(row['client_pid'],row['call_id']);item=calls.setdefault(key,{'client_pid':row['client_pid'],'call_id':row['call_id'],'ring':row['ring'],'application':row['application'],'events':[]})
                item['events'].append({'source':str(path),'line':number,**row})
    minutes=collections.Counter();incomplete=[]
    for item in calls.values():
        starts=[e for e in item['events']if e['event']=='call_start'];ends=[e for e in item['events']if e['event']=='call_end']
        item['complete']=len(starts)==len(ends)==1
        if not item['complete']:incomplete.append({'client_pid':item['client_pid'],'call_id':item['call_id'],'start_count':len(starts),'end_count':len(ends)});continue
        end=ends[0];detail=end['details'];item['elapsed_ns']=detail['elapsed_ns'];item['classification']=detail['classification'];item['result_debug']=detail['result_debug']
        minute=datetime.datetime.fromtimestamp(starts[0]['utc_ns']/1e9,datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:00Z')
        item['start_minute_utc']=minute
        minutes[(minute,item['ring'],item['application'],detail['classification'])]+=1
        item['initial_lock_events']=[e for e in item['events']if e['event']=='initial_lock_end']
        item['stage_events']=[e for e in item['events']if e['event']=='stage_end']
        item['initial_55P03']=any(e['details'].get('sqlstate')=='55P03'for e in item['initial_lock_events'])
        item['truncate_successes']=sum(e['details']['stage']=='truncate'and e['details']['ok']for e in item['stage_events'])
        item['commit_successes']=sum(e['details']['stage']=='commit'and e['details']['ok']for e in item['stage_events'])
    result={'schema':'prune-call-ledger-v1','calls':list(calls.values()),'per_start_minute':[{'minute':k[0],'ring':k[1],'application':k[2],'classification':k[3],'count':v}for k,v in sorted(minutes.items())],'incomplete_calls':incomplete,'malformed_marker_lines':malformed,'capture_scope':'run.log and top-level stderr/stdout; child forwarding must be proven by controls','limits':['Client pid/call id is process scoped, not a backend identity.','Unlogged/cancelled/panicked calls are unknown; logging errors were intentionally ignored by overlay.','Initial LOCK55P03 distinguished; later Blocked/error is not assigned an exact SQLSTATE unless an observed stage/server ledger supplies it.','Successful TRUNCATE is not commit proof; stages are attached to the same invocation.','UTC minute boundaries are observations; request duration uses monotonic elapsed.']}
    (out/'prune-call-ledger.json').write_text(json.dumps(result,indent=2)+'\n');return {'calls':len(calls),'complete':len(calls)-len(incomplete),'incomplete':len(incomplete),'malformed':len(malformed)}

def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=pathlib.Path)
    parser.add_argument('--cap-bytes',type=int,default=64*1024**2)
    args=parser.parse_args()
    if args.cap_bytes<=0:parser.error('--cap-bytes must be positive')
    print(json.dumps(analyze(args.directory,args.cap_bytes),indent=2))

if __name__=='__main__':main()
