import collections,threading,gzip,hashlib,importlib.util,json,os,pathlib,re,time
HERE=pathlib.Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('core_parser',HERE/'core_parser.py');core=importlib.util.module_from_spec(spec);spec.loader.exec_module(core)
oldspec=importlib.util.spec_from_file_location('old_telemetry',HERE/'log_helpers.py');old=importlib.util.module_from_spec(oldspec);oldspec.loader.exec_module(old)
family=old.family;issuer=old.issuer;digest=old.digest
CRASH=old.CRASH;SEVERE=old.SEVERE
class InodePump:
    def __init__(self,raw,feed,cap=16*1024**2,line_cap=1024**2):
        self.raw=pathlib.Path(raw);self.feed=pathlib.Path(feed);self.feed.touch(exist_ok=False);self.cap=cap;self.line_cap=line_cap;self.streams={};self.emitted=[];self.errors=[];self.crashes=[];self.bytes=0;self.next_ordinal=0;self.lock=threading.RLock()
    def discover(self):
        paths=list(self.raw.iterdir())
        if len(paths)>256:raise RuntimeError('raw file count cap')
        for p in sorted(paths):
            if not p.is_file() or p.suffix not in ('.json','.internal','.log'):continue
            st=p.stat();base=f'{st.st_dev}:{st.st_ino}';key=base
            active=[k for k,v in self.streams.items() if v['device']==st.st_dev and v['inode']==st.st_ino and v['path']==str(p) and not v.get('archived')]
            if active:key=active[-1]
            elif key in self.streams and self.streams[key].get('archived'):key=base+':generation'+str(self.next_ordinal)
            if key not in self.streams:
                self.streams[key]={'source_key':key,'original_path':str(p),'path':str(p),'device':st.st_dev,'inode':st.st_ino,'ordinal':self.next_ordinal,'offset':0,'partial':b''};self.next_ordinal+=1
    def poll(self,budget=32*1024**2):
        with self.lock:return self._poll(budget)
    def _poll(self,budget):
        self.discover();used=0;began=time.monotonic()
        for stream in list(self.streams.values()):
            if stream.get('archived') or stream.get('staged'):continue
            path=pathlib.Path(stream['path'])
            while used<budget and time.monotonic()-began<.25:
                if not path.exists():self.errors.append({'reason':'unarchived inode disappeared','source_key':stream['source_key']});break
                st=path.stat()
                if (st.st_dev,st.st_ino)!=(stream['device'],stream['inode']) or st.st_size<stream['offset']:self.errors.append({'reason':'inode replaced/truncated','source_key':stream['source_key']});break
                with path.open('rb') as f:f.seek(stream['offset']);data=f.read(min(1024**2,budget-used))
                if not data:break
                used+=len(data);offset=stream['offset']-len(stream['partial']);stream['offset']+=len(data);lines=(stream['partial']+data).split(b'\n');stream['partial']=lines.pop()
                for line in lines:
                    if len(line)>self.line_cap:self.errors.append({'reason':'line cap','source_key':stream['source_key'],'offset':offset})
                    else:self.line(stream,line,offset)
                    offset+=len(line)+1
                if len(stream['partial'])>self.line_cap:self.errors.append({'reason':'partial line cap','source_key':stream['source_key']});stream['partial']=b''
        self.last_poll={'bytes':used,'elapsed_s':time.monotonic()-began,'budget_bytes':budget,'soft_cpu_deadline_s':.25}
        return used
    def line(self,stream,line,offset):
        text=line.decode(errors='replace');record=None
        if pathlib.Path(stream['original_path']).suffix=='.json':
            try:record=json.loads(text)
            except ValueError:self.errors.append({'reason':'invalid JSON','source_key':stream['source_key'],'offset':offset})
        selected=(record and record.get('error_severity') in SEVERE) or any(x in text for x in CRASH) or (not record and re.search(r'\b(?:WARNING|ERROR|FATAL|PANIC):',text))
        if not selected:return
        if any(x in text for x in ('terminated by signal','TRAP:','PANIC:','all server processes terminated')) or (record and record.get('error_severity')=='PANIC'):self.crashes.append({'source_key':stream['source_key'],'offset':offset})
        event={'source_key':stream['source_key'],'original_path':stream['original_path'],'offset':offset,'sha256':hashlib.sha256(line).hexdigest(),'rendered':record.get('error_severity','')+': '+record.get('message','') if record else text,'record':record}
        data=(json.dumps(event,separators=(',',':'))+'\n').encode()
        if self.bytes+len(data)>self.cap:self.errors.append({'reason':'feed cap'});raise RuntimeError('feed cap')
        with self.feed.open('ab') as f:f.write(data)
        self.bytes+=len(data);self.emitted.append((stream['source_key'],offset,event['sha256']))
    def drain(self):
        while self.poll():pass
        return self.status()
    def status(self):return {'last_poll':getattr(self,'last_poll',None),'streams':[{**s,'partial':len(s['partial'])} for s in self.streams.values()],'emitted_keys':self.emitted,'feed_bytes':self.bytes,'errors':self.errors,'crashes':self.crashes}

class Utilities:
    def __init__(self,out,cap=256*1024**2,classify_statement=core.classify):
        self.classify_statement=classify_statement;self.out=pathlib.Path(out);self.out.mkdir(exist_ok=True);self.events=(self.out/'utility-events.jsonl').open('w');self.sessions={};self.counts=collections.Counter();self.minutes=collections.defaultdict(collections.Counter);self.quality=collections.Counter();self.cap=cap;self.protocol=collections.Counter()
    def emit(self,event):
        data=json.dumps(event)+'\n'
        if self.events.tell()+len(data.encode())>self.cap:raise RuntimeError('utility event cap')
        self.events.write(data)
        kind=event.get('kind','unknown');outcome=event.get('outcome','unknown');self.counts[kind+':'+outcome]+=1
        self.minutes[((event.get('started_at') or event.get('at') or 'unknown')[:16],event.get('app',''),event.get('database'),kind)][outcome]+=1
    def record(self,row):
        sid=row.get('session_id') or 'pid:'+str(row.get('pid'));s=self.sessions.setdefault(sid,{'last':None,'pending':None});line=row.get('line_num');last=s['last'];s['last']=line
        if isinstance(line,int) and isinstance(last,int):
            if line<=last:self.quality['duplicate_or_out_of_order']+=1
            elif line>last+1:self.quality['line_gaps']+=1
        app=row.get('application_name','');msg=core.normalize_message(row.get('message',''));statement=None
        if msg.startswith('statement: '):statement=msg[11:];protocol='simple'
        else:
            m=re.match(r'execute\s+(?:fetch from\s+)?[^:]+:\s*(.*)',msg,re.S)
            if m:statement=m[1];protocol='extended'
        if statement is not None:
            if s['pending']:
                self.quality['new_start_before_end']+=1
                if s['pending']['kind'] in core.TARGETS:self.emit({**s['pending'],'outcome':'unknown','reason':'new start before end'})
            s['pending']={**self.classify_statement(statement),'sql':statement,'app':app,'database':row.get('dbname'),'session_id':sid,'pid':row.get('pid'),'started_at':row.get('timestamp'),'start_line':line,'protocol':protocol};self.protocol[protocol+'_starts']+=1;return
        duration=re.match(r'duration:\s*([\d.]+)\s+ms(?:\s+(parse|bind|execute)\b)?',msg,re.I)
        if duration:
            phase=(duration[2] or 'generic').lower();self.protocol[phase+'_durations']+=1
            if phase in ('parse','bind'):
                if s['pending']:self.quality['phase_during_pending']+=1
                return
            pending=s['pending'];s['pending']=None
            if pending:
                if pending['kind'] in core.TARGETS:self.emit({**pending,'outcome':'success','sqlstate':None,'finished_at':row.get('timestamp'),'end_line':line,'duration_ms':float(duration[1])})
                elif pending['kind']=='unknown_batch':self.emit({**pending,'outcome':'unknown','reason':'batch completion'})
            return
        if row.get('error_severity') in ('ERROR','FATAL','PANIC'):
            pending=s['pending'];s['pending']=None;state=row.get('state_code')
            if pending and pending['kind'] in core.TARGETS:self.emit({**pending,'outcome':'blocked' if state=='55P03' else 'error','sqlstate':state,'finished_at':row.get('timestamp'),'end_line':line})
            elif pending and pending['kind']=='unknown_batch':self.emit({**pending,'outcome':'unknown','reason':'batch error','sqlstate':state})
            elif re.match(r'(LOCK|TRUNCATE)\b',core.remove_leading_comments(row.get('statement','')),re.I):self.emit({'kind':'pre_execute_error','app':app,'database':row.get('dbname'),'sqlstate':state,'outcome':'error','at':row.get('timestamp'),'sql':row.get('statement')})
    def finish(self):
        for s in self.sessions.values():
            pending=s['pending']
            if pending and pending['kind'] in core.TARGETS|{'unknown_batch'}:self.emit({**pending,'outcome':'unknown','reason':'EOF'})
        self.events.close()
        result={'counts':dict(self.counts),'quality':dict(self.quality),'protocol':dict(self.protocol),'per_minute_by_start':[{'minute_utc':key[0],'app':key[1],'database':key[2],'kind':key[3],'counts':dict(values)} for key,values in sorted(self.minutes.items(),key=lambda kv:str(kv[0]))],'unit':'direct SQL commands; successes do not prove transaction commit; no internal lock-request claim'}
        (self.out/'utility-summary.json').write_text(json.dumps(result,indent=2)+'\n');return result

def replay(sources,out,event_cap=256*1024**2):
    offline_spec=importlib.util.spec_from_file_location('offline_parser',HERE/'offline_core.py');offline_core=importlib.util.module_from_spec(offline_spec);offline_spec.loader.exec_module(offline_core)
    out=pathlib.Path(out);out.mkdir(exist_ok=True);utility=Utilities(out,cap=event_cap,classify_statement=offline_core.classify);selected=[];invalid=[]
    for source in sources:
        opener=gzip.open if source.get('gzip') else open;offset=0
        with opener(source['path'],'rb') as f:
            for raw in f:
                if not raw.endswith(b'\n'):invalid.append({'source_key':source['source_key'],'offset':offset,'reason':'partial EOF'})
                line=raw.rstrip(b'\n');text=line.decode(errors='replace');row=None
                if pathlib.Path(source['original_path']).suffix=='.json':
                    try:row=json.loads(text);utility.record(row)
                    except (ValueError,TypeError):invalid.append({'source_key':source['source_key'],'offset':offset,'reason':'invalid JSON'})
                if (row and row.get('error_severity') in SEVERE) or any(x in text for x in CRASH) or (not row and re.search(r'\b(?:WARNING|ERROR|FATAL|PANIC):',text)):selected.append((source['source_key'],offset,hashlib.sha256(line).hexdigest()))
                offset+=len(raw)
    result=utility.finish();result.update(invalid=invalid,selected_keys=selected);(out/'replay.json').write_text(json.dumps(result,indent=2)+'\n');return result


def compare_feed_replay(live_keys,replayed_keys):
    live=[tuple(key) for key in live_keys];replayed=[tuple(key) for key in replayed_keys]
    live_counts=collections.Counter(live);replayed_counts=collections.Counter(replayed)
    live_streams=collections.defaultdict(list);replayed_streams=collections.defaultdict(list)
    for key in live:live_streams[key[0]].append(key)
    for key in replayed:replayed_streams[key[0]].append(key)
    live_duplicates=sum(count-1 for count in live_counts.values() if count>1)
    replay_duplicates=sum(count-1 for count in replayed_counts.values() if count>1)
    multiplicity_match=live_counts==replayed_counts
    stream_order_match=dict(live_streams)==dict(replayed_streams)
    return {'match':multiplicity_match and stream_order_match and not live_duplicates and not replay_duplicates,
            'multiplicity_match':multiplicity_match,'per_stream_order_match':stream_order_match,
            'global_interleaving_match':live==replayed,'live_events':len(live),'replayed_events':len(replayed),
            'live_duplicate_keys':live_duplicates,'replayed_duplicate_keys':replay_duplicates,
            'missing_event_instances':sum((live_counts-replayed_counts).values()),
            'extra_event_instances':sum((replayed_counts-live_counts).values()),
            'order_scope':'exact within each inode stream; cross-stream replay order differs from live polling interleaving'}
