import argparse, collections, json, pathlib, re

TARGETS = {'lock_ax', 'truncate'}

def normalize_message(message):
    return re.sub(r'^(?:\[[^\]\n]+:\d+\]\s*)+', '', message)

def split_sql(sql):
    parts=[]; start=0; position=0; quote=None; dollar=None; block=0; line=False
    while position < len(sql):
        char=sql[position]; nextchar=sql[position+1:position+2]
        if line:
            if char=='\n':line=False
        elif block:
            if char=='/' and nextchar=='*':block+=1;position+=1
            elif char=='*' and nextchar=='/':block-=1;position+=1
        elif dollar:
            if sql.startswith(dollar,position):position+=len(dollar)-1;dollar=None
        elif quote:
            if char==quote:
                if nextchar==quote:position+=1
                else:quote=None
            elif char=='\\' and quote=="'":position+=1
        elif char=='-' and nextchar=='-':line=True;position+=1
        elif char=='/' and nextchar=='*':block=1;position+=1
        elif char in "'\"":quote=char
        elif char=='$':
            match=re.match(r'\$(?:[A-Za-z_]\w*)?\$',sql[position:])
            if match:dollar=match.group();position+=len(dollar)-1
        elif char==';':
            if sql[start:position].strip():parts.append(sql[start:position].strip())
            start=position+1
        position+=1
    if sql[start:].strip():parts.append(sql[start:].strip())
    return parts

def remove_leading_comments(sql):
    while True:
        sql=sql.lstrip()
        if sql.startswith('--'):
            end=sql.find('\n');sql='' if end<0 else sql[end+1:]
        elif sql.startswith('/*'):
            depth=1;index=2
            while index<len(sql) and depth:
                if sql[index:index+2]=='/*':depth+=1;index+=2
                elif sql[index:index+2]=='*/':depth-=1;index+=2
                else:index+=1
            sql=sql[index:]
        else:return sql

def split_targets(text):
    names=[];start=0;quoted=False;index=0
    while index<len(text):
        if text[index]=='"':
            if quoted and text[index+1:index+2]=='"':index+=1
            else:quoted=not quoted
        elif text[index]==',' and not quoted:names.append(text[start:index].strip());start=index+1
        index+=1
    names.append(text[start:].strip())
    return names

def classify_single(sql):
    sql=remove_leading_comments(sql).strip()
    match=re.fullmatch(r'LOCK\s+(?:TABLE\s+)?(.+?)\s+IN\s+ACCESS\s+EXCLUSIVE\s+MODE(?:\s+(NOWAIT))?',sql,re.I|re.S)
    if match:return {'kind':'lock_ax','targets':split_targets(match[1]),'nowait':bool(match[2])}
    match=re.match(r'TRUNCATE\s+(?:TABLE\s+)?(.+)',sql,re.I|re.S)
    if match:
        names=re.sub(r'\s+(?:RESTART|CONTINUE)\s+IDENTITY(?:\s+(?:CASCADE|RESTRICT))?\s*$|\s+(?:CASCADE|RESTRICT)\s*$','',match[1],flags=re.I)
        return {'kind':'truncate','targets':split_targets(names),'nowait':False}
    return {'kind':'other','targets':[],'nowait':False}

def classify(sql):
    statements=split_sql(sql)
    if len(statements)!=1:
        target_parts=[classify_single(s) for s in statements if classify_single(s)['kind'] in TARGETS]
        return {'kind':'unknown_batch' if target_parts else 'other','targets':[], 'nowait':False,'statement_count':len(statements)}
    return classify_single(statements[0])

def parse_records(records):
    groups=collections.defaultdict(list); duplicates=[]; bad=[]
    for record in records:
        key=record.get('session_id') or f"pid:{record.get('pid')}:{record.get('session_start','')}"
        groups[key].append(record)
    result={'commands':[],'pre_execute_errors':[],'unknown_batches':[],'gaps':[],'duplicates':duplicates,'protocol_anomalies':[],'protocol_counts':{},'records':len(records)}
    counters=collections.Counter()
    for session,rows in sorted(groups.items()):
        rows.sort(key=lambda r:r.get('line_num',0)); pending=None;last=None;seen={}
        for row in rows:
            line=row.get('line_num'); app=row.get('application_name',''); message=normalize_message(row.get('message',''))
            key=(session,line)
            if key in seen:
                duplicates.append({'session_id':session,'line_num':line,'app':app,'identical':seen[key]==row});continue
            seen[key]=row
            if isinstance(line,int) and isinstance(last,int) and line>last+1:result['gaps'].append({'session_id':session,'app':app,'after':last,'before':line})
            last=line
            statement=None;protocol=None
            if message.startswith('statement: '):statement=message[len('statement: '):];protocol='simple'
            else:
                match=re.match(r'execute\s+(?:fetch from\s+)?[^:]+:\s*(.*)',message,re.S)
                if match:statement=match[1];protocol='extended'
            if statement is not None:
                if pending:
                    result['protocol_anomalies'].append({'app':app,'reason':'new start before prior end','session_id':session,'prior':pending})
                    if pending['kind'] in TARGETS:result['commands'].append({**pending,'outcome':'unknown','end_reason':'new start before prior end'})
                info=classify(statement)
                pending={**info,'app':app,'sql':statement,'session_id':session,'pid':row.get('pid'),'database':row.get('dbname'),'user':row.get('user'),'started_at':row.get('timestamp'),'start_line':line,'protocol':protocol}
                counters[protocol+'_starts']+=1
                continue
            duration=re.match(r'duration:\s*([\d.]+)\s+ms(?:\s+(parse|bind|execute)\b)?',message,re.I)
            if duration:
                phase=(duration[2] or 'generic').lower();counters[phase+'_durations']+=1
                if phase in ('parse','bind'):
                    if pending:result['protocol_anomalies'].append({'app':app,'session_id':session,'reason':phase+' during pending execution'})
                    continue
                if pending:
                    end={**pending,'finished_at':row.get('timestamp'),'end_line':line,'duration_ms':float(duration[1]),'sqlstate':None,'outcome':'success'}
                    if pending['kind'] in TARGETS:result['commands'].append(end);counters['target_success_completions']+=1
                    elif pending['kind']=='unknown_batch':result['unknown_batches'].append({**end,'outcome':'unknown','end_reason':'simple batch completion cannot resolve utilities'})
                    else:counters['non_target_completions']+=1
                    pending=None
                else:counters['unpaired_non_execution_durations']+=1
                continue
            severity=row.get('error_severity','').upper()
            if severity in ('ERROR','FATAL','PANIC'):
                state=row.get('state_code'); counters['error_records']+=1
                if pending:
                    end={**pending,'finished_at':row.get('timestamp'),'end_line':line,'sqlstate':state,'outcome':'blocked' if state=='55P03' else 'error','error_message':row.get('message')}
                    if pending['kind'] in TARGETS:result['commands'].append(end)
                    elif pending['kind']=='unknown_batch':result['unknown_batches'].append({**end,'outcome':'unknown','end_reason':'batch error cannot resolve utilities'})
                    pending=None
                elif re.match(r'(LOCK|TRUNCATE)\b',remove_leading_comments(row.get('statement','')),re.I):
                    result['pre_execute_errors'].append({'app':app,'session_id':session,'line_num':line,'sql':row.get('statement'),'sqlstate':state,'at':row.get('timestamp'),'phase':'before observed Execute'})
        if pending:
            if pending['kind'] in TARGETS:result['commands'].append({**pending,'outcome':'unknown','end_reason':'end of captured log'})
            elif pending['kind']=='unknown_batch':result['unknown_batches'].append({**pending,'outcome':'unknown','end_reason':'end of captured log'})
    result['protocol_counts']=dict(counters)
    minutes=collections.defaultdict(collections.Counter)
    for command in result['commands']:
        minute=(command.get('started_at') or 'unknown')[:16]
        key=(minute,command.get('app',''),command.get('database'),command['kind'])
        minutes[key]['starts']+=1
        minutes[key][command['outcome']]+=1
        minutes[key]['explicit_targets']+=len(command['targets'])
    result['per_minute_by_start']=[dict(minute_utc=key[0],app=key[1],database=key[2],kind=key[3],**counts) for key,counts in sorted(minutes.items(),key=lambda item:tuple(str(k) for k in item[0]))]
    return result

def read_logs(paths):
    records=[];invalid=[];files=[]
    for path in sorted(paths):
        content=path.read_bytes();files.append({'path':str(path),'bytes':len(content)})
        for number,line in enumerate(content.splitlines(),1):
            try:records.append(json.loads(line))
            except (ValueError,TypeError):invalid.append({'path':str(path),'line':number,'bytes':len(line)})
    result=parse_records(records);result.update(files=files,invalid_json=invalid)
    return result

def verify_manual(result,expectations):
    wanted=collections.Counter((e['app'],e['kind'],e['outcome'],e.get('sqlstate')) for e in expectations if e['kind'] in TARGETS)
    observed=[e for e in result['commands'] if e['app'].startswith('core_')]
    actual=collections.Counter((e['app'],e['kind'],e['outcome'],e.get('sqlstate')) for e in observed)
    pre=[e for e in result['pre_execute_errors'] if e['app']=='core_preexecute']
    batches=[e for e in result['unknown_batches'] if e['app']=='core_simple_batch']
    quality={key:[e for e in result[key] if e.get('app','').startswith('core_')] for key in ['gaps','duplicates','protocol_anomalies']}
    checks={'exact_expected_commands':wanted==actual,'nine_target_starts':len(observed)==9,
        'six_success_two_blocked_one_error':collections.Counter(e['outcome'] for e in observed)=={'success':6,'blocked':2,'error':1},
        'pre_execute_error_retained':len(pre)==1 and pre[0]['sqlstate']=='42601',
        'simple_batch_unknown':len(batches)==1 and batches[0]['outcome']=='unknown',
        'non_ax_quiet':not any(e['app']=='core_non_ax' for e in observed),
        'multi_target_one_command':sum(e['app']=='core_ax_multi' and len(e['targets'])==2 for e in observed)==1,
        'prepared_statement_reuse_two_commands':sum(e['app']=='core_ax_reuse' for e in observed)==2,
        'parse_and_bind_counted_separately':result['protocol_counts'].get('parse_durations',0)>0 and result['protocol_counts'].get('bind_durations',0)>0,
        'no_own_log_gaps_duplicates_or_anomalies':not any(quality.values()),'valid_json':not result['invalid_json']}
    return {'status':'PASS' if all(checks.values()) else 'FAIL','checks':checks,'expected':list(wanted.items()),'actual':list(actual.items()),'quality':quality,'protocol_counts':result['protocol_counts'],'target_counts':dict(collections.Counter(e['outcome'] for e in observed))}

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--log-dir',type=pathlib.Path,required=True);parser.add_argument('--out',type=pathlib.Path,required=True);parser.add_argument('--probe-output',type=pathlib.Path)
    args=parser.parse_args();result=read_logs(args.log_dir.rglob('*.json'));args.out.mkdir(exist_ok=True)
    (args.out/'parsed.json').write_text(json.dumps(result,indent=2)+'\n')
    if args.probe_output:
        expected=[json.loads(line) for line in args.probe_output.read_text().splitlines() if line.startswith('{')]
        verdict=verify_manual(result,[x for x in expected if x.get('type')=='expected'])
        (args.out/'verdict.json').write_text(json.dumps(verdict,indent=2)+'\n');print(json.dumps(verdict));return 0 if verdict['status']=='PASS' else 1
    return 0
if __name__=='__main__':raise SystemExit(main())
