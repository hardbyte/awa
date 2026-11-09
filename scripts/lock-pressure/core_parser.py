import re

TARGETS = {'lock_ax', 'truncate'}

def normalize_message(message):
    return re.sub(r'^(?:\[[^\]\n]+:\d+\]\s*)+', '', message)

def split_sql(sql):
    if ';' not in sql:
        stripped=sql.strip()
        return [stripped] if stripped else []
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
