import json,glob,sys,os
def pct(v,p):
    if not v: return None
    v=sorted(v); return v[min(len(v)-1,int(p/100*len(v)))]
rows=[]
for d in sorted(glob.glob(sys.argv[1]+'/exp2-f14-*/*/tables-*')):
    var=d.split('exp2-f14-')[1].split('/')[0]; acct='flat' if 'flat' in d else 'hns'; n=int(d.split('tables-')[1])
    c=[];s=[];end=None;first=None
    for l in open(d+'/run.jsonl'):
        if '"kind":"commit"' in l: c.append(json.loads(l))
        elif '"kind":"sample"' in l: s.append(json.loads(l))
        elif '"kind":"end"' in l: end=json.loads(l)
        elif first is None: first=json.loads(l)
    chk=json.load(open(d+'/check.json'))
    ck=[x for x in c if x.get('checkpoint_ms') is not None]
    files=sum(x['files'] for x in c)
    t0=min(x['ts_ms'] for x in c); t1=max(x['ts_ms'] for x in c)
    maxv=max((x['delta_version'] or 0) for x in c)
    vis=[x['visible_oldest_ms'] for x in c]; cm=[x['commit_ms'] for x in c]; pw=[x.get('permit_wait_ms',0) for x in c]
    rows.append(dict(acct=acct,var=var,n=n,stalled=chk.get('stalled',False),ok=all(chk.get(k,False) for k in ('g1_no_loss','g2_no_duplicates','g3_atomic','g4_valid')),
      commits=len(c),files=files,produced=(end or {}).get('files_produced'),cps=round(len(c)/max(1,(t1-t0)/1000),1),
      fpc=round(files/len(c),2),maxv=maxv,
      commit_p50=round(pct(cm,50)),commit_p99=round(pct(cm,99)),vis_p50=round(pct(vis,50)),vis_p99=round(pct(vis,99)),vis_max=round(max(vis)),
      pw_p50=round(pct(pw,50)),pw_p99=round(pct(pw,99)),
      ckpts=len(ck),ck_p50=round(pct([x['checkpoint_ms'] for x in ck],50) or 0),ck_p99=round(pct([x['checkpoint_ms'] for x in ck],99) or 0),
      ckw_p99=round(pct([x.get('checkpoint_permit_wait_ms') or 0 for x in ck],99) or 0),
      thr_max=max(x.get('threads',{}).get('commit',0) for x in s),
      cpu=round((end or s[-1])['cpu_secs'].get('commit',0)),rss=round(max(x['rss_mb'] for x in s)),
      elapsed=round((end or {}).get('elapsed_s',0)),errs=len((end or {}).get('loop_errors',[]) or [])))
import csv
w=csv.DictWriter(sys.stdout,fieldnames=list(rows[0].keys())); w.writeheader(); [w.writerow(r) for r in rows]
