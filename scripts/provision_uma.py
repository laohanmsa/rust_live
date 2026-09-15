"""Run only on MP. Reuse existing Polygon endpoints; never print credentials."""
import json
import os
from pathlib import Path
import subprocess
from urllib.parse import urlsplit, urlunsplit

root=Path('/opt/polym-rust-demo/secrets')
root.mkdir(mode=0o700,parents=True,exist_ok=True)
rows=json.loads(subprocess.check_output(['docker','inspect','polym_amster-uma_listener-1','polym_amster-web-1']))
env={}
for row in rows:
    for item in row['Config']['Env']:
        k,_,v=item.partition('=')
        if k.startswith('POLYGON_') or k in ['ALCHEMY_API_KEY','PUNCHER_POLYGON_RPC_URL','PUNCHER_POLYGON_RPC_URLS']:
            if v:env[k]=v
http=[];ws=[]
for key in ['POLYGON_HTTP_URL','POLYGON_RPC_URL','POLYGON_BACKFILL_URL','POLYGON_RPC_URLS','PUNCHER_POLYGON_RPC_URL','PUNCHER_POLYGON_RPC_URLS','POLYGON_WSS_URL']:
    for url in env.get(key,'').replace(',', ' ').split():
        if url.startswith('https://'):http.append(url)
        if url.startswith('wss://'):ws.append(url)
        parsed=urlsplit(url)
        if parsed.hostname and 'alchemy.com' in parsed.hostname and parsed.scheme=='https':
            ws.insert(0,urlunsplit(('wss',parsed.netloc,parsed.path,parsed.query,'')))
if env.get('ALCHEMY_API_KEY'):
    key=env['ALCHEMY_API_KEY'].split(',')[0].strip()
    http.insert(0,'https://polygon-mainnet.g.alchemy.com/v2/'+key)
    ws.insert(0,'wss://polygon-mainnet.g.alchemy.com/v2/'+key)
http += ['https://polygon-bor-rpc.publicnode.com','https://polygon.drpc.org','https://gateway.tenderly.co/public/polygon']
ws += ['wss://polygon-bor-rpc.publicnode.com','wss://polygon.drpc.org']
def distinct_hosts(urls,limit):
    result=[];hosts=set()
    for url in urls:
        host=urlsplit(url).hostname
        if host and host not in hosts:
            hosts.add(host);result.append(url)
        if len(result)==limit:break
    return result
config=dict(http_urls=distinct_hosts(http,3),ws_urls=distinct_hosts(ws,2),nats_url='nats://nats:4222',bind='0.0.0.0:8788',retention_seconds=14400)
path=root/'uma.json'
fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o400)
os.fchmod(fd,0o400);os.fchown(fd,10001,10001)
with os.fdopen(fd,'w') as f:json.dump(config,f)
print(json.dumps({'uma_config_written':str(path),'http_provider_hosts':[urlsplit(v).hostname for v in config['http_urls']],'ws_provider_hosts':[urlsplit(v).hostname for v in config['ws_urls']]}))
