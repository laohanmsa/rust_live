"""Install the independently verified public Polygon endpoints on MP."""
import json
import os
from pathlib import Path
from urllib.parse import urlsplit
root=Path('/opt/polym-rust-demo/secrets')
root.mkdir(mode=0o700,parents=True,exist_ok=True)
config=dict(http_urls=['https://gateway.tenderly.co/public/polygon','https://polygon.drpc.org','https://polygon-bor-rpc.publicnode.com'],
            ws_urls=['wss://polygon.drpc.org','wss://polygon-bor-rpc.publicnode.com'],
            nats_url='nats://nats:4222',bind='0.0.0.0:8788',retention_seconds=14400)
path=root/'uma.json'
fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o400)
os.fchmod(fd,0o400);os.fchown(fd,10001,10001)
with os.fdopen(fd,'w') as f:json.dump(config,f)
print(json.dumps({'uma_config_written':str(path),'http_provider_hosts':[urlsplit(v).hostname for v in config['http_urls']],'ws_provider_hosts':[urlsplit(v).hostname for v in config['ws_urls']]}))
