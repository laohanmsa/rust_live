#!/usr/bin/env python3
"""Build a read-only monitor on Brahma, with a forced-command key for MP telemetry."""
import argparse
import json
from pathlib import Path
import shlex
import subprocess
from deploy import ROOT, REPOSITORY, credentials, remote, run, temp


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--registry-env')
    args=parser.parse_args()
    if run(['git','-C',str(ROOT),'status','--porcelain'],capture=True).strip():
        raise RuntimeError('monitor deployment requires a clean commit')
    sha=run(['git','-C',str(ROOT),'rev-parse','HEAD'],capture=True).decode().strip()
    root='/home/anchen/ops/rust-224-monitor'
    if remote('brahma','id -u',capture=True).strip()!=b'1000':raise RuntimeError('monitor UID must match Brahma operator')
    remote('brahma',f'install -d -m 0700 {root}/secrets {root}/state; test -f {root}/secrets/id_ed25519 || ssh-keygen -q -t ed25519 -N "" -C rust-224-readonly-monitor -f {root}/secrets/id_ed25519')
    pub=remote('brahma',f'cat {root}/secrets/id_ed25519.pub',capture=True).decode().strip()
    if not pub.startswith('ssh-ed25519 ') or len(pub.split())!=3:
        raise RuntimeError('invalid monitor public key')
    # Installed over the existing trusted operator channel. This key can only emit a scoped snapshot.
    remote('amster-p','install -d -m 0755 /opt/polym-rust-demo; cat > /opt/polym-rust-demo/monitor_snapshot.py; chmod 0644 /opt/polym-rust-demo/monitor_snapshot.py',data=(ROOT/'scripts/monitor_snapshot.py').read_bytes())
    line='restrict,command="/usr/bin/python3 -I /opt/polym-rust-demo/monitor_snapshot.py" '+pub
    existing=remote('amster-p','cat /root/.ssh/authorized_keys',capture=True).decode()
    if line not in existing.splitlines():
        remote('amster-p','umask 077; cat >> /root/.ssh/authorized_keys; chmod 0600 /root/.ssh/authorized_keys',data=('\n'+line+'\n').encode())

    host_key=remote('amster-p','cat /etc/ssh/ssh_host_ed25519_key.pub',capture=True).decode().split()
    remote('brahma',f'cat > {root}/secrets/known_hosts; chmod 0600 {root}/secrets/known_hosts',data=('95.179.181.132 '+' '.join(host_key[:2])+'\n').encode())
    # Only the notification token and the user's existing destination leave MP, never trading credentials.
    script="""import json,subprocess
row=json.loads(subprocess.check_output(['docker','inspect','polym_amster-web-1']))[0]
e=dict(item.split('=',1) for item in row['Config']['Env'] if '=' in item)
c={'pushover_app_token':e.get('PUSHOVER_APP_TOKEN',''),'pushover_user_key':e.get('PUSHOVER_USER_KEY',''),'ssh_target':'root@95.179.181.132'}
assert len(c['pushover_app_token'])>=20 and len(c['pushover_user_key'])>=20,'Pushover credentials unavailable'
print(json.dumps(c))
"""
    secret=remote('amster-p','python3 -',data=script.encode(),capture=True)
    remote('brahma',f'umask 077; cat > {root}/secrets/config.json',data=secret)
    stage=temp('brahma')
    try:
        remote('brahma',f'umask 077; cat > {stage}/config.json',data=credentials(args.registry_env))
        remote('brahma',f'mkdir {stage}/source; tar -xf - -C {stage}/source',data=run(['git','-C',str(ROOT),'archive',sha],capture=True))
        image=REPOSITORY+'-monitor:'+sha[:12]
        remote('brahma',f'sudo -n docker --config {stage} build --label org.opencontainers.image.revision={sha} -f {stage}/source/Dockerfile.monitor -t {image} {stage}/source')
        remote('brahma',f'sudo -n docker --config {stage} push {image}')
        digests=json.loads(remote('brahma',shlex.join(['sudo','-n','docker','image','inspect',image,'--format','{{json .RepoDigests}}']),capture=True))
        pinned=next(d for d in digests if d.startswith(REPOSITORY+'-monitor@sha256:'))
        compose={'name':'rust-224-monitor','services':{'monitor':{'image':pinned,'restart':'unless-stopped','user':'1000:1000','read_only':True,'tmpfs':['/tmp:size=8m'],
            'cap_drop':['ALL'],'security_opt':['no-new-privileges:true'],'cpus':.1,'mem_limit':'96m','pids_limit':32,
            'volumes':[root+'/state:/state',root+'/secrets/id_ed25519:/run/secrets/monitor_key:ro',root+'/secrets/known_hosts:/run/secrets/known_hosts:ro',root+'/secrets/config.json:/run/secrets/monitor_config:ro'],
            'healthcheck':{'test':['CMD','python3','-c',"import json,time; x=json.load(open('/state/heartbeat.json')); assert time.time()*1000-x['at_ms']<60000"],'interval':'30s','timeout':'5s','retries':3,'start_period':'30s'},
            'logging':{'driver':'json-file','options':{'max-size':'5m','max-file':'2'}}}}}
        remote('brahma',f'cat > {root}/compose.json',data=json.dumps(compose).encode())
        remote('brahma',f'sudo -n docker compose -f {root}/compose.json up -d --wait --wait-timeout 120')
        result=remote('brahma',f'cat {root}/state/heartbeat.json',capture=True)
        print('Brahma monitor running: '+result.decode().strip())
    finally:
        remote('brahma','sudo -n rm -rf -- '+shlex.quote(stage))


if __name__=='__main__':
    main()
