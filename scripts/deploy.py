#!/usr/bin/env python3
"""Build/test on Brahma, push to Vultr, and roll out only the isolated demo Compose project."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys


ROOT=Path(__file__).resolve().parents[1]
REGISTRY='ams.vultrcr.com/polym'
REPOSITORY=REGISTRY+'/rust-demo'
SSH=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','-o','ServerAliveInterval=30']


def run(args, *, data=None, capture=False):
    return subprocess.run(args,input=data,check=True,stdout=subprocess.PIPE if capture else None).stdout


def remote(host, text, *, data=None, capture=False):
    return run([*SSH,host,text],data=data,capture=capture)


def log(message):
    print(datetime.now(timezone.utc).strftime('[%H:%M:%S UTC] ')+message,flush=True)


def registry_key(path):
    keys=('VULTR_REGISTRY_API_KEY','VULTR_API_KEY_REGISTRY_READ','VULTR_API_KEY')
    for key in keys:
        if os.environ.get(key): return os.environ[key]
    paths=[Path(path)] if path else [
        Path('/Users/anchen/.codex/worktrees/polym-amster-deploy/.env'),
        Path('/Users/anchen/develop/ai/polym-main-deploy/deploy/targets/local/amster-p.env'),
        Path('/Users/anchen/.codex/worktrees/new_amster_deploy/.env')]
    for p in paths:
        if not p.is_file(): continue
        values={}
        for line in p.read_text().splitlines():
            if '=' not in line or line.lstrip().startswith('#'): continue
            k,v=line.split('=',1)
            if k.strip() in keys: values[k.strip()]=v.strip().strip('\"\'')
        for k in keys:
            if values.get(k): return values[k]
    raise RuntimeError('set VULTR_REGISTRY_API_KEY or pass --registry-env; values are never printed')


def credentials(env_path):
    key=registry_key(env_path)
    def api(path,method='GET'):
        if '\n' in key or '\r' in key: raise RuntimeError('invalid registry credential format')
        config='url = '+json.dumps('https://api.vultr.com/v2/'+path)+'\nheader = '+json.dumps('Authorization: Bearer '+key)+'\n'
        result=subprocess.run(['curl','--config','-','--silent','--show-error','--fail',
            '--connect-timeout','10','--max-time','30','--retry','2','--retry-delay','1',
            '--retry-all-errors','--request',method],input=config.encode(),stdout=subprocess.PIPE,check=True)
        return json.loads(result.stdout)
    entries=api('registries')['registries']
    matches=[r for r in entries if r.get('name')=='polym' and r.get('region')=='ams']
    if len(matches)!=1: raise RuntimeError('could not uniquely resolve the existing ams/polym registry')
    result=api(f"registry/{matches[0]['id']}/docker-credentials?expiry_seconds=3600&read_write=true",'OPTIONS')
    if not isinstance(result.get('auths'),dict): raise RuntimeError('invalid temporary Docker credential response')
    return json.dumps(result).encode()


def temp(host):
    p=remote(host,'umask 077; mktemp -d /tmp/polym-rust-deploy.XXXXXXXX',capture=True).decode().strip()
    if not re.fullmatch(r'/tmp/polym-rust-deploy\.[A-Za-z0-9]+',p): raise RuntimeError('invalid temporary remote directory')
    return p


def pin_uma_image(compose, image):
    if not re.fullmatch(re.escape(REPOSITORY)+r'@sha256:[a-f0-9]{64}',image):
        raise RuntimeError('trader-only deployment requires an immutable existing UMA image')
    source=compose.decode()
    source,count=re.subn(r'(?m)(  uma:\n    image: ).*$',lambda m:m[1]+image,source)
    if count!=1: raise RuntimeError('expected exactly one existing UMA service')
    return source.encode()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run',action='store_true')
    parser.add_argument('--build-only',action='store_true')
    parser.add_argument('--live-account',choices=['airdrop_224'])
    parser.add_argument('--trader-only',action='store_true',help='update live trader while preserving UMA and monitor containers')
    parser.add_argument('--registry-env')
    args=parser.parse_args()
    if args.trader_only and not args.live_account:
        parser.error('--trader-only requires --live-account')
    mode='live' if args.live_account else 'shadow'
    sha=run(['git','-C',str(ROOT),'rev-parse','HEAD'],capture=True).decode().strip()
    branch=run(['git','-C',str(ROOT),'branch','--show-current'],capture=True).decode().strip()
    if not re.fullmatch(r'[a-f0-9]{40}',sha) or not branch: raise RuntimeError('build from an identified Git branch')
    if run(['git','-C',str(ROOT),'status','--porcelain'],capture=True).strip(): raise RuntimeError('commit changes first; deployment only archives a clean commit')
    image=f'{REPOSITORY}:{sha[:12]}'
    log(f'branch={branch} revision={sha} | build=brahma | runtime=amster-p | mode={mode}')
    log(f'image={image} | project=polym-rust-demo | limits=0.5 CPU core / 256 MiB')
    if args.dry_run: return
    creds=credentials(args.registry_env)
    stages=[]
    try:
        build=temp('brahma');stages.append(('brahma',build))
        remote('brahma',f'umask 077; cat > {shlex.quote(build+"/config.json")}',data=creds)
        archive=run(['git','-C',str(ROOT),'archive',sha],capture=True)
        remote('brahma',f'mkdir {shlex.quote(build+"/source")}; tar -xf - -C {shlex.quote(build+"/source")}',data=archive)
        log('Brahma: compile, run isolated tests and static checks, build runtime image')
        script=f'''set -Eeuo pipefail
exec 9>/tmp/polym-rust-demo-build.lock
flock -w 1800 9
cd {shlex.quote(build+'/source')}
python3 -m unittest discover -s tests -p 'test_*.py'
sudo -n docker --config {shlex.quote(build)} build --progress=plain --platform linux/amd64 \\
 --label {shlex.quote('org.opencontainers.image.revision='+sha)} \\
 --label {shlex.quote('org.opencontainers.image.ref.name='+branch)} \\
 -t {shlex.quote(image)} {shlex.quote(build+'/source')}
sudo -n docker --config {shlex.quote(build)} push {shlex.quote(image)}
'''
        remote('brahma','bash -s',data=script.encode())
        digests=json.loads(remote('brahma',shlex.join(['sudo','-n','docker','image','inspect',image,'--format','{{json .RepoDigests}}']),capture=True))
        pinned=next((d for d in digests if d.startswith(REPOSITORY+'@sha256:')),None)
        if not pinned or not re.fullmatch(re.escape(REPOSITORY)+r'@sha256:[a-f0-9]{64}',pinned): raise RuntimeError('pushed digest unavailable')
        log(f'pushed={pinned}')
        if args.build_only: return
        if args.live_account:
            exists=remote('amster-p','sudo -n test -f /opt/polym-rust-demo/secrets/airdrop_224.json && echo yes || echo no',capture=True).decode().strip()
            if exists!='yes':
                log('amster-p: provision only the selected account credentials without sending them to the build host')
                remote('amster-p','sudo -n docker exec -i polym_amster-web-1 python manage.py shell',data=(ROOT/'scripts/provision_live_credentials.py').read_bytes())
                remote('amster-p','bash -s',data=b"""set -euo pipefail
sudo -n install -d -m 0700 /opt/polym-rust-demo/secrets
sudo -n docker cp polym_amster-web-1:/tmp/rust-live-account-224.json /opt/polym-rust-demo/secrets/airdrop_224.json
sudo -n docker cp polym_amster-web-1:/tmp/rust-live-access-224.token /opt/polym-rust-demo/secrets/access.token
sudo -n chown 10001:10001 /opt/polym-rust-demo/secrets/airdrop_224.json
sudo -n chmod 0400 /opt/polym-rust-demo/secrets/airdrop_224.json /opt/polym-rust-demo/secrets/access.token
sudo -n docker exec polym_amster-web-1 rm /tmp/rust-live-account-224.json /tmp/rust-live-access-224.token
""")
        if args.live_account:
            remote('amster-p','sudo -n python3 -',data=b"""import json,os
from pathlib import Path
root=Path('/opt/polym-rust-demo/secrets')
c=json.loads((root/'airdrop_224.json').read_text())
assert c['account_name']=='airdrop_224'
fd=os.open(root/'access.token',os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o400)
os.fchmod(fd,0o400)
with os.fdopen(fd,'w') as f:f.write(c['access_token'])
""")
        if args.live_account and not args.trader_only:
            log('amster-p: provision independent UMA endpoint configuration locally')
            remote('amster-p','python3 -',data=(ROOT/'scripts/provision_uma.py').read_bytes())
        target=temp('amster-p');stages.append(('amster-p',target))
        remote('amster-p',f'umask 077; cat > {shlex.quote(target+"/config.json")}',data=creds)
        compose=(ROOT/('deploy/compose.live.yaml' if args.live_account else 'deploy/compose.yaml')).read_bytes()
        previous_revision=''
        if args.trader_only:
            previous_revision=remote('amster-p',shlex.join(['sudo','-n','docker','inspect','polym-rust-demo-trader-1','--format','{{index .Config.Labels "org.opencontainers.image.revision"}}']),capture=True).decode().strip()
            if not re.fullmatch(r'[a-f0-9]{40}',previous_revision):raise RuntimeError('missing current trader revision')
            run(['git','-C',str(ROOT),'merge-base','--is-ancestor',previous_revision,sha])
            uma_image=remote('amster-p',shlex.join(['sudo','-n','docker','inspect','polym-rust-demo-uma-1','--format','{{.Config.Image}}']),capture=True).decode().strip()
            compose=pin_uma_image(compose,uma_image)
        remote('amster-p',f'cat > {shlex.quote(target+"/compose.yaml")}',data=compose)
        remote('amster-p',f'cat > {shlex.quote(target+"/image.env")}',data=f'DEMO_IMAGE={pinned}\nSOURCE_SHA={sha}\n'.encode())
        release=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+sha[:12]
        log('amster-p: pull immutable image and update the isolated Compose service')
        script=r'''set -Eeuo pipefail
root=/opt/polym-rust-demo
stage=@STAGE@
image=@IMAGE@
revision=@SHA@
release=@RELEASE@
exec 9>/tmp/polym-rust-demo-deploy.lock
flock -w 60 9
if test @TRADER_ONLY@ = yes; then
 test "$(sudo -n docker inspect polym-rust-demo-trader-1 --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" = @PREVIOUS_REVISION@
fi
sudo -n docker --config "$stage" pull "$image"
test "$(sudo -n docker image inspect "$image" --format '{{.Os}}/{{.Architecture}}')" = linux/amd64
test "$(sudo -n docker image inspect "$image" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" = "$revision"
sudo -n install -d -m 0755 "$root/releases/$release"
previous=0
if test -f "$root/image.env"; then
 previous=1
 sudo -n cp "$root/image.env" "$root/releases/$release/previous.env"
 sudo -n cp "$root/compose.yaml" "$root/releases/$release/previous-compose.yaml"
fi
sudo -n install -m 0644 "$stage/image.env" "$root/image.env"
sudo -n install -m 0644 "$stage/compose.yaml" "$root/compose.yaml"
sudo -n cp "$root/image.env" "$root/releases/$release/image.env"
sudo -n cp "$root/compose.yaml" "$root/releases/$release/compose.yaml"
compose() { sudo -n docker compose -p polym-rust-demo --env-file "$root/image.env" -f "$root/compose.yaml" "$@"; }
verify() {
 if test @MODE@ = live && test @TRADER_ONLY@ = no; then
  compose up -d --no-deps --pull never --wait --wait-timeout 600 uma || return 1
 fi
 compose up -d --no-deps --pull never --wait --wait-timeout 120 trader || return 1
 cid=$(compose ps -q trader)
 test "$(sudo -n docker inspect "$cid" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" = "$revision" || return 1
 python3 - <<'READY' || return 1
import json,time,urllib.request
for _ in range(90):
    try:
        with urllib.request.urlopen("http://127.0.0.1:18787/health",timeout=5) as response:
            state=json.load(response)
        if state.get("mode")=="@MODE@" and state.get("ready"):
            break
    except OSError:
        pass
    time.sleep(1)
else:
    raise SystemExit("input/account/context readiness timed out")
READY
}
if ! verify; then
 echo 'ERROR new deployment failed verification' >&2
 if test @MODE@ = live; then
  echo 'Live container retained for audit; do not silently roll back after possible real submission' >&2
  exit 1
 fi
 if test "$previous" = 1; then
  sudo -n cp "$root/releases/$release/previous.env" "$root/image.env"
  sudo -n cp "$root/releases/$release/previous-compose.yaml" "$root/compose.yaml"
  compose up -d --no-deps --pull never --wait --wait-timeout 120 trader
  echo 'Restored previous demo image/configuration; data volume preserved' >&2
 fi
 exit 1
fi
compose ps
'''
        for k,v in {'@STAGE@':target,'@IMAGE@':pinned,'@SHA@':sha,'@RELEASE@':release,'@MODE@':mode,'@TRADER_ONLY@':'yes' if args.trader_only else 'no','@PREVIOUS_REVISION@':previous_revision}.items(): script=script.replace(k,shlex.quote(v))
        remote('amster-p','bash -s',data=script.encode())
        log('deployment verified; reading timings and resource usage')
        run([sys.executable,str(ROOT/'scripts/observe.py'),'--seconds','5'])
        if args.live_account and not args.trader_only:
            log('Brahma: deploy read-only monitoring and Pushover delivery')
            command=[sys.executable,str(ROOT/'scripts/deploy_monitor.py')]
            if args.registry_env:command.extend(['--registry-env',args.registry_env])
            run(command)
    finally:
        for host,path in reversed(stages):
            try: remote(host,'sudo -n rm -rf -- '+shlex.quote(path))
            except subprocess.CalledProcessError: print(f'WARNING cleanup failed on {host}; remove only {path}',file=sys.stderr)

if __name__=='__main__':
    try: main()
    except Exception as error:
        print(f'ERROR: {error}',file=sys.stderr);sys.exit(1)
