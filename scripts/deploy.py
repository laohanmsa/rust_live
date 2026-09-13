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
import urllib.request

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
        req=urllib.request.Request('https://api.vultr.com/v2/'+path,method=method,headers={'Authorization':'Bearer '+key})
        with urllib.request.urlopen(req,timeout=30) as response: return json.load(response)
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


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run',action='store_true')
    parser.add_argument('--build-only',action='store_true')
    parser.add_argument('--registry-env')
    args=parser.parse_args()
    sha=run(['git','-C',str(ROOT),'rev-parse','HEAD'],capture=True).decode().strip()
    branch=run(['git','-C',str(ROOT),'branch','--show-current'],capture=True).decode().strip()
    if not re.fullmatch(r'[a-f0-9]{40}',sha) or not branch: raise RuntimeError('build from an identified Git branch')
    if run(['git','-C',str(ROOT),'status','--porcelain'],capture=True).strip(): raise RuntimeError('commit changes first; deployment only archives a clean commit')
    image=f'{REPOSITORY}:{sha[:12]}'
    log(f'branch={branch} revision={sha} | build=brahma | runtime=amster-p | mode=demo')
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
        target=temp('amster-p');stages.append(('amster-p',target))
        remote('amster-p',f'umask 077; cat > {shlex.quote(target+"/config.json")}',data=creds)
        remote('amster-p',f'cat > {shlex.quote(target+"/compose.yaml")}',data=(ROOT/'deploy/compose.yaml').read_bytes())
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
 compose up -d --no-deps --pull never --wait --wait-timeout 120 trader || return 1
 cid=$(compose ps -q trader)
 test "$(sudo -n docker inspect "$cid" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" = "$revision" || return 1
 python3 -c 'import json,urllib.request; d=json.load(urllib.request.urlopen("http://127.0.0.1:18787/health",timeout=5)); assert d["mode"]=="demo" and d["ready"],d' || return 1
}
if ! verify; then
 echo 'ERROR new demo deployment failed verification' >&2
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
        for k,v in {'@STAGE@':target,'@IMAGE@':pinned,'@SHA@':sha,'@RELEASE@':release}.items(): script=script.replace(k,shlex.quote(v))
        remote('amster-p','bash -s',data=script.encode())
        log('deployment verified; reading timings and resource usage')
        run([sys.executable,str(ROOT/'scripts/observe.py'),'--seconds','5'])
    finally:
        for host,path in reversed(stages):
            try: remote(host,'sudo -n rm -rf -- '+shlex.quote(path))
            except subprocess.CalledProcessError: print(f'WARNING cleanup failed on {host}; remove only {path}',file=sys.stderr)

if __name__=='__main__':
    try: main()
    except Exception as error:
        print(f'ERROR: {error}',file=sys.stderr);sys.exit(1)
