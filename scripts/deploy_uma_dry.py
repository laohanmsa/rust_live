#!/usr/bin/env python3
"""Build and deploy only the UMA trader; live mode requires an explicit authorized account."""
import argparse
import json
import re
import shlex
import sys
from pathlib import Path

from deploy import REPOSITORY, ROOT, credentials, remote, run, temp


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-account", choices=["airdrop_224"], help="explicitly enable authorized live trading on this account")
    args=parser.parse_args()
    mode="live" if args.live_account else "shadow"
    account=args.live_account
    compose_file="deploy/compose.uma.yaml" if account else "deploy/compose.uma-dry.yaml"
    if account:
        remote("amster-p", "sudo -n test -f /opt/polym-rust-demo/secrets/airdrop_224.json")
    if run(["git", "-C", str(ROOT), "branch", "--show-current"], capture=True).decode().strip() != "main":
        raise RuntimeError("deploy from clean main")
    if run(["git", "-C", str(ROOT), "status", "--porcelain"], capture=True).strip():
        raise RuntimeError("deploy from clean main")
    sha = run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture=True).decode().strip()
    run([sys.executable, str(ROOT / "scripts/deploy.py"), "--build-only"])
    image = f"{REPOSITORY}:{sha[:12]}"
    digests = json.loads(remote("brahma", shlex.join(["sudo", "-n", "docker", "image", "inspect", image, "--format", "{{json .RepoDigests}}"]), capture=True))
    pinned = next(d for d in digests if re.fullmatch(re.escape(REPOSITORY) + r"@sha256:[a-f0-9]{64}", d))
    before = remote("amster-p", "docker inspect polym-rust-demo-trader-1 polym-rust-demo-uma-1 --format '{{.Id}}'", capture=True)
    secret_exists = remote("amster-p", "sudo -n test -f /opt/polym-rust-uma/secrets/database-reader.json && echo yes || echo no", capture=True).decode().strip()
    if secret_exists != "yes":
        remote("amster-p", "docker exec -i polym_amster-web-1 python manage.py shell", data=(ROOT / "scripts/provision_database_reader.py").read_bytes())
        remote("amster-p", "bash -s", data=b"""set -euo pipefail
sudo -n install -d -m 0700 /opt/polym-rust-uma/secrets
sudo -n docker cp polym_amster-web-1:/tmp/rust-uma-database-reader.json /opt/polym-rust-uma/secrets/database-reader.json
sudo -n chown 10001:10001 /opt/polym-rust-uma/secrets/database-reader.json
sudo -n chmod 0400 /opt/polym-rust-uma/secrets/database-reader.json
sudo -n docker exec polym_amster-web-1 rm /tmp/rust-uma-database-reader.json
""")
    stage = temp("amster-p")
    try:
        remote("amster-p", f"umask 077; cat > {shlex.quote(stage + '/config.json')}", data=credentials(None))
        remote("amster-p", f"cat > {shlex.quote(stage + '/compose.yaml')}", data=(ROOT / compose_file).read_bytes())
        image_env=f"RUST_UMA_IMAGE={pinned}\nSOURCE_SHA={sha}\n"
        if account:
            image_env+="RUST_UMA_ACCOUNT=airdrop_224\nRUST_UMA_CREDENTIALS=/opt/polym-rust-demo/secrets/airdrop_224.json\n"
        remote("amster-p", f"cat > {shlex.quote(stage + '/image.env')}", data=image_env.encode())
        script = f"""set -euo pipefail
root=/opt/polym-rust-uma
stage={shlex.quote(stage)}
exec 9>/tmp/polym-rust-uma-deploy.lock
flock -w 60 9
sudo -n docker --config "$stage" pull {shlex.quote(pinned)}
test "$(sudo -n docker image inspect {shlex.quote(pinned)} --format '{{{{index .Config.Labels "org.opencontainers.image.revision"}}}}')" = {shlex.quote(sha)}
test "$(sudo -n docker image inspect {shlex.quote(pinned)} --format '{{{{.Os}}}}/{{{{.Architecture}}}}')" = linux/amd64
sudo -n install -d -m 0755 "$root/releases/{sha[:12]}"
if test -f "$root/image.env"; then
 sudo -n cp "$root/image.env" "$root/releases/{sha[:12]}/previous.env"
 sudo -n cp "$root/compose.yaml" "$root/releases/{sha[:12]}/previous-compose.yaml"
fi
sudo -n install -m 0644 "$stage/compose.yaml" "$root/compose.yaml"
sudo -n install -m 0644 "$stage/image.env" "$root/image.env"
sudo -n docker compose -p polym-rust-uma --env-file "$root/image.env" -f "$root/compose.yaml" {"--profile live" if account else ""} up -d --no-deps --pull never --wait --wait-timeout 180 trader-uma
curl --fail --silent http://127.0.0.1:18789/health
"""
        remote("amster-p", "bash -s", data=script.encode())
        after = remote("amster-p", "docker inspect polym-rust-demo-trader-1 polym-rust-demo-uma-1 --format '{{.Id}}'", capture=True)
        if before != after:
            raise RuntimeError("original trader or UMA service changed")
        health=json.loads(remote("amster-p", "curl --fail --silent http://127.0.0.1:18789/health", capture=True))
        if health.get("mode") != mode or health.get("account") != account or not health.get("ready"):
            raise RuntimeError("UMA mode/account readiness mismatch; retain container and journals for inspection")
        print(json.dumps({"revision": sha, "image": pinned, "mode": mode, "account": account, "original_services_unchanged": True}))
    finally:
        remote("amster-p", "sudo -n rm -rf -- " + shlex.quote(stage))


if __name__ == "__main__":
    main()
