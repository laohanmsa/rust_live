#!/usr/bin/env python3
"""Forced SSH command: return scoped, read-only telemetry without credentials."""
import concurrent.futures
import json
from pathlib import Path
import subprocess
import time
import urllib.request


def get(url, token=None):
    request = urllib.request.Request(url)
    if token:
        request.add_header('Authorization', 'Bearer ' + token)
    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            return json.load(response)
    except Exception as error:
        return {'error': type(error).__name__}


def main():
    token = Path('/opt/polym-rust-demo/secrets/access.token').read_text().strip()
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        health = pool.submit(get, 'http://127.0.0.1:18787/health')
        metrics = pool.submit(get, 'http://127.0.0.1:18787/metrics?window_seconds=60', token)
        uma = pool.submit(get, 'http://127.0.0.1:18788/health')
        m = metrics.result()
        if 'sources' in m:
            m['sources'].pop('recent_mock_orders', None)
            m['sources'].pop('recent_rejections', None)
        result = dict(schema_version=1, captured_at_ms=time.time_ns()//1_000_000,
                      trader=health.result(), metrics=m, uma=uma.result())
    names = ['polym-rust-demo-trader-1', 'polym-rust-demo-uma-1']
    try:
        rows = json.loads(subprocess.check_output(['docker', 'inspect', *names], timeout=5))
        result['containers'] = {row['Name'].lstrip('/'): {
            'running': row['State']['Running'], 'oom': row['State']['OOMKilled'],
            'restarts': row['RestartCount'], 'started_at': row['State']['StartedAt']
        } for row in rows}
        stats = subprocess.check_output(['docker','stats','--no-stream','--format','{{json .}}',*names], timeout=5, text=True)
        result['resources'] = [json.loads(line) for line in stats.splitlines()]
    except Exception as error:
        result['container_error'] = type(error).__name__
    disk = __import__('os').statvfs('/opt/polym-rust-demo')
    result['disk_available_bytes'] = disk.f_bavail * disk.f_frsize
    print(json.dumps(result, separators=(',', ':')))


if __name__ == '__main__':
    main()
