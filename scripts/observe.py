#!/usr/bin/env python3
"""Read one prototype's timing, outcomes and container resources in one call."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import re
import shlex
import statistics
import subprocess
import sys
import threading
import time
import urllib.request
import uuid

ROOT = '/opt/polym-rust-demo'
URL = 'http://127.0.0.1:18787'
TOKEN = 'demo-local-only'


def command(*args):
    return subprocess.check_output(args, text=True, timeout=15).strip()


def get(path):
    req = urllib.request.Request(URL + path, headers={'Authorization': 'Bearer ' + TOKEN})
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.load(response)


def memory_mib(value):
    number, unit = re.fullmatch(r'([0-9.]+)\s*([A-Za-z]+)', value.strip()).groups()
    return float(number) * {'B': 1/1048576, 'kB': 1000/1048576, 'KB': 1000/1048576,
                          'KiB': 1/1024, 'MB': 1000000/1048576, 'MiB': 1,
                          'GB': 1000000000/1048576, 'GiB': 1024}[unit]


def demo_load(count, result):
    def send(index):
        payload = dict(id=f'observe-{uuid.uuid4().hex}-{index}', token_id='1', ask='0.50',
                       fair_value='0.60', observed_at_ms=time.time_ns()//1000000, book_valid=True)
        req = urllib.request.Request(URL+'/signal', data=json.dumps(payload).encode(),
            headers={'Authorization': 'Bearer '+TOKEN, 'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                return json.load(response)['state']
        except Exception:
            return 'request_error_or_rejection'
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = []
        for index in range(count):
            futures.append(pool.submit(send, index))
            time.sleep(.01)
        result.update(Counter(f.result() for f in futures))


def collect(args):
    cid = command('sudo', '-n', 'docker', 'compose', '-p', 'polym-rust-demo',
                  '--env-file', ROOT+'/image.env', '-f', ROOT+'/compose.yaml', 'ps', '-q', 'trader')
    if not re.fullmatch(r'[a-f0-9]{12,64}', cid):
        raise RuntimeError('prototype container is not running')
    info = json.loads(command('sudo', '-n', 'docker', 'inspect', cid))[0]
    health = get('/health')
    exercise = {}
    load = None
    if args.exercise_demo:
        if health.get('mode') != 'demo' or not health.get('ready'):
            raise RuntimeError('exercise requires a ready demo, never live mode')
        load = threading.Thread(target=demo_load, args=(args.exercise_demo, exercise))
        load.start()
    samples = []
    started = time.monotonic()
    while True:
        row = json.loads(command('sudo', '-n', 'docker', 'stats', '--no-stream', '--format', '{{json .}}', cid))
        samples.append(dict(cpu_percent=float(row['CPUPerc'].rstrip('%')),
                            memory_mib=memory_mib(row['MemUsage'].split('/')[0]),
                            pids=int(row['PIDs']), network=row['NetIO'], disk_io=row['BlockIO']))
        if time.monotonic()-started >= args.seconds:
            break
    if load:
        load.join(timeout=15)
        if load.is_alive():
            raise RuntimeError('demo exercise did not finish in time')
    metrics = get(f'/metrics?window_seconds={args.window}')
    elapsed = time.monotonic()-started
    host = dict(line.split(':',1) for line in Path('/proc/meminfo').read_text().splitlines())
    result = dict(host='amster-p', collected_at_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        mode=health['mode'], health=get('/health'), image=info['Config']['Image'],
        revision=info['Config'].get('Labels',{}).get('org.opencontainers.image.revision'),
        container_state=info['State']['Status'], restarts=info['RestartCount'],
        limits=dict(cpu_cores=info['HostConfig']['NanoCpus']/1e9,
                    memory_mib=info['HostConfig']['Memory']/1048576,
                    pids=info['HostConfig']['PidsLimit']),
        resources=dict(seconds=round(elapsed,2),samples=len(samples),
            cpu_percent_of_one_core_avg=round(statistics.mean(s['cpu_percent'] for s in samples),3),
            cpu_percent_of_one_core_max=max(s['cpu_percent'] for s in samples),
            memory_mib_avg=round(statistics.mean(s['memory_mib'] for s in samples),3),
            memory_mib_max=max(s['memory_mib'] for s in samples),pids_max=max(s['pids'] for s in samples),
            network_cumulative=samples[-1]['network'],disk_io_cumulative=samples[-1]['disk_io'],
            journal_bytes=int(command('sudo','-n','docker','exec',cid,'du','-sb','/app/data').split()[0]),
            host_available_memory_mib=int(host['MemAvailable'].split()[0])/1024),
        metrics=metrics,exercise_demo=dict(requested=args.exercise_demo,outcomes=exercise))
    return result


def render(data):
    m, r, limits = data['metrics'], data['resources'], data['limits']
    print(f"{data['host']} | {data['mode']} | ready={data['health']['ready']} | revision={data['revision'][:12]}")
    print(f"Resource sample: {r['seconds']}s, {r['samples']} samples")
    print(f"CPU: average {r['cpu_percent_of_one_core_avg']}%, max {r['cpu_percent_of_one_core_max']}% of one core; limit {limits['cpu_cores']} core")
    print(f"Memory: average {r['memory_mib_avg']:.2f} MiB, max {r['memory_mib_max']:.2f} MiB; limit {limits['memory_mib']} MiB; processes/threads max {r['pids_max']}")
    print(f"Journal: {r['journal_bytes']} bytes | restarts: {data['restarts']} | network/disk are cumulative container counters")
    print(f"Since boot: received={m['received']}, completed={m['completed']}, replayed={m['replayed']}; queue={m['queued']}, active={m['active']}")
    print(f"Observation window: {m['window_seconds']}s; retained={m['window_samples']}/{m['sample_capacity']}; truncated={m['window_truncated']}")
    print('Response states (including replays): '+json.dumps(m['states'])+' | Reasons: '+json.dumps(m['reasons']))
    print(f"{'Stage (milliseconds)':26} {'n':>5} {'p50':>10} {'p95':>10} {'p99':>10} {'max':>10}")
    for stage, row in m['latency_ms'].items():
        fmt=lambda x: '-' if x is None else f'{x:.3f}'
        print(f"{stage:26} {row['n']:5} "+' '.join(f'{fmt(row[k]):>10}' for k in ('p50','p95','p99','max')))
    print('Dispatch: handler entry to application request submission. Source age uses wall clocks. Replays are excluded from latency. No samples are shown as missing, never zero.')
    if data['exercise_demo']['requested']:
        print('Explicit mock exercise: '+json.dumps(data['exercise_demo']))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--host',default='amster-p')
    p.add_argument('--seconds',type=int,default=5)
    p.add_argument('--window',type=int,default=3600)
    p.add_argument('--json',action='store_true')
    p.add_argument('--exercise-demo',type=int,default=0,help='explicitly submit up to 100 mock signals; never live')
    p.add_argument('--local',action='store_true',help=argparse.SUPPRESS)
    args=p.parse_args()
    if not 1<=args.seconds<=30 or not 1<=args.window<=86400 or not 0<=args.exercise_demo<=100:
        p.error('seconds: 1..30; window: 1..86400; exercise-demo: 0..100')
    if args.local:
        data=collect(args)
    else:
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*',args.host):
            p.error('invalid SSH host')
        remote=shlex.join(['python3','-','--local','--json','--seconds',str(args.seconds),'--window',str(args.window),'--exercise-demo',str(args.exercise_demo)])
        run=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10',args.host,remote],
            input=Path(__file__).read_text(),text=True,capture_output=True,timeout=args.seconds+50)
        if run.returncode:
            raise RuntimeError(run.stderr.strip() or 'remote observation failed')
        data=json.loads(run.stdout)
    if args.json: print(json.dumps(data,indent=2))
    else: render(data)

if __name__=='__main__':
    try: main()
    except Exception as error: print(f'ERROR: {error}',file=sys.stderr);sys.exit(1)
