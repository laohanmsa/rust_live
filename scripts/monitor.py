#!/usr/bin/env python3
"""Read-only off-host monitor. Incident detection, durable dedupe and Pushover delivery."""
import argparse
import json
import ipaddress
import shlex
import os
from pathlib import Path
import subprocess
import time
import urllib.request


def issues(snapshot, now):
    out = {}
    if snapshot.get('schema_version') != 1 or abs(now*1000-snapshot.get('captured_at_ms', 0)) > 45000:
        return {'mp_unreachable': (30, '无法读取 MP 的实时状态')}
    trader, metrics, uma = (snapshot.get(k, {}) for k in ('trader','metrics','uma'))
    sources, history = metrics.get('sources', {}), metrics.get('history', {})
    if trader.get('stopped') is True:
        out['trading_stopped'] = (0, 'Rust 策略已暂停: '+str(trader.get('stop_reason') or '原因未记录'))
    if trader.get('ready') is not True and trader.get('stopped') is not True:
        out['trader_not_ready'] = (45, 'Rust 尚未满足数据或账户就绪条件')
    if 'error' in metrics or not sources or not history:
        out['metrics_unavailable'] = (30, '交易监测数据读取失败')
    if sources.get('nats_connected') is not True:
        out['ober_disconnected'] = (15, 'OBer 消息连接中断')
    if now*1000-sources.get('last_ober_ms', now*1000) > 120000 and sources.get('eligible_markets',0)>0:
        out['ober_silent'] = (30, '存在候选市场但超过两分钟未收到 OBer 消息，请检查上游')
    if sources.get('context_age_ms',0) > 90000:
        out['context_stale'] = (0, 'Django 市场资料已过期')
    if history.get('unresolved',0):
        out['unknown_order'] = (0, '存在结果未知的提交，需要先核对成交，不能直接重启重发')
    if history.get('pending',0) or history.get('error'):
        out['history_backlog'] = (60, '订单历史同步持续积压或失败')
    if history.get('journal_entries',0) >= .8 * history.get('journal_capacity',50000):
        out['journal_capacity'] = (0, '订单日志已达到容量的 80%，需要归档维护')
    if uma.get('ready') is not True or uma.get('scan_age_ms',0)>10000 or uma.get('head_age_ms',0)>15000:
        out['uma_unhealthy'] = (30, '独立 UMA 监听或补抓未就绪: '+str(uma.get('fault') or uma.get('error') or '链上数据过期'))
    if uma.get('ws') and not any(w.get('connected') and now*1000-w.get('last_message_ms',0)<15000 for w in uma['ws']):
        out['uma_ws_unavailable'] = (30, '两路 UMA 实时连接均不可用，正在依赖日志扫描')
    if uma.get('events_cached',0) >= .8 * uma.get('event_capacity',100000):
        out['uma_capacity'] = (0, 'UMA 缓存达到容量的 80%')
    for name, container in snapshot.get('containers',{}).items():
        if not container.get('running') or container.get('oom'):
            out['container_'+name] = (15, name+' 未运行或因内存不足退出')
    if snapshot.get('container_error'):
        out['containers_unavailable'] = (30, '容器状态读取失败')
    for resource in snapshot.get('resources',[]):
        try:
            if float(resource['MemPerc'].rstrip('%')) >= 85:
                out['memory_'+resource['Name']] = (30, resource['Name']+' 内存使用达到限制的 85%')
        except (KeyError,ValueError):
            out['resources_invalid'] = (30, '容器资源数据格式异常')
    if snapshot.get('disk_available_bytes',10**12)<2*1024**3:
        out['disk_low'] = (30, 'MP 可用磁盘不足 2 GB')
    return out


def transitions(state, found, now):
    active = state.setdefault('incidents', {})
    notices = []
    for code, (delay, message) in found.items():
        row = active.setdefault(code, dict(first_seen=now, notified=False, last_sent=0))
        recovered=row.pop('healthy_since', None)
        if recovered is not None and not row['notified']:
            row['first_seen']=now
        row['message'] = message
        if now-row['first_seen'] >= delay and (not row['notified'] or now-row['last_sent'] >= 1800):
            notices.append((code, 'alert', message))
    for code, row in list(active.items()):
        if code in found:
            continue
        row.setdefault('healthy_since', now)
        if now-row['healthy_since'] >= 30:
            if row['notified']:
                notices.append((code, 'recovery', row['message']))
            else:
                del active[code]
    return notices


def delivered(state, notices, now):
    for code, kind, _ in notices:
        if kind == 'recovery':
            state['incidents'].pop(code, None)
        else:
            state['incidents'][code].update(notified=True, last_sent=now)


def save(path, value):
    temporary = path.with_suffix('.tmp')
    with temporary.open('w') as f:
        json.dump(value, f); f.flush(); os.fsync(f.fileno())
    temporary.replace(path)


def push(config, title, message, priority=0):
    data = json.dumps(dict(token=config['pushover_app_token'], user=config['pushover_user_key'],
        title=title, message=message[:1000], priority=priority)).encode()
    request=urllib.request.Request('https://api.pushover.net/1/messages.json', data=data,
                                  headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(request, timeout=10) as response:
        result=json.load(response)
    if result.get('status') != 1:
        raise RuntimeError('Pushover did not acknowledge delivery')


def ssh_command(config):
    base=['ssh','-T','-i','/run/secrets/monitor_key','-o','BatchMode=yes','-o','IdentitiesOnly=yes',
        '-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile=/run/secrets/known_hosts',
        '-o','ConnectTimeout=8','-o','ServerAliveInterval=5','-o','ServerAliveCountMax=1']
    command=list(base)
    relay=config.get('ssh_relay')
    if relay:
        user,host=relay.split('@',1)
        if user!='root':raise ValueError('unsupported relay user')
        ipaddress.IPv4Address(host)
        command.extend(['-o','ProxyCommand='+shlex.join(base+[relay])])
    return command+[config['ssh_target']]


def read_snapshot(config):
    command=ssh_command(config)
    try:
        result=subprocess.run(command,capture_output=True,timeout=20,check=True)
        if len(result.stdout)>1024*1024:
            raise ValueError('snapshot size limit')
        return json.loads(result.stdout)
    except subprocess.TimeoutExpired:
        return {'monitor_read_error':'snapshot_timeout'}
    except subprocess.CalledProcessError as error:
        return {'monitor_read_error':'ssh_failed','ssh_exit_code':error.returncode}
    except Exception as error:
        return {'monitor_read_error':type(error).__name__}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',default='/run/secrets/monitor_config')
    parser.add_argument('--state-dir',default='/state')
    parser.add_argument('--test-push',action='store_true')
    parser.add_argument('--once',action='store_true')
    args=parser.parse_args()
    config=json.loads(Path(args.config).read_text())
    if args.test_push:
        push(config,'airdrop_224 监控测试','这是监控上线测试。Brahma 上的独立监控已能发送 Pushover；该消息不表示交易发生故障。')
        print(json.dumps({'event':'test_push_accepted'}),flush=True)
        return
    root=Path(args.state_dir);root.mkdir(parents=True,exist_ok=True)
    path=root/'state.json';state=json.loads(path.read_text()) if path.exists() else {}
    while True:
        started=time.monotonic();snapshot=read_snapshot(config);now=time.time()
        found=issues(snapshot,now);notices=transitions(state,found,now)
        if notices and now-state.get('last_delivery_attempt',0)>=60:
            state['last_delivery_attempt']=now
            message='\n'.join(('恢复: ' if kind=='recovery' else '异常: ')+text for _,kind,text in notices)
            try:
                push(config,'airdrop_224 运行监控',message,1 if any(code in ['trading_stopped','unknown_order','mp_unreachable'] and kind=='alert' for code,kind,_ in notices) else 0)
                delivered(state,notices,now)
                entry=dict(at_ms=int(now*1000),event='notification_accepted',codes=[(c,k) for c,k,_ in notices])
            except Exception as error:
                entry=dict(at_ms=int(now*1000),event='notification_failed',error=type(error).__name__)
            log=root/'alerts.jsonl'
            if log.exists() and log.stat().st_size>5*1024*1024:
                log.replace(root/'alerts.previous.jsonl')
            with log.open('a') as f:
                f.write(json.dumps(entry)+'\n');f.flush();os.fsync(f.fileno())
            print(json.dumps(entry),flush=True)
        save(path,state)
        save(root/'last-snapshot.json',snapshot)
        save(root/'heartbeat.json',dict(at_ms=int(time.time()*1000),issues=sorted(found),monitor='running'))
        print(json.dumps(dict(event='monitor_check',at_ms=int(now*1000),issues=sorted(found))),flush=True)
        if args.once:
            return
        time.sleep(max(0,15-(time.monotonic()-started)))


if __name__=='__main__':
    main()
