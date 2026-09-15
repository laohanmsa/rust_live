import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from monitor import issues, transitions, delivered, ssh_command

class MonitorTest(unittest.TestCase):
    def snapshot(self):
        return dict(schema_version=1,captured_at_ms=1000000,trader=dict(ready=True,stopped=False),
            metrics=dict(sources=dict(nats_connected=True),history=dict(pending=0,unresolved=0)),
            uma=dict(ready=True),containers={},resources=[])

    def test_hard_halt_and_quiet_trading(self):
        s=self.snapshot()
        self.assertEqual(issues(s,1000),{})  # No fills alone is not an incident.
        s['trader']=dict(stopped=True,ready=False,stop_reason='lifecycle_capacity')
        found=issues(s,1000);state={}
        notices=transitions(state,found,1000)
        self.assertEqual([n[0] for n in notices],['trading_stopped'])
        delivered(state,notices,1000)
        self.assertEqual(transitions(state,found,1015),[])
        self.assertEqual(transitions(state,{},1020),[])
        recovery=transitions(state,{},1050)
        self.assertEqual(recovery[0][1],'recovery')
        delivered(state,recovery,1050)
        self.assertEqual(state['incidents'],{})

    def test_host_outage_debounces_and_failed_delivery_retries(self):
        state={};found=issues({},1000)
        self.assertEqual(transitions(state,found,1000),[])
        alarm=transitions(state,found,1030)
        self.assertEqual(alarm[0][0],'mp_unreachable')
        self.assertEqual(transitions(state,found,1045),alarm) # Not marked sent before acknowledgement.
        delivered(state,alarm,1045)
        self.assertEqual(transitions(state,found,1060),[])

    def test_intermittent_failures_do_not_accumulate_into_a_continuous_outage(self):
        state={};found=issues({},1000)
        self.assertEqual(transitions(state,found,1000),[])
        self.assertEqual(transitions(state,{},1015),[])
        self.assertEqual(transitions(state,found,1030),[])
        self.assertEqual(transitions(state,found,1045),[])
        self.assertEqual(transitions(state,found,1060)[0][0],'mp_unreachable')

    def test_relay_keeps_host_verification_and_rejects_shell_text(self):
        config={'ssh_target':'root@95.179.181.132','ssh_relay':'root@70.34.203.243'}
        command=ssh_command(config)
        proxy=next(v for v in command if v.startswith('ProxyCommand='))
        self.assertIn('StrictHostKeyChecking=yes',proxy)
        self.assertEqual(command[-1],config['ssh_target'])
        config['ssh_relay']='root@70.34.203.243; echo unsafe'
        with self.assertRaises(ValueError):ssh_command(config)

    def test_journal_alert_uses_reported_capacity(self):
        s=self.snapshot()
        s['metrics']['history'].update(journal_entries=50000,journal_capacity=1000000)
        self.assertNotIn('journal_capacity',issues(s,1000))
        s['metrics']['history']['journal_entries']=800000
        self.assertIn('journal_capacity',issues(s,1000))

    def test_stale_uma_and_unknown_orders_are_detected(self):
        s=self.snapshot();s['uma']['scan_age_ms']=11000
        s['metrics']['history']['unresolved']=1
        found=issues(s,1000)
        self.assertIn('uma_unhealthy',found)
        self.assertEqual(found['unknown_order'][0],0)

if __name__=='__main__':unittest.main()
