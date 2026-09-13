"""Run inside MP's existing Django container; never on a build host."""
import json
import logging
import os
from decimal import Decimal
from pathlib import Path
from django.core import signing
from airdrop.models import AirdropAccount
from airdrop.polymarket_clob import ClobClient
from airdrop.services import PolymarketService

logging.disable(logging.CRITICAL)
account_name = 'airdrop_224'
try:
    account = AirdropAccount.objects.get(name=account_name)
    assert account.is_active and account.proxy_wallet, 'account is not ready'
    snapshots = PolymarketService().fetch_v2_cash_snapshot_batch({account_name: account.trading_wallet})
    snapshot = snapshots.get(account_name)
    assert snapshot is not None, 'live cash check unavailable'
    assert min(snapshot.trading_pusd, snapshot.pusd_ctf_exchange_allowance, snapshot.pusd_neg_risk_exchange_allowance) >= Decimal('10'), 'live cash or allowance insufficient'
    client = ClobClient(host='https://clob.polymarket.com', key=account.private_key, chain_id=137, signature_type=2, funder=account.proxy_wallet)
    creds = client.create_or_derive_api_key()
    token = signing.dumps({'account_id':account.pk,'account_name':account.name}, salt='rust-live-orders-v1')
    payload = dict(account_name=account.name, account_id=account.pk, signer_address=account.address, funder=account.proxy_wallet,
        private_key=account.private_key, api_key=creds.api_key, api_secret=creds.api_secret, api_passphrase=creds.api_passphrase, access_token=token)
    for filename, content in [('/tmp/rust-live-account-224.json',json.dumps(payload)),('/tmp/rust-live-access-224.token',token)]:
        fd=os.open(filename,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
        os.fchmod(fd,0o600)
        with os.fdopen(fd,'w') as file:file.write(content)
    print(json.dumps({'account':account.name,'account_id':account.pk,'live_balance':str(snapshot.trading_pusd),'signature_type':2,'credential_files_ready':True}))
except Exception as error:
    raise SystemExit('Live credential preparation failed: '+type(error).__name__) from None
