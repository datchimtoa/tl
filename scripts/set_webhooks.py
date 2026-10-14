"""Register both bots after deployment: python -m scripts.set_webhooks."""

import argparse
import json
import os
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from api.index import webhook_secret


def telegram_request(token, method, payload):
  request = Request(
      f'https://api.telegram.org/bot{token}/{method}',
      data=json.dumps(payload).encode('utf-8'),
      headers={'Content-Type': 'application/json'}, method='POST',
  )
  with urlopen(request, timeout=30) as response:
    result = json.load(response)
  if not result.get('ok'):
    raise RuntimeError('Telegram did not accept the request.')
  return result.get('result')


def main():
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('--info', action='store_true', help='Show current webhook status without changing it.')
  parser.add_argument('--drop-pending-updates', action='store_true', help='Explicitly discard queued updates.')
  args = parser.parse_args()
  try:
    tokens = {name: os.environ[f'{name.upper()}_TOKEN'] for name in ('shop', 'admin')}
    if not all(tokens.values()):
      raise ValueError('SHOP_TOKEN and ADMIN_TOKEN must not be empty.')
    if not args.info:
      secret = webhook_secret()
      base = os.environ['WEBHOOK_BASE_URL'].rstrip('/')
      parsed = urlsplit(base)
      if (parsed.scheme != 'https' or not parsed.netloc or parsed.path
          or parsed.query or parsed.fragment or parsed.username or parsed.password):
        raise ValueError('WEBHOOK_BASE_URL must be a public HTTPS origin, without a path.')
    for name, token in tokens.items():
      if not args.info:
        telegram_request(token, 'setWebhook', {
            'url': f'{base}/api/webhook/{name}',
            'secret_token': secret,
            'allowed_updates': ['message', 'callback_query'],
            'max_connections': 1,
            'drop_pending_updates': args.drop_pending_updates,
        })
        if name == 'shop':
          telegram_request(token, 'setMyCommands', {'commands': [
              {'command': 'start', 'description': 'Mở Cửa Hàng'},
              {'command': 'link', 'description': 'Cách nuôi acc'},
          ]})
      status = telegram_request(token, 'getWebhookInfo', {})
      print(f"{name}: url={status.get('url', '')}, pending={status.get('pending_update_count', 0)}")
      if status.get('last_error_message'):
        print(f"{name}: Telegram reports a delivery error; check getWebhookInfo and Vercel logs.")
  except Exception as exc:
    # Network exception messages can include the bot token in the request URL.
    raise SystemExit(f'Webhook setup failed ({type(exc).__name__}). Check environment variables and connectivity.') from None


if __name__ == '__main__':
  main()