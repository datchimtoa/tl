"""Vercel's standard-library Python HTTP function; no polling or worker threads."""

import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import os
import re
from urllib.parse import parse_qs, urlsplit

from tl.storage import StorageBusy, StorageError

logger = logging.getLogger(__name__)
MAX_BODY_BYTES = 1024 * 1024


def webhook_secret():
  secret = os.environ.get('WEBHOOK_SECRET', '')
  if not re.fullmatch(r'[A-Za-z0-9_-]{1,256}', secret):
    raise ValueError('Configure WEBHOOK_SECRET using Telegram-supported characters.')
  return secret


class handler(BaseHTTPRequestHandler):
  def respond(self, status, payload, headers=None, head=False):
    body = json.dumps(payload).encode('utf-8')
    self.send_response(status)
    self.send_header('Content-Type', 'application/json; charset=utf-8')
    self.send_header('Content-Length', str(len(body)))
    self.send_header('Cache-Control', 'no-store')
    for name, value in (headers or {}).items():
      self.send_header(name, value)
    self.end_headers()
    if not head:
      self.wfile.write(body)

  def do_GET(self, head=False):
    if urlsplit(self.path).path.rstrip('/') not in ('', '/api', '/api/health', '/api/index', '/api/index.py'):
      self.respond(404, {'ok': False, 'error': 'Not found'}, head=head)
      return
    # Liveness only: does not import the bot, expose secrets, or call Telegram.
    self.respond(200, {'ok': True, 'service': 'telegram-webhooks'}, head=head)

  def do_HEAD(self):
    self.do_GET(head=True)

  def do_POST(self):
    parsed = urlsplit(self.path)
    path = parsed.path.rstrip('/')
    bot_name = {
        '/api/webhook/shop': 'shop', '/api/webhook/admin': 'admin',
    }.get(path)
    if path in ('/api', '/api/index', '/api/index.py'):
      bot_name = parse_qs(parsed.query).get('bot', [''])[0]
    if bot_name not in ('shop', 'admin'):
      self.respond(404, {'ok': False, 'error': 'Not found'})
      return
    try:
      expected_secret = webhook_secret()
    except ValueError:
      self.respond(503, {'ok': False, 'error': 'Webhook secret is not configured'})
      return
    supplied = self.headers.get('X-Telegram-Bot-Api-Secret-Token', '')
    if not hmac.compare_digest(supplied.encode('utf-8'), expected_secret.encode('utf-8')):
      self.respond(403, {'ok': False, 'error': 'Forbidden'})
      return
    if self.headers.get_content_type() != 'application/json':
      self.respond(415, {'ok': False, 'error': 'Content-Type must be application/json'})
      return
    try:
      length = int(self.headers.get('Content-Length', '0'))
    except ValueError:
      self.respond(400, {'ok': False, 'error': 'Invalid Content-Length'})
      return
    if length <= 0 or length > MAX_BODY_BYTES:
      self.respond(413 if length > MAX_BODY_BYTES else 400, {'ok': False, 'error': 'Invalid body size'})
      return
    try:
      payload = json.loads(self.rfile.read(length))
      if not isinstance(payload, dict) or type(payload.get('update_id')) is not int:
        raise ValueError('An integer update_id is required.')
      if payload['update_id'] < 0:
        raise ValueError('update_id must not be negative.')
      from telebot.types import Update
      update = Update.de_json(payload)
    except (ValueError, TypeError, KeyError, AttributeError):
      self.respond(400, {'ok': False, 'error': 'Invalid Telegram update'})
      return
    try:
      # Lazy import keeps health checks and rejected requests independent of bot config.
      from tl.tl import process_webhook
      processed = process_webhook(bot_name, update)
    except (StorageBusy, StorageError):
      self.respond(503, {'ok': False, 'error': 'Storage temporarily unavailable'}, {'Retry-After': '2'})
      return
    except (KeyError, ValueError, ImportError):
      logger.error('Bot initialization or update handling failed; check configuration and dependencies.')
      self.respond(500, {'ok': False, 'error': 'Bot configuration or update handling failed'})
      return
    except Exception:
      # Do not log Telegram API exception text: it may contain credentials or user data.
      logger.error('Webhook processing failed.')
      self.respond(500, {'ok': False, 'error': 'Update processing failed'})
      return
    self.respond(200, {'ok': True, 'duplicate': not processed})


if __name__ == '__main__':
  port = int(os.environ.get('PORT', '8000'))
  print(f'Webhook server listening on http://127.0.0.1:{port}')
  ThreadingHTTPServer(('127.0.0.1', port), handler).serve_forever()