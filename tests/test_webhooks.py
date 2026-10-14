import base64
import http.client
from http.server import ThreadingHTTPServer
import importlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from api.index import handler
from scripts import set_webhooks
from scripts.migrate_data import migrate
from tl.storage import StateStore, StorageBusy, StorageError

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = ROOT / '.runtime'
SCRATCH.mkdir(exist_ok=True)
TEST_ENV = {
    'SHOP_TOKEN': '123456:TEST_SHOP', 'ADMIN_TOKEN': '654321:TEST_ADMIN',
    'ADMIN_ID': '99', 'WEBHOOK_SECRET': 'test-secret',
    'VERCEL': '', 'UPSTASH_REDIS_REST_URL': '', 'UPSTASH_REDIS_REST_TOKEN': '',
    'BOT_STORAGE_KEY': 'test:state',
}


def message_update(update_id, text, uid=42):
  payload = {
      'update_id': update_id,
      'message': {
          'message_id': update_id, 'date': 1700000000,
          'chat': {'id': uid, 'type': 'private'},
          'from': {'id': uid, 'is_bot': False, 'first_name': 'Test'},
          'text': text,
      },
  }
  if text.startswith('/'):
    payload['message']['entities'] = [{'type': 'bot_command', 'offset': 0, 'length': len(text)}]
  return payload


class MemoryRedisStore(StateStore):
  """Test double for Redis commands, including ownership-checked commits."""
  def __init__(self, data_file, sessions_dir, database=None):
    super().__init__(data_file, sessions_dir)
    self.url = 'https://redis.example.test'
    self.database = database if database is not None else {}

  def command(self, *args):
    name = args[0]
    if name == 'GET':
      return self.database.get(args[1])
    if name == 'SET':
      key, value = args[1:3]
      if 'NX' in args and key in self.database:
        return None
      self.database[key] = value
      return 'OK'
    if name == 'EVAL':
      script = args[1]
      if script == self.COMMIT:
        lock, key, token, encoded = args[3:]
        if self.database.get(lock) != token:
          return 0
        self.database[key] = encoded
        return 1
      lock, token = args[3:]
      if self.database.get(lock) == token:
        del self.database[lock]
        return 1
      return 0
    raise AssertionError(f'Unexpected command: {name}')


class BotTests(unittest.TestCase):
  def setUp(self):
    self.temp = tempfile.TemporaryDirectory(dir=SCRATCH)
    self.addCleanup(self.temp.cleanup)
    self.env = patch.dict(os.environ, {**TEST_ENV, 'BOT_DATA_DIR': self.temp.name})
    self.env.start()
    self.addCleanup(self.env.stop)
    sys.modules.pop('tl.tl', None)
    with patch('telebot.apihelper._make_request', side_effect=AssertionError('Import called Telegram')):
      self.bot = importlib.import_module('tl.tl')
    self.api = patch('telebot.apihelper._make_request', side_effect=AssertionError('Unexpected live Telegram call'))
    self.api.start()
    self.addCleanup(self.api.stop)
    self.bot.store = MemoryRedisStore(self.bot.DATA_FILE, self.bot.SESSIONS_DIR)

  def update(self, payload):
    from telebot.types import Update
    return Update.de_json(payload)

  def seed(self, **fields):
    data = self.bot.default_data()
    data.update(fields)
    with self.bot.store.transaction():
      self.bot.store.save(data)

  def test_import_uses_synchronous_bots_and_bundled_qr(self):
    self.assertFalse(self.bot.shop_bot.threaded)
    self.assertFalse(self.bot.admin_bot.threaded)
    self.assertTrue(Path(self.bot.QR_IMAGE_PATH).is_file())
    self.assertNotIn('infinity_polling', (ROOT / 'tl' / 'tl.py').read_text())

  def test_shop_start_is_dispatched_and_duplicate_is_not(self):
    update = self.update(message_update(10, '/start'))
    with patch.object(self.bot.shop_bot, 'send_message') as send:
      self.assertTrue(self.bot.process_webhook('shop', update))
      self.assertFalse(self.bot.process_webhook('shop', update))
    self.assertEqual(send.call_count, 1)
    self.assertEqual(self.bot.store.load()['processed_updates']['shop'], [10])

  def test_admin_route_uses_the_admin_bot_and_separate_update_ids(self):
    update = self.update(message_update(10, '/start', uid=99))
    with patch.object(self.bot.admin_bot, 'send_message') as admin_send, patch.object(self.bot.shop_bot, 'send_message') as shop_send:
      self.bot.process_webhook('admin', update)
      self.bot.process_webhook('shop', update)
    admin_send.assert_called_once()
    shop_send.assert_called_once()

  def test_deposit_conversation_survives_a_cold_start(self):
    self.seed(verified_users=[42])
    with patch.object(self.bot.shop_bot, 'send_message'):
      self.bot.process_webhook('shop', self.update(message_update(1, '💰 Nạp Tiền')))
    self.assertEqual(self.bot.store.load()['pending_deposits']['42'], 'WAITING_AMOUNT')
    # A new module starts with empty memory; only Redis has the pending step.
    database = self.bot.store.database
    self.bot = importlib.reload(self.bot)
    self.bot.store = MemoryRedisStore(self.bot.DATA_FILE, self.bot.SESSIONS_DIR, database)
    self.assertEqual(self.bot.pending_deposits, {})
    with patch.object(self.bot.shop_bot, 'send_photo') as photo:
      self.bot.process_webhook('shop', self.update(message_update(2, '10000')))
    photo.assert_called_once()
    self.assertEqual(self.bot.store.load()['pending_deposits']['42'], 10000)

  def test_failure_does_not_commit_or_mark_update_processed(self):
    self.seed()
    original = self.bot.store.load()
    update = self.update({'update_id': 123})
    with patch.object(self.bot.shop_bot, 'process_new_updates', side_effect=RuntimeError('handler failed')):
      with self.assertRaises(RuntimeError):
        self.bot.process_webhook('shop', update)
    self.assertEqual(self.bot.store.load(), original)
    self.assertFalse(self.bot._processing_update)
    self.assertNotIn(self.bot.store.key + ':lock', self.bot.store.database)

  def test_callback_query_is_dispatched_and_conversation_step_is_saved(self):
    self.seed(verified_users=[42], user_ref_balances={'42': 30000}, user_ref_deposit_valid={'42': 1})
    payload = {
        'update_id': 55,
        'callback_query': {
            'id': 'callback-55', 'chat_instance': 'test-chat',
            'from': {'id': 42, 'is_bot': False, 'first_name': 'Test'},
            'message': message_update(54, 'Withdraw')['message'],
            'data': 'request_withdraw',
        },
    }
    with patch.object(self.bot.shop_bot, 'answer_callback_query') as answer, patch.object(self.bot.shop_bot, 'send_message'):
      self.bot.process_webhook('shop', self.update(payload))
    answer.assert_called_once()
    self.assertEqual(self.bot.store.load()['pending_withdraws']['42']['step'], 'WAITING_WD_AMOUNT')

  def test_authorized_http_update_runs_real_handler_and_persists_state(self):
    server = ThreadingHTTPServer(('127.0.0.1', 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
      connection = http.client.HTTPConnection(*server.server_address, timeout=5)
      with patch.object(self.bot.shop_bot, 'send_message') as send:
        connection.request('POST', '/api/webhook/shop', body=json.dumps(message_update(77, '/start')), headers={
            'Content-Type': 'application/json',
            'X-Telegram-Bot-Api-Secret-Token': 'test-secret',
        })
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        self.assertFalse(json.loads(response.read())['duplicate'])
      connection.close()
      send.assert_called_once()
      self.assertEqual(self.bot.store.load()['processed_updates']['shop'], [77])
    finally:
      server.shutdown()
      server.server_close()
      thread.join()

  def test_duplicate_history_is_bounded(self):
    self.seed(processed_updates={'shop': list(range(2000))})
    self.bot.process_webhook('shop', self.update({'update_id': 2000}))
    self.assertEqual(self.bot.store.load()['processed_updates']['shop'], list(range(1, 2001)))


class StorageTests(unittest.TestCase):
  def setUp(self):
    self.temp = tempfile.TemporaryDirectory(dir=SCRATCH)
    self.addCleanup(self.temp.cleanup)
    env = patch.dict(os.environ, TEST_ENV)
    env.start()
    self.addCleanup(env.stop)
    self.file = Path(self.temp.name) / 'bot_data.json'
    self.sessions = Path(self.temp.name) / 'sessions_storage'

  def test_local_json_roundtrip(self):
    store = StateStore(self.file, self.sessions)
    self.assertIsNone(store.load())
    with store.transaction():
      store.save({'pending_deposits': {'42': 'WAITING_AMOUNT'}})
    self.assertEqual(store.load()['pending_deposits']['42'], 'WAITING_AMOUNT')

  def test_vercel_refuses_temporary_storage(self):
    with patch.dict(os.environ, {'VERCEL': '1'}):
      with self.assertRaises(StorageError):
        StateStore(self.file, self.sessions)

  def test_incomplete_redis_configuration_is_rejected(self):
    with patch.dict(os.environ, {'UPSTASH_REDIS_REST_URL': 'https://redis.test'}):
      with self.assertRaises(StorageError):
        StateStore(self.file, self.sessions)

  def test_distributed_lock_rejects_concurrent_instance(self):
    first = MemoryRedisStore(self.file, self.sessions)
    second = MemoryRedisStore(self.file, self.sessions, first.database)
    with first.transaction():
      with self.assertRaises(StorageBusy):
        with second.transaction():
          self.fail('Second instance acquired an owned lock')
      first.save({'version': 1})
    with second.transaction():
      second.save({'version': 2})
    self.assertEqual(first.load(), {'version': 2})

  def test_expired_lock_cannot_commit_or_delete_new_owner(self):
    store = MemoryRedisStore(self.file, self.sessions)
    with store.transaction():
      store.database[store.key + ':lock'] = 'new-owner'
      with self.assertRaises(StorageError):
        store.save({'bad': 'write'})
    self.assertEqual(store.database[store.key + ':lock'], 'new-owner')
    self.assertIsNone(store.load())

  def test_session_files_restore_on_a_fresh_instance(self):
    store = MemoryRedisStore(self.file, self.sessions)
    path = self.sessions / 'product' / 'example.session'
    path.parent.mkdir(parents=True)
    path.write_bytes(b'example-session-data')
    store.save_file(path)
    path.unlink()
    second = MemoryRedisStore(self.file, self.sessions, store.database)
    second.restore_file(path)
    self.assertEqual(path.read_bytes(), b'example-session-data')
    with self.assertRaises(StorageError):
      second.restore_file(path.parent / 'missing.session')

  def test_files_outside_session_root_are_rejected(self):
    store = MemoryRedisStore(self.file, self.sessions)
    with self.assertRaises(ValueError):
      store.restore_file(self.file)

  def test_rest_command_uses_authenticated_json_body(self):
    store = MemoryRedisStore(self.file, self.sessions)
    store.token = 'redis-secret'
    with patch('tl.storage.urlopen', return_value=io.BytesIO(b'{"result":"OK"}')) as request:
      result = StateStore.command(store, 'SET', 'key', 'value')
    self.assertEqual(result, 'OK')
    sent = request.call_args.args[0]
    self.assertEqual(json.loads(sent.data), ['SET', 'key', 'value'])
    self.assertEqual(sent.get_header('Authorization'), 'Bearer redis-secret')

  def test_rest_errors_are_not_silently_ignored(self):
    store = MemoryRedisStore(self.file, self.sessions)
    with patch('tl.storage.urlopen', return_value=io.BytesIO(b'{"error":"denied"}')):
      with self.assertRaises(StorageError):
        StateStore.command(store, 'GET', 'key')


class HTTPTests(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.server = ThreadingHTTPServer(('127.0.0.1', 0), handler)
    cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
    cls.thread.start()

  @classmethod
  def tearDownClass(cls):
    cls.server.shutdown()
    cls.server.server_close()
    cls.thread.join()

  def setUp(self):
    env = patch.dict(os.environ, TEST_ENV)
    env.start()
    self.addCleanup(env.stop)

  def request(self, method='POST', path='/api/webhook/shop', body=None, headers=None):
    connection = http.client.HTTPConnection(*self.server.server_address, timeout=5)
    defaults = {'Content-Type': 'application/json', 'X-Telegram-Bot-Api-Secret-Token': 'test-secret'}
    defaults.update(headers or {})
    if body is None:
      body = json.dumps({'update_id': 123})
    connection.request(method, path, body=body, headers=defaults)
    response = connection.getresponse()
    content = response.read()
    result = (response.status, json.loads(content) if content else {}, dict(response.headers))
    connection.close()
    return result

  def test_health_needs_no_bot_config_or_network(self):
    with patch.dict(os.environ, {}, clear=True):
      status, body, _ = self.request('GET', '/api/health')
    self.assertEqual(status, 200)
    self.assertTrue(body['ok'])
    self.assertEqual(self.request('HEAD', '/api/health')[0], 200)

  def test_missing_or_wrong_secret_is_forbidden(self):
    for secret in ('', 'wrong', 'é'):
      self.assertEqual(self.request(headers={'X-Telegram-Bot-Api-Secret-Token': secret})[0], 403)

  def test_missing_config_is_unavailable(self):
    with patch.dict(os.environ, {'WEBHOOK_SECRET': ''}):
      self.assertEqual(self.request()[0], 503)

  def test_invalid_payload_and_content_type(self):
    for body in ('not json', '[]', '{}', '{"update_id":true}', '{"update_id":-1}'):
      self.assertEqual(self.request(body=body)[0], 400)
    self.assertEqual(self.request(headers={'Content-Type': 'text/plain'})[0], 415)
    self.assertEqual(self.request(body='x' * (1024 * 1024 + 1))[0], 413)

  def test_unknown_path(self):
    self.assertEqual(self.request(path='/api/webhook/unknown')[0], 404)
    self.assertEqual(self.request('GET', '/unknown')[0], 404)

  def test_both_bot_routes_and_vercel_rewrite_target(self):
    with patch('tl.tl.process_webhook', return_value=True) as process:
      for path, name in (
          ('/api/webhook/shop', 'shop'), ('/api/webhook/admin', 'admin'),
          ('/api?bot=admin', 'admin'), ('/api/index?bot=admin', 'admin'),
      ):
        self.assertEqual(self.request(path=path)[0], 200)
        self.assertEqual(process.call_args.args[0], name)
        self.assertEqual(process.call_args.args[1].update_id, 123)

  def test_duplicate_ack_and_retryable_storage_failure(self):
    with patch('tl.tl.process_webhook', return_value=False):
      self.assertTrue(self.request()[1]['duplicate'])
    for error in (StorageBusy('busy'), StorageError('offline')):
      with patch('tl.tl.process_webhook', side_effect=error):
        status, _, headers = self.request()
        self.assertEqual(status, 503)
        self.assertEqual(headers['Retry-After'], '2')
    with patch('tl.tl.process_webhook', side_effect=RuntimeError('private details')):
      status, body, _ = self.request()
      self.assertEqual(status, 500)
      self.assertNotIn('private details', json.dumps(body))


class DeploymentTests(unittest.TestCase):
  def test_registration_uses_two_authenticated_webhooks_without_dropping_updates(self):
    with patch.dict(os.environ, {**TEST_ENV, 'WEBHOOK_BASE_URL': 'https://bot.example.test'}), patch.object(sys, 'argv', ['set_webhooks']), patch('scripts.set_webhooks.telegram_request', return_value={}) as telegram:
      set_webhooks.main()
    webhooks = [call for call in telegram.call_args_list if call.args[1] == 'setWebhook']
    self.assertEqual(len(webhooks), 2)
    for call, name in zip(webhooks, ('shop', 'admin')):
      payload = call.args[2]
      self.assertEqual(payload['url'], f'https://bot.example.test/api/webhook/{name}')
      self.assertEqual(payload['secret_token'], 'test-secret')
      self.assertEqual(payload['max_connections'], 1)
      self.assertFalse(payload['drop_pending_updates'])

  def test_vercel_routes_and_duration_match_the_lock(self):
    config = json.loads((ROOT / 'vercel.json').read_text())
    maximum = config['functions']['api/index.py']['maxDuration']
    self.assertLess(maximum, StateStore.LOCK_SECONDS)
    routes = {route['source'] for route in config['rewrites']}
    self.assertTrue({'/api/webhook/shop', '/api/webhook/admin', '/api/health'} <= routes)

  def test_static_output_cannot_publish_source_or_original_archive(self):
    config = json.loads((ROOT / 'vercel.json').read_text())
    self.assertEqual(config['outputDirectory'], 'public')
    self.assertTrue((ROOT / 'public').is_dir())
    self.assertFalse(any(path.suffix in ('.py', '.zip', '.session', '.json') for path in (ROOT / 'public').rglob('*')))

  def test_migration_normalizes_file_paths_and_preserves_balances(self):
    with tempfile.TemporaryDirectory(dir=SCRATCH) as directory, patch.dict(os.environ, TEST_ENV):
      root = Path(directory)
      sessions = root / 'sessions_storage'
      sessions.mkdir()
      (sessions / 'one.session').write_bytes(b'session')
      (sessions / 'one.json').write_text('{}')
      source = root / 'bot_data.json'
      source.write_text(json.dumps({
          'user_balances': {'42': 50000},
          'tele_products': {'p': {'stock': [{
              'session_path': 'sessions_storage/one.session',
              'json_path': 'sessions_storage/one.json',
          }]}},
      }))
      store = MemoryRedisStore(source, sessions)
      with patch('scripts.migrate_data.StateStore', return_value=store):
        self.assertEqual(migrate(source, sessions), 2)
        with self.assertRaises(ValueError):
          migrate(source, sessions)
      data = store.load()
      self.assertEqual(data['user_balances']['42'], 50000)
      self.assertEqual(data['tele_products']['p']['stock'][0]['session_path'], '/tmp/tl/sessions_storage/one.session')
      self.assertEqual(base64.b64decode(store.database[store.key + ':file:one.session']), b'session')


if __name__ == '__main__':
  unittest.main()