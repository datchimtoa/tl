import base64
import ast
import asyncio
import http.client
from http.server import ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import sys
import subprocess
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from api import index as application
from api.index import APP_VERSION, handler, StateStore, StorageBusy, StorageError
from scripts import set_webhooks
from scripts.migrate_data import migrate

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = ROOT / '.runtime'
SCRATCH.mkdir(exist_ok=True)
TEST_ENV = {
    'SHOP_TOKEN': '123456:TEST_SHOP', 'ADMIN_TOKEN': '654321:TEST_ADMIN',
    'ADMIN_ID': '99', 'WEBHOOK_SECRET': 'test-secret',
    'VERCEL': '', 'UPSTASH_REDIS_REST_URL': '', 'UPSTASH_REDIS_REST_TOKEN': '',
    'KV_REST_API_URL': '', 'KV_REST_API_TOKEN': '',
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
    self.bot = application
    self.bot._runtime_initialized = False
    with patch('telebot.apihelper._make_request', side_effect=AssertionError('Import called Telegram')):
      self.bot.ensure_runtime()
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
    self.assertNotIn('infinity_polling', (ROOT / 'api' / 'index.py').read_text())

  def test_initialization_is_idempotent_and_registers_each_handler_once(self):
    before = (len(self.bot.shop_bot.message_handlers), len(self.bot.shop_bot.callback_query_handlers), len(self.bot.admin_bot.message_handlers), len(self.bot.admin_bot.callback_query_handlers))
    shop = self.bot.shop_bot
    self.bot.ensure_runtime()
    self.assertIs(shop, self.bot.shop_bot)
    after = (len(self.bot.shop_bot.message_handlers), len(self.bot.shop_bot.callback_query_handlers), len(self.bot.admin_bot.message_handlers), len(self.bot.admin_bot.callback_query_handlers))
    self.assertEqual(before, (4, 4, 5, 2))
    self.assertEqual(after, before)

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
    self.bot._runtime_initialized = False
    self.bot.ensure_runtime()
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

  def test_committed_update_is_acknowledged_when_lock_cleanup_fails(self):
    store = self.bot.store
    original = store.command

    def fail_release(*args):
      if args[:2] == ('EVAL', store.RELEASE):
        raise StorageError('private-provider-error', code='redis_request_failed')
      return original(*args)

    with patch.object(store, 'command', side_effect=fail_release):
      self.assertTrue(self.bot.process_webhook('shop', self.update({'update_id': 12})))
    self.assertEqual(store.load()['processed_updates']['shop'], [12])

  def test_storage_error_identifies_the_failing_operation(self):
    for name, operation in (
        ('lock_acquire', 'transaction'), ('state_load', 'load'), ('state_commit', 'save'),
    ):
      failure = StorageError('private-provider-error', code='redis_request_failed')
      with self.subTest(stage=name), patch.object(self.bot.store, operation, side_effect=failure):
        with self.assertRaises(StorageError) as error:
          self.bot.process_webhook('shop', self.update({'update_id': 88}))
      self.assertEqual(error.exception.stage, name)

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

  def test_http_returns_200_after_commit_even_when_cleanup_fails(self):
    store = self.bot.store
    original = store.command

    def fail_release(*args):
      if args[:2] == ('EVAL', store.RELEASE):
        raise StorageError('private-provider-error', code='redis_request_failed')
      return original(*args)

    server = ThreadingHTTPServer(('127.0.0.1', 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    connection = http.client.HTTPConnection(*server.server_address, timeout=5)
    try:
      with patch.object(store, 'command', side_effect=fail_release):
        connection.request('POST', '/api/webhook/shop', body=json.dumps({'update_id': 90}), headers={
            'Content-Type': 'application/json',
            'X-Telegram-Bot-Api-Secret-Token': 'test-secret',
        })
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        self.assertTrue(json.loads(response.read())['ok'])
      self.assertEqual(store.load()['processed_updates']['shop'], [90])
    finally:
      connection.close()
      server.shutdown()
      server.server_close()
      thread.join()

  def test_duplicate_history_is_bounded(self):
    self.seed(processed_updates={'shop': list(range(2000))})
    self.bot.process_webhook('shop', self.update({'update_id': 2000}))
    self.assertEqual(self.bot.store.load()['processed_updates']['shop'], list(range(1, 2001)))

  def test_admin_commands_reset_the_tables_and_reach_users(self):
    self.seed(user_balances={'42': 1234, '43': 99}, verified_users=[42])
    with patch.object(self.bot.admin_bot, 'reply_to') as reply, \
        patch.object(self.bot.shop_bot, 'send_message') as notice:
      self.bot.process_webhook('admin', self.update(message_update(201, '/resettop all', uid=99)))
    reply.assert_called_once()
    saved = self.bot.store.load()
    self.assertEqual(saved['user_refs_count'], {})
    self.assertEqual(saved['user_total_deposits'], {})
    self.assertEqual(notice.call_count, 2)
    self.assertEqual(self.bot.user_refs_count, {})

  def test_admin_resettop_rejects_an_unknown_table(self):
    self.seed(user_refs_count={'42': 3})
    with patch.object(self.bot.admin_bot, 'reply_to'), patch.object(self.bot.shop_bot, 'send_message'):
      self.bot.process_webhook('admin', self.update(message_update(202, '/resettop everything', uid=99)))
    self.assertEqual(self.bot.store.load()['user_refs_count'], {'42': 3})

  def test_admin_resettop_broadcast_is_capped_per_invocation(self):
    users = {str(uid): 1 for uid in range(1, 151)}
    self.seed(user_balances=users)
    with patch.object(self.bot, 'MAX_BROADCAST_RECIPIENTS', 100), \
        patch.object(self.bot.admin_bot, 'reply_to'), \
        patch.object(self.bot.shop_bot, 'send_message') as notice:
      self.bot.process_webhook('admin', self.update(message_update(203, '/resettop ref', uid=99)))
    self.assertEqual(notice.call_count, 100)

  def test_donacc_command_shares_the_order_group_link(self):
    with patch.object(self.bot.shop_bot, 'send_message') as send:
      self.bot.process_webhook('shop', self.update(message_update(204, '/donacc')))
    self.assertIn(self.bot.DON_ACC_GROUP_URL, send.call_args.args[1])

  def test_donacc_menu_button_does_not_require_verification(self):
    with patch.object(self.bot.shop_bot, 'send_message') as send, \
        patch.object(self.bot.shop_bot, 'reply_to') as reply:
      self.bot.process_webhook('shop', self.update(message_update(205, '🛒 Đơn acc')))
    reply.assert_not_called()
    self.assertIn(self.bot.DON_ACC_GROUP_URL, send.call_args.args[1])

  def test_order_group_id_is_configurable_and_normalized(self):
    for value in ('-100123', '100123'):
      with self.subTest(value=value), patch.dict(os.environ, {'DON_MUA_GROUP_CHAT_ID': value}):
        self.bot._runtime_initialized = False
        self.bot.ensure_runtime()
        self.assertEqual(self.bot.DON_MUA_GROUP_CHAT_ID, -100123)

  def test_giftcode_menu_step_is_consumed_so_a_typo_does_not_block_the_chat(self):
    self.seed(verified_users=[42])
    with patch.object(self.bot.shop_bot, 'send_message') as send:
      self.bot.process_webhook('shop', self.update(message_update(210, '🎁 Nhập Code')))
      self.bot.process_webhook('shop', self.update(message_update(211, 'khong-ton-tai')))
    invalid = send.call_args_list[-1].args[1]
    self.assertIn('không hợp lệ', invalid)
    self.assertEqual(self.bot.pending_withdraws, {})


class PurchaseTests(unittest.TestCase):
  """Live-account verification, refunds and order logging on purchase."""

  def setUp(self):
    self.temp = tempfile.TemporaryDirectory(dir=SCRATCH)
    self.addCleanup(self.temp.cleanup)
    self.env = patch.dict(os.environ, {**TEST_ENV, 'BOT_DATA_DIR': self.temp.name})
    self.env.start()
    self.addCleanup(self.env.stop)
    self.bot = application
    self.bot._runtime_initialized = False
    with patch('telebot.apihelper._make_request', side_effect=AssertionError('Import called Telegram')):
      self.bot.ensure_runtime()
    self.api = patch('telebot.apihelper._make_request', side_effect=AssertionError('Unexpected live Telegram call'))
    self.api.start()
    self.addCleanup(self.api.stop)
    self.bot.store = MemoryRedisStore(self.bot.DATA_FILE, self.bot.SESSIONS_DIR)

  def stock_accounts(self, count):
    accounts = []
    # Real stock lives under SESSIONS_DIR so it can be mirrored to storage.
    root = Path(self.temp.name) / 'sessions_storage'
    root.mkdir(parents=True, exist_ok=True)
    for index in range(count):
      session = root / f'acc{index}.session'
      credentials = root / f'acc{index}.json'
      session.write_bytes(f'session-{index}'.encode())
      credentials.write_text(json.dumps({'app_id': 1, 'app_hash': 'hash'}))
      self.bot.store.save_file(session)
      self.bot.store.save_file(credentials)
      accounts.append({'session_path': str(session), 'json_path': str(credentials)})
    return accounts

  def confirm_purchase(self, qty, price=10000):
    with self.bot.store.transaction():
      self.bot.restore_data(self.bot.default_data())
      self.bot.user_balances[42] = price * qty
      self.bot.verified_users.add(42)
      self.bot.tele_products['p1'] = {
          'name': 'Tele pack', 'price': price, 'stock': self.stock_accounts(qty),
      }
      self.bot.pending_buy[42] = {
          'cat': 'tele', 'p_id': 'p1', 'qty': qty,
          'total_price': price * qty, 'step': 'CONFIRM_BUY',
      }
      self.bot.store.save(self.bot.snapshot_data())
    return {'update_id': 300, 'callback_query': {
        'id': 'confirm-300', 'chat_instance': 'test-chat',
        'from': {'id': 42, 'is_bot': False, 'first_name': 'Buyer'},
        'message': message_update(299, 'Confirm')['message'],
        'data': 'confirm_buy_yes',
    }}

  def shop_patches(self):
    patches = {
        'answer_callback_query': patch.object(self.bot.shop_bot, 'answer_callback_query'),
        'delete_message': patch.object(self.bot.shop_bot, 'delete_message'),
        'edit_message_text': patch.object(self.bot.shop_bot, 'edit_message_text'),
        'send_document': patch.object(self.bot.shop_bot, 'send_document'),
        'send_message': patch.object(self.bot.shop_bot, 'send_message'),
    }
    started = {}
    for name, patcher in patches.items():
      started[name] = patcher.start()
      self.addCleanup(patcher.stop)
    return started

  def test_dead_accounts_are_removed_and_the_missing_part_is_refunded(self):
    payload = self.confirm_purchase(qty=3)
    calls = self.shop_patches()
    answers = iter([(True, 'ok'), (False, 'die'), (True, 'ok')])
    with patch.object(self.bot, 'run_async_check', side_effect=lambda *a, **k: next(answers)):
      self.bot.process_webhook('shop', self.bot_update(payload))
    self.assertTrue(calls['send_document'].called)
    saved = self.bot.store.load()
    self.assertEqual(saved['user_balances']['42'], 10000)
    # Only one account was still alive to sell, so no stock is left over.
    self.assertEqual(self.bot.tele_products, {})

  def test_purchase_is_fully_refunded_when_no_account_is_alive(self):
    payload = self.confirm_purchase(qty=2)
    calls = self.shop_patches()
    with patch.object(self.bot, 'run_async_check', return_value=(False, 'die')):
      self.bot.process_webhook('shop', self.bot_update(payload))
    calls['send_document'].assert_not_called()
    saved = self.bot.store.load()
    self.assertEqual(saved['user_balances']['42'], 20000)
    # Everything died, so the product is emptied and then pruned from the shop.
    self.assertEqual(self.bot.tele_products, {})
    self.assertNotIn('p1', saved['tele_products'])
    self.assertTrue(any('hoàn lại' in call.args[1] for call in calls['send_message'].call_args_list))

  def test_session_checks_per_purchase_are_bounded(self):
    payload = self.confirm_purchase(qty=5)
    calls = self.shop_patches()
    with patch.object(self.bot, 'MAX_SESSION_CHECKS_PER_PURCHASE', 2), \
        patch.object(self.bot, 'run_async_check', return_value=(False, 'die')) as check:
      self.bot.process_webhook('shop', self.bot_update(payload))
    self.assertEqual(check.call_count, 2)
    # Untouched accounts stay in stock for a later attempt.
    self.assertEqual(len(self.bot.store.load()['tele_products']['p1']['stock']), 3)

  def test_order_is_logged_to_the_configured_group(self):
    payload = self.confirm_purchase(qty=1)
    calls = self.shop_patches()
    with patch.object(self.bot, 'run_async_check', return_value=(True, 'ok')), \
        patch.object(self.bot, 'DON_MUA_GROUP_CHAT_ID', -100999):
      self.bot.process_webhook('shop', self.bot_update(payload))
    chats = [call.args[0] for call in calls['send_message'].call_args_list]
    self.assertIn(-100999, chats)

  def bot_update(self, payload):
    from telebot.types import Update
    return Update.de_json(payload)


class SessionCheckTests(unittest.TestCase):
  """Telethon session checks use fakes; no network access happens here."""

  class FakeClient:
    def __init__(self, authorized=True, user=None, dialogs_error=None):
      self.authorized = authorized
      self.user = user if user is not None else SimpleNamespace(
          deactivated=False, restricted=False
      )
      self.dialogs_error = dialogs_error
      self.connected = False
      self.disconnected = False

    async def connect(self):
      self.connected = True

    async def is_user_authorized(self):
      return self.authorized

    async def get_me(self):
      return self.user

    def is_connected(self):
      return self.connected

    async def disconnect(self):
      self.disconnected = True

    async def iter_dialogs(self, limit=None):
      if self.dialogs_error:
        raise self.dialogs_error
      return
      yield  # pragma: no cover - makes this an async generator

  def check(self, client, session_path='session.session'):
    return asyncio.run(application.check_single_session(
        session_path, 1, 'hash',
        client_factory=lambda *args, **kwargs: client,
    ))

  def test_live_session_is_accepted_and_disconnected(self):
    client = self.FakeClient()
    with tempfile.NamedTemporaryFile(suffix='.session') as session:
      live, result = self.check(client, session.name)
    self.assertTrue(live)
    self.assertEqual(result, client.user)
    self.assertTrue(client.disconnected)

  def test_missing_session_file_is_reported_without_a_client(self):
    live, reason = self.check(self.FakeClient(), '/nonexistent/missing.session')
    self.assertFalse(live)
    self.assertIn('không tồn tại', reason)

  def test_unauthorized_session_is_dead(self):
    live, reason = self.check(self.FakeClient(authorized=False), __file__)
    self.assertFalse(live)
    self.assertEqual(reason, 'Unauthorized/Session Die')

  def test_flagged_user_is_dead(self):
    for flag in ('deactivated', 'restricted'):
      with self.subTest(flag=flag):
        flags = {'deactivated': False, 'restricted': False}
        flags[flag] = True
        client = self.FakeClient(user=SimpleNamespace(**flags))
        live, reason = self.check(client, __file__)
        self.assertFalse(live)
        self.assertEqual(reason, 'Deactivated/Frozen')
        self.assertTrue(client.disconnected)

  def test_frozen_dialog_error_is_dead(self):
    client = self.FakeClient(dialogs_error=RuntimeError('USER_DEACTIVATED'))
    live, reason = self.check(client, __file__)
    self.assertFalse(live)
    self.assertIn('Frozen/Invalid', reason)

  def test_unrelated_dialog_error_still_counts_as_live(self):
    client = self.FakeClient(dialogs_error=RuntimeError('temporary timeout'))
    live, _ = self.check(client, __file__)
    self.assertTrue(live)


class ReferralTests(unittest.TestCase):
  """The referral bonus pays VND but stays bounded."""

  def setUp(self):
    self.temp = tempfile.TemporaryDirectory(dir=SCRATCH)
    self.addCleanup(self.temp.cleanup)
    self.env = patch.dict(os.environ, {**TEST_ENV, 'BOT_DATA_DIR': self.temp.name})
    self.env.start()
    self.addCleanup(self.env.stop)
    self.bot = application
    self.bot._runtime_initialized = False
    with patch('telebot.apihelper._make_request', side_effect=AssertionError('Import called Telegram')):
      self.bot.ensure_runtime()
    self.api = patch('telebot.apihelper._make_request', side_effect=AssertionError('Unexpected live Telegram call'))
    self.api.start()
    self.addCleanup(self.api.stop)
    self.bot.store = MemoryRedisStore(self.bot.DATA_FILE, self.bot.SESSIONS_DIR)

  def verify(self, referrer_refs):
    with self.bot.store.transaction():
      data = self.bot.default_data()
      data['user_refs_count'] = {'99': referrer_refs}
      data['user_balances']['99'] = 1000
      self.bot.store.save(data)
    with patch.object(self.bot.shop_bot, 'get_chat_member') as member, \
        patch.object(self.bot.shop_bot, 'send_message'), \
        patch.object(self.bot.admin_bot, 'send_message'), \
        patch.object(self.bot.shop_bot, 'answer_callback_query'), \
        patch.object(self.bot.shop_bot, 'edit_message_text'), \
        patch.object(self.bot.random, 'randint', return_value=250):
      member.return_value.status = 'member'
      self.bot.process_webhook('shop', self.bot_update(message_update(400, '/start ref_99', uid=42)))
      self.bot.process_webhook('shop', self.bot_update({
          'update_id': 401,
          'callback_query': {
              'id': 'callback-401', 'chat_instance': 'test-chat',
              'from': {'id': 42, 'is_bot': False, 'first_name': 'Ref'},
              'message': message_update(400, '/start')['message'],
              'data': 'verify_join',
          },
      }))
    return self.bot.store.load()

  def test_bonus_is_paid_in_vnd_for_a_new_referral(self):
    saved = self.verify(referrer_refs=0)
    self.assertEqual(saved['user_refs_count']['99'], 1)
    self.assertEqual(saved['user_balances']['99'], 1250)

  def test_bonus_stops_after_the_limit(self):
    saved = self.verify(referrer_refs=self.bot.REFERRAL_BONUS_LIMIT)
    self.assertEqual(saved['user_refs_count']['99'], self.bot.REFERRAL_BONUS_LIMIT + 1)
    self.assertEqual(saved['user_balances']['99'], 1000)

  def bot_update(self, payload):
    from telebot.types import Update
    return Update.de_json(payload)


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
      with self.assertRaises(StorageError) as error:
        StateStore(self.file, self.sessions)
    self.assertEqual(error.exception.code, 'redis_not_configured')

  def test_incomplete_redis_configuration_is_rejected(self):
    with patch.dict(os.environ, {'UPSTASH_REDIS_REST_URL': 'https://redis.test'}):
      with self.assertRaises(StorageError):
        StateStore(self.file, self.sessions)

  def test_vercel_kv_rest_variable_names_are_supported(self):
    with patch.dict(os.environ, {
        'VERCEL': '1', 'KV_REST_API_URL': ' https://redis.test/ ',
        'KV_REST_API_TOKEN': ' redis-secret ',
    }):
      store = StateStore(self.file, self.sessions)
    self.assertEqual(store.url, 'https://redis.test')
    self.assertEqual(store.token, 'redis-secret')

  def test_primary_credentials_are_not_mixed_with_kv_credentials(self):
    with patch.dict(os.environ, {
        'UPSTASH_REDIS_REST_URL': 'https://primary.test',
        'KV_REST_API_URL': 'https://other.test', 'KV_REST_API_TOKEN': 'other-token',
    }):
      with self.assertRaises(StorageError) as error:
        StateStore(self.file, self.sessions)
    self.assertEqual(error.exception.code, 'redis_config_incomplete')

  def test_redis_url_must_be_an_https_origin(self):
    for url in ('redis://redis.test', 'https://redis.test/set', 'https://redis.test?token=private', 'https://private@redis.test'):
      with self.subTest(url=url), patch.dict(os.environ, {
          'UPSTASH_REDIS_REST_URL': url, 'UPSTASH_REDIS_REST_TOKEN': 'private',
      }):
        with self.assertRaises(StorageError) as error:
          StateStore(self.file, self.sessions)
        self.assertEqual(error.exception.code, 'redis_url_invalid')

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

  def test_cleanup_error_does_not_mask_the_original_transaction_error(self):
    store = MemoryRedisStore(self.file, self.sessions)
    original = store.command

    def fail_release(*args):
      if args[:2] == ('EVAL', store.RELEASE):
        raise StorageError('private-cleanup', code='redis_request_failed')
      return original(*args)

    with patch.object(store, 'command', side_effect=fail_release):
      with self.assertRaisesRegex(RuntimeError, 'original-handler-failure'):
        with store.transaction():
          raise RuntimeError('original-handler-failure')
    self.assertFalse(store._mutex.locked())

  def test_cleanup_failure_before_any_commit_is_still_reported(self):
    store = MemoryRedisStore(self.file, self.sessions)
    original = store.command

    def fail_release(*args):
      if args[:2] == ('EVAL', store.RELEASE):
        raise StorageError('private-cleanup', code='redis_request_failed')
      return original(*args)

    with patch.object(store, 'command', side_effect=fail_release):
      with self.assertRaises(StorageError):
        with store.transaction():
          pass
    self.assertFalse(store._mutex.locked())

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
    with patch('api.index.urlopen', return_value=io.BytesIO(b'{"result":"OK"}')) as request:
      result = StateStore.command(store, 'SET', 'key', 'value')
    self.assertEqual(result, 'OK')
    sent = request.call_args.args[0]
    self.assertEqual(json.loads(sent.data), ['SET', 'key', 'value'])
    self.assertEqual(sent.get_header('Authorization'), 'Bearer redis-secret')

  def test_rest_errors_are_not_silently_ignored(self):
    store = MemoryRedisStore(self.file, self.sessions)
    with patch('api.index.urlopen', return_value=io.BytesIO(b'{"error":"denied"}')):
      with self.assertRaises(StorageError):
        StateStore.command(store, 'GET', 'key')

  def test_http_failures_have_credential_safe_error_codes(self):
    store = MemoryRedisStore(self.file, self.sessions)
    for status, expected in ((401, 'redis_auth_failed'), (403, 'redis_permission_denied'), (429, 'redis_rate_limited'), (503, 'redis_http_error')):
      failure = HTTPError('https://private-url.test', status, 'private-token', {}, io.BytesIO(b'{"error":"private-data"}'))
      with self.subTest(status=status), patch('api.index.urlopen', side_effect=failure):
        with self.assertRaises(StorageError) as error:
          StateStore.command(store, 'SET', 'private-key', 'private-value')
        self.assertEqual(error.exception.code, expected)
        self.assertNotIn('private', str(error.exception))

  def test_provider_and_network_errors_are_classified_without_echoing_details(self):
    store = MemoryRedisStore(self.file, self.sessions)
    for message, expected in (
        ('NOPERM private-state', 'redis_permission_denied'),
        ('READONLY private-state', 'redis_permission_denied'),
        ('ERR max requests limit exceeded private-state', 'redis_rate_limited'),
        ('NOAUTH private-token', 'redis_auth_failed'),
    ):
      with self.subTest(message=message), patch('api.index.urlopen', return_value=io.BytesIO(json.dumps({'error': message}).encode())):
        with self.assertRaises(StorageError) as error:
          StateStore.command(store, 'SET', 'key', 'value')
        self.assertEqual(error.exception.code, expected)
        self.assertNotIn('private', str(error.exception))
    with patch('api.index.urlopen', side_effect=URLError('private-url')):
      with self.assertRaises(StorageError) as error:
        StateStore.command(store, 'PING')
    self.assertEqual(error.exception.code, 'redis_request_failed')

  def test_http_400_permission_error_body_is_classified_and_closed(self):
    store = MemoryRedisStore(self.file, self.sessions)
    body = io.BytesIO(b'{"error":"NOPERM private-token"}')
    failure = HTTPError('https://private-url.test', 400, 'private-token', {}, body)
    with patch('api.index.urlopen', side_effect=failure):
      with self.assertRaises(StorageError) as error:
        StateStore.command(store, 'SET', 'key', 'value')
    self.assertEqual(error.exception.code, 'redis_permission_denied')
    self.assertTrue(body.closed)
    self.assertNotIn('private', str(error.exception))

  def test_non_json_provider_response_has_a_safe_error_code(self):
    store = MemoryRedisStore(self.file, self.sessions)
    for response in (b'not json', b'[]', b'{"unexpected":"private-data"}'):
      with self.subTest(response=response), patch('api.index.urlopen', return_value=io.BytesIO(response)):
        with self.assertRaises(StorageError) as error:
          StateStore.command(store, 'PING')
        self.assertEqual(error.exception.code, 'redis_response_invalid')


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
    self.assertEqual(body['version'], APP_VERSION)
    self.assertEqual(self.request('HEAD', '/api/health')[0], 200)

  def test_storage_check_requires_authentication_and_never_dispatches_updates(self):
    with patch('api.index.StateStore') as factory, patch('api.index.process_webhook') as process:
      self.assertEqual(self.request('GET', '/api/health?check=storage', headers={'X-Telegram-Bot-Api-Secret-Token': 'wrong'})[0], 403)
      factory.assert_not_called()
      store = factory.return_value
      store.url = 'https://redis.test'
      store.command.side_effect = ['PONG', 1]
      status, body, _ = self.request('GET', '/api/health?check=storage')
      self.assertEqual(status, 200)
      self.assertEqual(body['check'], 'read-only')
      self.assertEqual([call.args for call in store.command.call_args_list], [('PING',), ('EVAL', 'return 1', 0)])
      process.assert_not_called()

  def test_storage_configuration_failure_is_specific_and_private(self):
    with patch.dict(os.environ, {'VERCEL': '1'}):
      status, body, _ = self.request('GET', '/api/health?check=storage')
    self.assertEqual(status, 503)
    self.assertEqual(body['code'], 'redis_not_configured')
    with patch('api.index.StateStore', side_effect=StorageError('private-token', code='redis_auth_failed')):
      with self.assertLogs('api.index', level='ERROR') as logs:
        status, body, _ = self.request('GET', '/api/health?check=storage')
    self.assertEqual(body['code'], 'redis_auth_failed')
    self.assertNotIn('private-token', json.dumps(body) + ''.join(logs.output))

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
    # The server rejects from Content-Length before reading the body. Sending
    # a full megabyte races its early socket close and can cause BrokenPipe.
    self.assertEqual(self.request(body='', headers={'Content-Length': str(1024 * 1024 + 1)})[0], 413)

  def test_unknown_path(self):
    self.assertEqual(self.request(path='/api/webhook/unknown')[0], 404)
    self.assertEqual(self.request('GET', '/unknown')[0], 404)

  def test_both_bot_routes_and_vercel_rewrite_target(self):
    with patch('api.index.process_webhook', return_value=True) as process:
      for path, name in (
          ('/api/webhook/shop', 'shop'), ('/api/webhook/admin', 'admin'),
          ('/api?bot=admin', 'admin'), ('/api/index?bot=admin', 'admin'),
      ):
        self.assertEqual(self.request(path=path)[0], 200)
        self.assertEqual(process.call_args.args[0], name)
        self.assertEqual(process.call_args.args[1].update_id, 123)

  def test_duplicate_ack_and_retryable_storage_failure(self):
    with patch('api.index.process_webhook', return_value=False):
      self.assertTrue(self.request()[1]['duplicate'])
    for error in (StorageBusy('busy'), StorageError('offline')):
      with patch('api.index.process_webhook', side_effect=error):
        status, _, headers = self.request()
        self.assertEqual(status, 503)
        self.assertEqual(headers['Retry-After'], '2')
    with patch('api.index.process_webhook', side_effect=RuntimeError('private details')):
      status, body, _ = self.request()
      self.assertEqual(status, 500)
      self.assertNotIn('private details', json.dumps(body))

  def test_webhook_storage_errors_expose_codes_not_credentials(self):
    with patch('api.index.process_webhook', side_effect=StorageError('private-token', code='redis_permission_denied', stage='state_commit')):
      with self.assertLogs('api.index', level='ERROR') as logs:
        status, body, _ = self.request()
    self.assertEqual(status, 503)
    self.assertEqual(body['code'], 'redis_permission_denied')
    self.assertEqual(body['stage'], 'state_commit')
    self.assertIn('stage=state_commit', ''.join(logs.output))
    self.assertNotIn('private-token', json.dumps(body) + ''.join(logs.output))

  def test_missing_bot_settings_have_a_safe_runtime_error(self):
    with patch.dict(os.environ, {'SHOP_TOKEN': ''}), patch.object(application, '_runtime_initialized', False):
      status, body, _ = self.request()
    self.assertEqual(status, 503)
    self.assertEqual(body['code'], 'bot_config_missing')
    self.assertEqual(body['stage'], 'runtime_init')


class DeploymentTests(unittest.TestCase):
  def test_registration_uses_two_authenticated_webhooks_without_dropping_updates(self):
    with patch.dict(os.environ, {**TEST_ENV, 'WEBHOOK_BASE_URL': 'https://bot.example.test'}), patch.object(sys, 'argv', ['set_webhooks']), patch('api.index.deployment_preflight') as preflight, patch('api.index.telegram_request', return_value={}) as telegram:
      set_webhooks.main()
    preflight.assert_called_once_with('https://bot.example.test', 'test-secret')
    webhooks = [call for call in telegram.call_args_list if call.args[1] == 'setWebhook']
    self.assertEqual(len(webhooks), 2)
    for call, name in zip(webhooks, ('shop', 'admin')):
      payload = call.args[2]
      self.assertEqual(payload['url'], f'https://bot.example.test/api/webhook/{name}')
      self.assertEqual(payload['secret_token'], 'test-secret')
      self.assertEqual(payload['max_connections'], 1)
      self.assertFalse(payload['drop_pending_updates'])

  def test_failed_preflight_does_not_change_webhook_registration(self):
    with patch.dict(os.environ, TEST_ENV), patch('api.index.deployment_preflight', side_effect=RuntimeError('Wrong deployed version')), patch('api.index.telegram_request') as telegram:
      with self.assertRaises(RuntimeError):
        application.configure_webhooks('https://bot.example.test')
    telegram.assert_not_called()

  def test_preflight_rejects_old_liveness_response(self):
    with patch('api.index.urlopen', return_value=io.BytesIO(b'{"ok":true,"service":"telegram-webhooks"}')):
      with self.assertRaises(RuntimeError):
        application.deployment_preflight('https://bot.example.test', 'test-secret')

  def test_single_file_contains_handlers_storage_and_no_local_imports(self):
    source = (ROOT / 'api' / 'index.py').read_text()
    tree = ast.parse(source)
    for node in ast.walk(tree):
      if isinstance(node, ast.ImportFrom):
        self.assertFalse((node.module or '').startswith(('tl.', 'api.', 'scripts.')))
    self.assertFalse((ROOT / 'tl' / 'tl.py').exists())
    self.assertFalse((ROOT / 'tl' / 'storage.py').exists())
    self.assertIn('def shop_start(', source)
    self.assertIn('class StateStore:', source)
    self.assertNotIn('exec(', source)

  def test_single_file_import_without_credentials_or_optional_dependencies(self):
    script = (
        'import sys\n'
        'from api import index\n'
        'assert not index._runtime_initialized\n'
        'assert "telebot" not in sys.modules\n'
        'assert "telethon" not in sys.modules\n'
        'assert "qrcode" not in sys.modules\n'
        'print(index.APP_VERSION)\n'
    )
    result = subprocess.run([sys.executable, '-c', script], cwd=ROOT, env={}, capture_output=True, text=True, timeout=20)
    self.assertEqual(result.returncode, 0, result.stderr)
    self.assertIn(APP_VERSION, result.stdout)

  def test_entrypoint_runs_when_copied_without_the_repository(self):
    with tempfile.TemporaryDirectory(dir=SCRATCH) as directory:
      entrypoint = Path(directory) / 'webhook.py'
      entrypoint.write_text((ROOT / 'api' / 'index.py').read_text())
      script = (
          'import runpy, sys\n'
          'app = runpy.run_path(sys.argv[1])\n'
          'assert not app["_runtime_initialized"]\n'
          'assert "telebot" not in sys.modules\n'
          'assert callable(app["handler"])\n'
          'print(app["APP_VERSION"])\n'
      )
      result = subprocess.run([sys.executable, '-c', script, str(entrypoint)], cwd=directory, env={}, capture_output=True, text=True, timeout=20)
    self.assertEqual(result.returncode, 0, result.stderr)
    self.assertIn(APP_VERSION, result.stdout)

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