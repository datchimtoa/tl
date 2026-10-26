"""Single-file Telegram shop/admin webhook runtime for Vercel.

Run: python -m api.index
Register: python -m api.index --set-webhooks --url https://your-project.vercel.app
All handlers and durable storage are readable code in this file.
"""

import argparse
import asyncio
import base64
from contextlib import contextmanager
from datetime import datetime
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import logging
import os
from pathlib import Path
import random
import re
import shutil
import threading
import uuid
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request, urlopen
import zipfile

logger = logging.getLogger(__name__)
MAX_BODY_BYTES = 1024 * 1024
APP_VERSION = 'single-file-webhook-v1'
PROJECT_DIR = Path(__file__).resolve().parent.parent
_runtime_lock = threading.Lock()
_runtime_initialized = False
_processing_update = False


# --- DURABLE STORAGE ---

class StorageError(RuntimeError):
  def __init__(self, message, code='storage_unavailable', stage='storage'):
    super().__init__(message)
    self.code = code
    self.stage = stage


class StorageBusy(StorageError):
  def __init__(self, message):
    super().__init__(message, code='storage_busy')


def redis_error_code(error):
  # Classify provider errors without exposing their text (which may include
  # command arguments, URLs, keys, credentials, or private state).
  text = error.upper() if isinstance(error, str) else ''
  if any(word in text for word in ('NOPERM', 'READONLY', 'READ-ONLY', 'PERMISSION')):
    return 'redis_permission_denied'
  if any(word in text for word in ('NOAUTH', 'WRONGPASS', 'INVALID TOKEN', 'UNAUTHORIZED')):
    return 'redis_auth_failed'
  if any(word in text for word in ('RATE LIMIT', 'QUOTA', 'MAX REQUEST', 'REQUEST LIMIT')):
    return 'redis_rate_limited'
  return 'redis_command_failed'


class StateStore:
  # Longer than vercel.json's 60-second maximum invocation duration.
  LOCK_SECONDS = 75
  COMMIT = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[2], ARGV[2])
return 1
"""
  RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""

  def __init__(self, data_file, sessions_dir):
    self.data_file = Path(data_file)
    self.sessions_dir = Path(sessions_dir)
    primary_url = os.environ.get('UPSTASH_REDIS_REST_URL', '').strip()
    primary_token = os.environ.get('UPSTASH_REDIS_REST_TOKEN', '').strip()
    # Some Vercel integrations expose the same REST credentials with KV_ names.
    # Select a whole pair; never mix credentials from different databases.
    if primary_url or primary_token:
      self.url, self.token = primary_url.rstrip('/'), primary_token
    else:
      self.url = os.environ.get('KV_REST_API_URL', '').strip().rstrip('/')
      self.token = os.environ.get('KV_REST_API_TOKEN', '').strip()
    self.key = os.environ.get('BOT_STORAGE_KEY', 'tl:state')
    self._mutex = threading.Lock()
    self._lock_token = None
    self._committed = False
    if bool(self.url) != bool(self.token):
      raise StorageError('Set a complete Redis REST URL/token pair.', code='redis_config_incomplete')
    if os.environ.get('VERCEL') and not self.url:
      raise StorageError('Vercel requires durable Redis REST storage; local files are temporary.', code='redis_not_configured')
    if self.url:
      try:
        parsed = urlsplit(self.url)
      except ValueError:
        raise StorageError('The Redis REST URL must be an HTTPS origin.', code='redis_url_invalid') from None
      if (parsed.scheme != 'https' or not parsed.netloc or parsed.path
          or parsed.query or parsed.fragment or parsed.username or parsed.password):
        raise StorageError('The Redis REST URL must be an HTTPS origin.', code='redis_url_invalid')

  def command(self, *args):
    request = Request(
        self.url,
        data=json.dumps(args).encode('utf-8'),
        headers={'Authorization': f'Bearer {self.token}', 'Content-Type': 'application/json'},
        method='POST',
    )
    try:
      with urlopen(request, timeout=10) as response:
        payload = json.load(response)
    except HTTPError as exc:
      code = {401: 'redis_auth_failed', 403: 'redis_permission_denied', 429: 'redis_rate_limited'}.get(exc.code, 'redis_http_error')
      try:
        error_payload = json.loads(exc.read(4096))
        if isinstance(error_payload, dict) and 'error' in error_payload:
          classified = redis_error_code(error_payload['error'])
          if code == 'redis_http_error' and classified != 'redis_command_failed':
            code = classified
      except (ValueError, OSError):
        pass
      finally:
        exc.close()
      raise StorageError('Redis REST returned an HTTP error.', code=code) from None
    except (URLError, TimeoutError, OSError):
      raise StorageError('Redis REST could not be reached.', code='redis_request_failed') from None
    except ValueError:
      raise StorageError('Redis REST returned invalid JSON.', code='redis_response_invalid') from None
    if not isinstance(payload, dict) or ('result' not in payload and 'error' not in payload):
      raise StorageError('Redis REST returned an invalid response.', code='redis_response_invalid')
    if 'error' in payload:
      raise StorageError('Redis REST rejected the command.', code=redis_error_code(payload['error']))
    return payload['result']

  def load(self):
    if self.url:
      raw = self.command('GET', self.key)
      return json.loads(raw) if raw is not None else None
    if self.data_file.exists():
      with self.data_file.open(encoding='utf-8') as stream:
        return json.load(stream)
    return None

  def save(self, data):
    encoded = json.dumps(data, ensure_ascii=False)
    if self.url:
      if self._lock_token is None:
        raise StorageError('Redis state must be saved inside a transaction.')
      result = self.command(
          'EVAL', self.COMMIT, 2, self.key + ':lock', self.key,
          self._lock_token, encoded,
      )
      if result != 1:
        raise StorageError('The state lock expired; refusing to overwrite newer data.', code='storage_lock_expired')
      self._committed = True
    else:
      self.data_file.parent.mkdir(parents=True, exist_ok=True)
      temporary = self.data_file.with_suffix('.json.tmp')
      temporary.write_text(encoded, encoding='utf-8')
      temporary.replace(self.data_file)

  @contextmanager
  def transaction(self):
    # Serialize the legacy module's globals within one Python process as well.
    if not self._mutex.acquire(blocking=False):
      raise StorageBusy('Another update is being processed.')
    acquired = False
    failed = False
    self._committed = False
    try:
      if self.url:
        token = uuid.uuid4().hex
        if self.command('SET', self.key + ':lock', token, 'NX', 'EX', self.LOCK_SECONDS) != 'OK':
          raise StorageBusy('Another instance is processing an update.')
        self._lock_token = token
        acquired = True
      yield
    except BaseException:
      failed = True
      raise
    finally:
      try:
        if acquired:
          try:
            self.command('EVAL', self.RELEASE, 1, self.key + ':lock', self._lock_token)
          except StorageError as exc:
            # Cleanup must not replace a handler error or fail a durable commit.
            # The lock still expires automatically; subsequent updates retry.
            logger.warning('Lock cleanup failed: %s; committed=%s', exc.code, self._committed)
            if not failed and not self._committed:
              exc.stage = 'lock_release'
              raise
      finally:
        self._lock_token = None
        self._committed = False
        self._mutex.release()

  def _file_key(self, path):
    relative = Path(path).resolve().relative_to(self.sessions_dir.resolve())
    return self.key + ':file:' + relative.as_posix()

  def save_file(self, path):
    if self.url:
      encoded = base64.b64encode(Path(path).read_bytes()).decode('ascii')
      if self.command('SET', self._file_key(path), encoded) != 'OK':
        raise StorageError('Could not persist a session file.')

  def restore_file(self, path):
    if self.url:
      # Do not trust a stale file left by a different invocation.
      encoded = self.command('GET', self._file_key(path))
      if encoded is None:
        raise StorageError('A required session file is missing from durable storage.')
      target = Path(path)
      target.parent.mkdir(parents=True, exist_ok=True)
      target.write_bytes(base64.b64decode(encoded, validate=True))

# --- SHOP AND ADMIN HANDLERS ---

CHANNELS = [
    {
        "title": "Nhóm 1",
        "url": "https://t.me/+aOHaAaUITyFiOWI1",
    },
    {
        "title": "Kênh 2",
        "url": "https://t.me/aeanbam",
        "username": "@aeanbam",
    },
    {
        "title": "Kênh 3",
        "url": "https://t.me/aeanbam250",
        "username": "@aeanbam250",
    },
    {
        "title": "Kênh 4",
        "url": "https://t.me/stackriseearn",
        "username": "@stackriseearn",
    },
]


def load_data():
  data = store.load()
  return data if data is not None else default_data()


def default_data():
  return {
      "user_balances": {str(ADMIN_ID): 1000000},
      "user_ref_balances": {},
      "user_refs_count": {},
      "user_ref_deposit_valid": {},
      "user_total_deposits": {},
      "user_data": {},
      "verified_users": [],
      "banned_users": [],
      "giftcodes": {},
      "tele_products": {},
      "umo_products": {},
      "ff_products": {},
  }


def clean_empty_products():
  for p_dict in [tele_products, umo_products, ff_products]:
    empty_keys = [k for k, v in p_dict.items() if len(v.get("stock", [])) == 0]
    for k in empty_keys:
      del p_dict[k]


def save_data():
  clean_empty_products()
  # Existing handlers call this at intermediate steps. Commit once, together
  # with conversation state and update deduplication, after the handler returns.
  if not _processing_update:
    with store.transaction():
      store.save(snapshot_data())


def snapshot_data():
  return {
      "user_balances": user_balances,
      "user_ref_balances": user_ref_balances,
      "user_refs_count": user_refs_count,
      "user_ref_deposit_valid": user_ref_deposit_valid,
      "user_total_deposits": user_total_deposits,
      "user_data": user_data,
      "verified_users": list(verified_users),
      "banned_users": list(banned_users),
      "giftcodes": giftcodes,
      "tele_products": tele_products,
      "umo_products": umo_products,
      "ff_products": ff_products,
      "pending_deposits": pending_deposits,
      "pending_withdraws": pending_withdraws,
      "pending_buy": pending_buy,
      "admin_states": admin_states,
      "processed_updates": processed_updates,
  }


def restore_data(data):
  integer_key_fields = (
      "user_balances", "user_ref_balances", "user_refs_count",
      "user_ref_deposit_valid", "user_total_deposits", "user_data",
      "pending_deposits", "pending_withdraws", "pending_buy", "admin_states",
  )
  for name in integer_key_fields:
    globals()[name] = {int(k): v for k, v in data.get(name, {}).items()}
  for name in ("verified_users", "banned_users"):
    globals()[name] = {int(uid) for uid in data.get(name, [])}
  for name in ("giftcodes", "tele_products", "umo_products", "ff_products"):
    globals()[name] = data.get(name, {})
  globals()["processed_updates"] = data.get("processed_updates", {})
  clean_empty_products()




def process_webhook(bot_name, update):
  global _processing_update
  ensure_runtime()
  bot = {"shop": shop_bot, "admin": admin_bot}[bot_name]
  stage = "lock_acquire"
  try:
    with store.transaction():
      stage = "state_load"
      restore_data(load_data())
      recent = processed_updates.setdefault(bot_name, [])
      if update.update_id in recent:
        return False
      _processing_update = True
      try:
        stage = "handler"
        bot.process_new_updates([update])
        recent.append(update.update_id)
        del recent[:-2000]
        clean_empty_products()
        stage = "state_commit"
        store.save(snapshot_data())
        stage = "lock_release"
      finally:
        _processing_update = False
  except StorageError as exc:
    if exc.stage == "storage":
      exc.stage = stage
    raise
  return True


def restore_session_files(account):
  for name in ("session_path", "json_path"):
    store.restore_file(account[name])


# --- FIX LỖI EVENT LOOP & TELETHON DISCONNECT ---
async def check_single_session(session_path, api_id=None, api_hash=None):
  api_id = api_id or DEFAULT_API_ID
  api_hash = api_hash or DEFAULT_API_HASH
  
  if not session_path or not os.path.exists(session_path):
    return False, "File session không tồn tại"

  client = TelegramClient(session_path, api_id, api_hash, system_version="4.16.30-vx")
  try:
    await client.connect()
    
    if not await client.is_user_authorized():
      return False, "Unauthorized/Session Die"

    user = await client.get_me()
    if not user:
      return False, "No user data"

    if getattr(user, "deactivated", False) or getattr(user, "restricted", False):
      return False, "Deactivated/Frozen"

    try:
      async for _ in client.iter_dialogs(limit=1):
        break
    except Exception as inner_e:
      err_lower = str(inner_e).lower()
      if any(k in err_lower for k in ["frozen", "deactivated", "banned", "auth_key", "restricted", "USER_DEACTIVATED", "AUTH_KEY_UNREGISTERED"]):
        return False, f"Frozen/Invalid session: {inner_e}"

    return True, user

  except (
      AuthKeyUnregisteredError,
      UserDeactivatedBanError,
      SessionRevokedError,
  ):
    return False, "Banned/Deactivated/Invalid Session"
  except Exception as e:
    err_str = str(e).lower()
    if any(k in err_str for k in ["deactivated", "frozen", "ban", "authkey", "restricted", "flood", "invalid"]):
      return False, f"Frozen/Invalid: {e}"
    return False, str(e)
  finally:
    try:
      if client.is_connected():
        await client.disconnect()
    except Exception:
      pass


def run_async_check(session_path, api_id, api_hash):
  loop = asyncio.new_event_loop()
  asyncio.set_event_loop(loop)
  try:
    res = loop.run_until_complete(
        check_single_session(session_path, api_id, api_hash)
    )
  finally:
    loop.close()
  return res


def get_main_keyboard():
  markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
  markup.row("🛒 Mua Hàng", "💰 Nạp Tiền")
  markup.row("🔗 Link Ref", "🎁 Nhập Code")
  markup.row("🏆 Top Ref/Nạp", "ℹ️ Thông Tin")
  return markup


def get_cancel_keyboard():
  markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
  markup.row("❌ Hủy Thao Tác")
  return markup


def generate_qr_bytes(text_data):
  qr = qrcode.QRCode(
      version=1,
      error_correction=qrcode.constants.ERROR_CORRECT_L,
      box_size=10,
      border=4,
  )
  qr.add_data(text_data)
  qr.make(fit=True)
  img = qr.make_image(fill_color="black", back_color="white")
  img_bytes = io.BytesIO()
  img.save(img_bytes, format="JPEG")
  img_bytes.seek(0)
  return img_bytes


# --- SHOP BOT ---


def shop_link_info(message):
  text = (
      "📖 **CÁCH NUÔI ACC TELEGRAM & THÔNG TIN NHÓM:**\n\n"
      "👉 Tham gia nhóm hướng dẫn chi tiết tại đây:\n"
      "🔗 https://t.me/banacctele250"
  )
  try:
    shop_bot.send_message(
        message.chat.id,
        text,
        parse_mode="Markdown",
        disable_web_page_preview=False,
    )
  except Exception:
    pass


def shop_donacc(message):
  text = (
      "📦 **LINK NHÓM ĐƠN ACC:**\n\n"
      "👉 Bấm vào đây để tham gia nhóm:\n"
      "🔗 https://t.me/donacctele"
  )
  try:
    shop_bot.send_message(
        message.chat.id,
        text,
        parse_mode="Markdown",
        disable_web_page_preview=False,
    )
  except Exception:
    pass


def shop_start(message):
  if message.chat.type != "private":
    return
  uid = message.from_user.id
  if uid in banned_users:
    try:
      shop_bot.send_message(
          message.chat.id, "⛔ Tài khoản của bạn đã bị khóa (Ban)!"
      )
    except Exception:
      pass
    return

  args = message.text.split()
  if len(args) > 1 and args[1].startswith("ref_"):
    try:
      referrer_id = int(args[1].replace("ref_", ""))
      if referrer_id != uid and uid not in user_data:
        user_data.setdefault(uid, {})["referred_by"] = referrer_id
        save_data()
    except Exception:
      pass

  if uid in verified_users:
    verified_users.remove(uid)
    save_data()

  markup = types.InlineKeyboardMarkup()
  for ch in CHANNELS:
    markup.add(types.InlineKeyboardButton(f"📢 {ch['title']}", url=ch["url"]))
  markup.add(
      types.InlineKeyboardButton(
          "✅ Xác Minh Đã Join", callback_data="verify_join"
      )
  )

  text = (
      f"👋 Chào mừng {message.from_user.first_name} đến với hệ thống!\n\n"
      "⚠️ BẠN VUI LÒNG THAM GIA ĐỦ CÁC KÊNH / NHÓM SAU ĐÂY VÀ BẤM XÁC MINH:"
  )
  try:
    shop_bot.send_message(message.chat.id, text, reply_markup=markup)
  except Exception:
    pass


def verify_join_callback(call):
  uid = call.from_user.id
  if uid in banned_users:
    try:
      shop_bot.answer_callback_query(
          call.id, "⛔ Tài khoản đã bị khóa!", show_alert=True
      )
    except Exception:
      pass
    return

  not_joined = []
  for ch in CHANNELS:
    if "username" in ch:
      try:
        member = shop_bot.get_chat_member(ch["username"], uid)
        if member.status not in ["member", "administrator", "creator"]:
          not_joined.append(ch["title"])
      except Exception:
        pass

  if not_joined:
    try:
      shop_bot.answer_callback_query(
          call.id,
          "❌ Bạn chưa gia nhập đủ nhóm/kênh yêu cầu hoặc mạng chập chờn!",
          show_alert=True,
      )
    except Exception:
      pass
    return

  is_first_time_verified = uid not in verified_users
  verified_users.add(uid)
  now_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

  name = call.from_user.first_name
  username = (
      f"@{call.from_user.username}" if call.from_user.username else "Không có"
  )

  if uid not in user_data:
    user_data[uid] = {}
  user_data[uid]["join_date"] = now_str
  user_data[uid]["name"] = name
  user_data[uid]["username"] = username

  referrer_id = user_data[uid].get("referred_by")
  ref_info_text = "Không có"

  if referrer_id:
    ref_name = user_data.get(referrer_id, {}).get("name", f"User {referrer_id}")
    ref_uname = user_data.get(referrer_id, {}).get("username", "Không có")
    ref_info_text = f"`{ref_name}` ({ref_uname} - ID: `{referrer_id}`)"

    if is_first_time_verified and not user_data[uid].get("ref_counted", False):
      user_data[uid]["ref_counted"] = True
      user_refs_count[referrer_id] = user_refs_count.get(referrer_id, 0) + 1
      
      # Random thưởng từ 100 VNĐ đến 500 VNĐ cộng vào số dư VNĐ
      ref_bonus_vnd = random.randint(100, 500)
      user_balances[referrer_id] = user_balances.get(referrer_id, 0) + ref_bonus_vnd
      save_data()

      try:
        shop_bot.send_message(
            referrer_id,
            f"🎉 **CÓ REF MỚI THÀNH CÔNG!**\n\n"
            f"👤 Thành viên `{name}` ({username}) đã hoàn tất xác minh.\n"
            f"💵 Thưởng nóng: **+{ref_bonus_vnd:,} VNĐ** vào số dư!",
            parse_mode="Markdown",
        )
      except Exception:
        pass

  save_data()
  try:
    shop_bot.answer_callback_query(call.id, "Xác minh thành công!")
    shop_bot.edit_message_text(
        "🎉 Xác minh thành công! Cảm ơn bạn đã tham gia các kênh.",
        call.message.chat.id,
        call.message.message_id,
    )
    shop_bot.send_message(
        call.message.chat.id,
        "👇 Chọn chức năng bên dưới:",
        reply_markup=get_main_keyboard(),
    )

    notify_admin_text = (
        f"📢 **THÔNG BÁO XÁC MINH MỚI!**\n\n"
        f"👤 User: `{name}` ({username})\n"
        f"🆔 ID: `{uid}`\n"
        f"🔗 Ref của: {ref_info_text}\n"
        f"⏰ Ngày giờ: `{now_str}`"
    )
    admin_bot.send_message(ADMIN_ID, notify_admin_text, parse_mode="Markdown")
  except Exception:
    pass


def handle_shop_text(message):
  if message.chat.type != "private":
    return
  uid = message.from_user.id
  if uid in banned_users:
    try:
      shop_bot.reply_to(message, "⛔ Tài khoản của bạn đã bị khóa!")
    except Exception:
      pass
    return
  if uid not in verified_users:
    try:
      shop_bot.reply_to(
          message,
          "⚠️ Bạn chưa xác minh tham gia nhóm! Vui lòng ấn /start để làm lại.",
      )
    except Exception:
      pass
    return

  text = message.text.strip() if message.text else ""

  if text == "❌ Hủy Thao Tác":
    pending_deposits.pop(uid, None)
    pending_buy.pop(uid, None)
    try:
      shop_bot.send_message(
          message.chat.id,
          "❌ Đã hủy thao tác.",
          reply_markup=get_main_keyboard(),
      )
    except Exception:
      pass
    return

  if "Mua Hàng" in text:
    pending_deposits.pop(uid, None)
    clean_empty_products()
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton(
            "📱 Acc Telegram", callback_data="shop_cat_tele"
        )
    )
    markup.add(
        types.InlineKeyboardButton(
            "🎁 Code Umo", callback_data="shop_cat_umo"
        ),
        types.InlineKeyboardButton("🎮 Acc FF", callback_data="shop_cat_ff"),
    )
    try:
      shop_bot.send_message(
          message.chat.id,
          "📂 **CHỌN DANH MỤC MUA HÀNG:**",
          reply_markup=markup,
          parse_mode="Markdown",
      )
    except Exception:
      pass
    return

  elif "Top Ref/Nạp" in text:
    pending_deposits.pop(uid, None)
    sorted_refs = sorted(
        user_refs_count.items(), key=lambda x: x[1], reverse=True
    )[:10]
    ref_text = "🏆 **TOP 10 GIỚI THIỆU (REF):**\n"
    if not sorted_refs:
      ref_text += "Chưa có dữ liệu.\n"
    else:
      for idx, (u_id, cnt) in enumerate(sorted_refs, 1):
        u_name = (
            user_data.get(int(u_id), {})
            .get("name", f"User {u_id}")
            .replace("*", "")
        )
        ref_text += f"{idx}. {u_name} - **{cnt}** ref\n"

    sorted_deposits = sorted(
        user_total_deposits.items(), key=lambda x: x[1], reverse=True
    )[:10]
    dep_text = "\n🏆 **TOP 10 NẠP TIỀN:**\n"
    if not sorted_deposits:
      dep_text += "Chưa có dữ liệu.\n"
    else:
      for idx, (u_id, amt) in enumerate(sorted_deposits, 1):
        u_name = (
            user_data.get(int(u_id), {})
            .get("name", f"User {u_id}")
            .replace("*", "")
        )
        dep_text += f"{idx}. {u_name} - **{amt:,} VNĐ**\n"

    try:
      shop_bot.send_message(
          message.chat.id,
          ref_text + dep_text,
          reply_markup=get_main_keyboard(),
          parse_mode="Markdown",
      )
    except Exception:
      pass
    return

  elif "Nạp Tiền" in text:
    pending_buy.pop(uid, None)
    try:
      shop_bot.send_message(
          message.chat.id,
          "💵 Vui lòng nhập số tiền bạn muốn nạp (Tối thiểu **5,000 VNĐ**):",
          reply_markup=get_cancel_keyboard(),
          parse_mode="Markdown",
      )
    except Exception:
      pass
    pending_deposits[uid] = "WAITING_AMOUNT"
    return

  elif "Link Ref" in text:
    pending_deposits.pop(uid, None)
    pending_buy.pop(uid, None)
    bot_info = shop_bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{uid}"
    count = user_refs_count.get(uid, 0)
    try:
      shop_bot.send_message(
          message.chat.id,
          f"🔗 **LINK REF (1F = 100 VNĐ - 500 VNĐ vào số dư):**\n\n"
          f"`{ref_link}`\n\n"
          f"👥 Tổng số 1F đã mời: **{count}** người",
          parse_mode="Markdown",
      )
    except Exception:
      pass
    return

  elif "Nhập Code" in text:
    pending_deposits.pop(uid, None)
    pending_buy.pop(uid, None)
    try:
      shop_bot.send_message(
          message.chat.id,
          "🎁 Vui lòng nhập mã Giftcode của bạn:",
          reply_markup=get_cancel_keyboard(),
      )
    except Exception:
      pass
    pending_buy[uid] = {"step": "WAITING_GIFTCODE"}
    return

  elif "Thông Tin" in text:
    pending_deposits.pop(uid, None)
    pending_buy.pop(uid, None)
    u_info = user_data.get(uid, {})
    name = message.from_user.first_name
    bal_vnd = user_balances.get(uid, 0)
    count = user_refs_count.get(uid, 0)
    join_date = u_info.get("join_date", "Chưa xác định")
    info_msg = (
        f"ℹ️ **TÀI KHOẢN**\n\n"
        f"👤 Tên: {name}\n"
        f"🆔 ID: `{uid}`\n"
        f"💵 Số dư VNĐ: {bal_vnd:,} VNĐ\n"
        f"👥 Số 1F đã mời: {count}\n"
        f"📅 Ngày tham gia: {join_date}"
    )
    try:
      shop_bot.send_message(message.chat.id, info_msg, parse_mode="Markdown")
    except Exception:
      pass
    return

  if uid in pending_buy and pending_buy[uid].get("step") == "WAITING_GIFTCODE":
    code_input = text.strip().upper()
    pending_buy.pop(uid, None)

    if code_input in giftcodes:
      g_info = giftcodes[code_input]
      if g_info["usage_left"] > 0:
        if uid in g_info.get("used_by", []):
          shop_bot.send_message(
              message.chat.id,
              "❌ Bạn đã từng nhập mã giftcode này rồi!",
              reply_markup=get_main_keyboard(),
          )
          return

        bonus = g_info["amount"]
        g_info["usage_left"] -= 1
        g_info.setdefault("used_by", []).append(uid)
        user_balances[uid] = user_balances.get(uid, 0) + bonus
        save_data()

        shop_bot.send_message(
            message.chat.id,
            f"🎉 **NHẬP GIFTCODE THÀNH CÔNG!**\n\n🎁 Bạn đã nhận:"
            f" **+{bonus:,} VNĐ** vào tài khoản.\n💵 Số dư hiện tại:"
            f" **{user_balances[uid]:,} VNĐ**",
            reply_markup=get_main_keyboard(),
            parse_mode="Markdown",
        )

        try:
          u_name = message.from_user.first_name
          admin_bot.send_message(
              ADMIN_ID,
              f"🎁 **THÔNG BÁO CODE:** User `{u_name}` (ID: `{uid}`) vừa nhập"
              f" thành công mã `{code_input}` (Thưởng: {bonus:,} VNĐ)",
              parse_mode="Markdown",
          )
        except Exception:
          pass
      else:
        shop_bot.send_message(
            message.chat.id,
            "❌ Mã giftcode này đã hết lượt sử dụng!",
            reply_markup=get_main_keyboard(),
        )
    else:
      shop_bot.send_message(
          message.chat.id,
          "❌ Mã giftcode không hợp lệ hoặc không tồn tại!",
          reply_markup=get_main_keyboard(),
      )
    return

  if uid in pending_buy and pending_buy[uid].get("step") == "WAITING_QTY":
    try:
      qty = int(text)
      cat = pending_buy[uid]["cat"]
      p_id = pending_buy[uid]["p_id"]

      if cat == "tele":
        product = tele_products.get(p_id)
      elif cat == "umo":
        product = umo_products.get(p_id)
      else:
        product = ff_products.get(p_id)

      if not product:
        shop_bot.send_message(
            message.chat.id,
            "❌ Sản phẩm không tồn tại!",
            reply_markup=get_main_keyboard(),
        )
        pending_buy.pop(uid, None)
        return

      stock_len = len(product["stock"])
      if qty <= 0 or qty > stock_len:
        shop_bot.send_message(
            message.chat.id,
            f"❌ Số lượng không hợp lệ! Kho chỉ còn {stock_len} sản phẩm.",
            reply_markup=get_cancel_keyboard(),
        )
        return

      total_price = qty * product["price"]
      pending_buy[uid] = {
          "step": "CONFIRM_BUY",
          "cat": cat,
          "p_id": p_id,
          "qty": qty,
          "total_price": total_price,
      }

      confirm_markup = types.InlineKeyboardMarkup()
      confirm_markup.add(
          types.InlineKeyboardButton(
              "✅ Xác Nhận Mua", callback_data="confirm_buy_yes"
          ),
          types.InlineKeyboardButton(
              "❌ Hủy", callback_data="confirm_buy_no"
          ),
      )
      shop_bot.send_message(
          message.chat.id,
          f"📦 **XÁC NHẬN GIAO DỊCH**\n\n- Sản phẩm: **{product['name']}**\n- Số"
          f" lượng: **{qty}**\n- Tổng tiền: **{total_price:,} VNĐ**",
          reply_markup=confirm_markup,
          parse_mode="Markdown",
      )
    except Exception:
      shop_bot.send_message(
          message.chat.id,
          "❌ Vui lòng nhập số lượng hợp lệ:",
          reply_markup=get_cancel_keyboard(),
      )
    return

  if uid in pending_deposits and pending_deposits[uid] == "WAITING_AMOUNT":
    try:
      amount = int(
          text.replace(".", "").replace(",", "").replace("VNĐ", "").strip()
      )
      if amount < 5000:
        shop_bot.send_message(
            message.chat.id,
            "❌ Số tiền tối thiểu từ **5,000 VNĐ**!",
            reply_markup=get_cancel_keyboard(),
            parse_mode="Markdown",
        )
        return

      qr_caption = (
          f"💳 **THÔNG TIN CHUYỂN KHOẢN**\n\n"
          f"🏦 Ngân hàng: **{BANK_NAME}**\n"
          f"🔢 Số tài khoản: `{BANK_STK}`\n"
          f"👤 Chủ tài khoản: **{ACCOUNT_NAME}**\n"
          f"💵 Số tiền: **{amount:,} VNĐ**\n\n"
          f"⚠️ Gửi ảnh bill chuyển khoản vào đây để duyệt!"
      )
      if os.path.exists(QR_IMAGE_PATH):
        with open(QR_IMAGE_PATH, "rb") as qr_photo:
          shop_bot.send_photo(
              message.chat.id,
              qr_photo,
              caption=qr_caption,
              reply_markup=get_cancel_keyboard(),
              parse_mode="Markdown",
          )
      else:
        shop_bot.send_photo(
            message.chat.id,
            generate_qr_bytes(f"STK:{BANK_STK}|AMT:{amount}"),
            caption=qr_caption,
            reply_markup=get_cancel_keyboard(),
            parse_mode="Markdown",
        )
      pending_deposits[uid] = amount
    except Exception:
      shop_bot.send_message(
          message.chat.id,
          "❌ Số tiền không hợp lệ!",
          reply_markup=get_cancel_keyboard(),
      )
    return

  if uid in pending_deposits and isinstance(pending_deposits[uid], int):
    if message.content_type == "photo":
      amount = pending_deposits[uid]
      admin_caption = (
          f"📥 **NẠP TIỀN MỚI!**\n🆔 `{uid}`\n💵 **{amount:,} VNĐ**"
      )
      markup_admin = types.InlineKeyboardMarkup()
      markup_admin.add(
          types.InlineKeyboardButton(
              "✅ Duyệt", callback_data=f"approve_{uid}_{amount}"
          ),
          types.InlineKeyboardButton(
              "❌ Hủy", callback_data=f"reject_{uid}_{amount}"
          ),
      )
      try:
        file_info = shop_bot.get_file(message.photo[-1].file_id)
        admin_bot.send_photo(
            ADMIN_ID,
            shop_bot.download_file(file_info.file_path),
            caption=admin_caption,
            reply_markup=markup_admin,
            parse_mode="Markdown",
        )
        shop_bot.send_message(
            message.chat.id,
            "✅ Đã gửi bill cho Admin!",
            reply_markup=get_main_keyboard(),
        )
      except Exception:
        pass
      pending_deposits.pop(uid, None)
    return


def callback_shop_categories(call):
  uid = call.from_user.id
  data = call.data

  if data == "shop_cat_tele":
    clean_empty_products()
    markup = types.InlineKeyboardMarkup()
    for p_id, p_info in tele_products.items():
      markup.add(
          types.InlineKeyboardButton(
              f"📱 {p_info['name']} - {p_info['price']:,}đ (Còn:"
              f" {len(p_info['stock'])})",
              callback_data=f"buy_cat_tele_{p_id}",
          )
      )
    shop_bot.edit_message_text(
        "📂 **CHỌN MỤC ACC TELEGRAM:**",
        call.message.chat.id,
        call.message.message_id,
        reply_markup=markup,
    )
    return

  if data == "shop_cat_umo":
    clean_empty_products()
    markup = types.InlineKeyboardMarkup()
    for p_id, p_info in umo_products.items():
      markup.add(
          types.InlineKeyboardButton(
              f"🎁 {p_info['name']} - Giá: {p_info['price']:,}đ (Còn:"
              f" {len(p_info['stock'])})",
              callback_data=f"buy_cat_umo_{p_id}",
          )
      )
    if not umo_products:
      shop_bot.answer_callback_query(
          call.id, "❌ Tạm hết Code Umo!", show_alert=True
      )
      return
    shop_bot.edit_message_text(
        "📂 **CHỌN DANH MỤC CODE UMO:**",
        call.message.chat.id,
        call.message.message_id,
        reply_markup=markup,
    )
    return

  if data == "shop_cat_ff":
    clean_empty_products()
    markup = types.InlineKeyboardMarkup()
    for p_id, p_info in ff_products.items():
      markup.add(
          types.InlineKeyboardButton(
              f"🎮 {p_info['name']} - Giá: {p_info['price']:,}đ (Còn:"
              f" {len(p_info['stock'])})",
              callback_data=f"buy_cat_ff_{p_id}",
          )
      )
    if not ff_products:
      shop_bot.answer_callback_query(
          call.id, "❌ Tạm hết Acc FF!", show_alert=True
      )
      return
    shop_bot.edit_message_text(
        "📂 **CHỌN DANH MỤC ACC FF:**",
        call.message.chat.id,
        call.message.message_id,
        reply_markup=markup,
    )
    return

  if data.startswith("buy_cat_"):
    parts = data.replace("buy_cat_", "").split("_", 1)
    cat = parts[0]
    p_id = parts[1]

    if cat == "tele":
      product = tele_products.get(p_id)
    elif cat == "umo":
      product = umo_products.get(p_id)
    else:
      product = ff_products.get(p_id)

    if not product or len(product["stock"]) == 0:
      shop_bot.answer_callback_query(
          call.id, "❌ Sản phẩm đã hết hàng!", show_alert=True
      )
      return

    pending_buy[uid] = {"cat": cat, "p_id": p_id, "step": "WAITING_QTY"}
    try:
      shop_bot.answer_callback_query(call.id)
      shop_bot.send_message(
          call.message.chat.id,
          f"📦 **{product['name']}**\n- Giá: **{product['price']:,} VNĐ/1**\n-"
          f" Số lượng còn lại: **{len(product['stock'])}**\n\n👉 Nhập **số"
          " lượng** bạn muốn mua:",
          reply_markup=get_cancel_keyboard(),
          parse_mode="Markdown",
      )
    except Exception:
      pass


def callback_confirm_buy(call):
  uid = call.from_user.id
  if uid not in pending_buy or pending_buy[uid].get("step") != "CONFIRM_BUY":
    try:
      shop_bot.answer_callback_query(
          call.id, "❌ Giao dịch hết hạn!", show_alert=True
      )
    except Exception:
      pass
    return

  if call.data == "confirm_buy_no":
    pending_buy.pop(uid, None)
    try:
      shop_bot.answer_callback_query(call.id, "❌ Đã hủy.")
      shop_bot.edit_message_text(
          "❌ Giao dịch đã hủy.",
          call.message.chat.id,
          call.message.message_id,
      )
      shop_bot.send_message(
          call.message.chat.id, "Chọn menu:", reply_markup=get_main_keyboard()
      )
    except Exception:
      pass
    return

  buy_info = pending_buy.pop(uid)
  cat = buy_info["cat"]
  p_id = buy_info["p_id"]
  qty = buy_info["qty"]
  total_price = buy_info["total_price"]

  if cat == "tele":
    product = tele_products.get(p_id)
  elif cat == "umo":
    product = umo_products.get(p_id)
  else:
    product = ff_products.get(p_id)

  user_bal = user_balances.get(uid, 0)
  if user_bal < total_price:
    try:
      shop_bot.answer_callback_query(
          call.id, "❌ Số dư không đủ!", show_alert=True
      )
    except Exception:
      pass
    return

  user_balances[uid] = user_bal - total_price
  save_data()

  try:
    shop_bot.answer_callback_query(call.id, "✅ Đang xử lý...")
    shop_bot.delete_message(call.message.chat.id, call.message.message_id)
  except Exception:
    pass

  buyer_name = call.from_user.first_name
  buyer_username = (
      f"@{call.from_user.username}" if call.from_user.username else f"ID: {uid}"
  )

  if cat == "tele":
    progress_msg = shop_bot.send_message(
        uid,
        f"⏳ Đang kiểm tra acc (vòng 1)...\nTổng sống: 0 / Mục tiêu {qty}",
    )

    collected_live = []
    vong = 1

    while len(collected_live) < qty and len(product["stock"]) > 0:
      need_more = qty - len(collected_live)
      take_batch = [
          product["stock"].pop(0)
          for _ in range(min(need_more, len(product["stock"])))
      ]

      live_this_round = 0
      die_this_round = 0

      for idx, acc in enumerate(take_batch, 1):
        session_p = acc.get("session_path")
        json_p = acc.get("json_path")

        if session_p and os.path.exists(session_p):
          api_id, api_hash = DEFAULT_API_ID, DEFAULT_API_HASH
          if json_p and os.path.exists(json_p):
            try:
              with open(json_p, "r", encoding="utf-8") as jf:
                jdata = json.load(jf)
                api_id = jdata.get("app_id", DEFAULT_API_ID)
                api_hash = jdata.get("app_hash", DEFAULT_API_HASH)
            except Exception:
              pass

          is_live, _ = run_async_check(session_p, api_id, api_hash)
          if is_live:
            collected_live.append(acc)
            live_this_round += 1
          else:
            die_this_round += 1
            try:
              if os.path.exists(session_p):
                os.remove(session_p)
              if json_p and os.path.exists(json_p):
                os.remove(json_p)
            except Exception:
              pass
        else:
          die_this_round += 1

        msg_text = (
            f"🔍 Đang kiểm tra (vòng {vong})\n"
            f"🟢 Sống vòng này: {live_this_round} | 🔴 Lỗi/Die: {die_this_round}\n"
            f"📦 Tổng acc sống: {len(collected_live)}/{qty}"
        )
        try:
          shop_bot.edit_message_text(
              msg_text,
              chat_id=uid,
              message_id=progress_msg.message_id,
          )
        except Exception:
          pass

      vong += 1
      save_data()

    if len(collected_live) < qty:
      actual_count = len(collected_live)
      actual_price = actual_count * product["price"]
      refund = total_price - actual_price

      if refund > 0:
        user_balances[uid] = user_balances.get(uid, 0) + refund
        save_data()
        shop_bot.send_message(
            uid,
            f"⚠️ Kho không đủ acc sống! Đã hoàn lại **{refund:,} VNĐ** cho"
            f" **{qty - actual_count}** acc thiếu.",
        )

      if actual_count == 0:
        shop_bot.send_message(
            uid,
            "❌ Rất tiếc, kho hiện tại tạm hết acc sống. Tiền đã được hoàn lại"
            " vào tài khoản!",
        )
        return

      qty = actual_count
      total_price = actual_price

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
      for acc in collected_live:
        if os.path.exists(acc["session_path"]):
          zip_file.write(
              acc["session_path"],
              arcname=os.path.basename(acc["session_path"]),
          )
        if os.path.exists(acc["json_path"]):
          zip_file.write(
              acc["json_path"], arcname=os.path.basename(acc["json_path"])
          )
    zip_buffer.seek(0)

    current_date_str = datetime.now().strftime("%d/%m")
    zip_file_name = f"done {qty} acc {current_date_str}.zip"

    caption_text = (
        f"🎉 **MUA THÀNH CÔNG!**\n\n"
        f"📦 Số lượng acc sống: **{qty}**\n"
        f"💵 Số tiền: **{total_price:,} VNĐ**\n"
        f"🔑 Mật khẩu: `tele0212`\n\n"
        f"👉 Nếu gặp lỗi hãy liên hệ Admin: @Vuaanbam250"
    )

    shop_bot.send_document(
        uid,
        zip_buffer,
        visible_file_name=zip_file_name,
        caption=caption_text,
        reply_markup=get_main_keyboard(),
        parse_mode="Markdown",
    )

    # Thông báo đơn hàng vào nhóm Đơn acc với các icon đẹp mắt
    try:
      log_msg = (
          f"🛒 **THÔNG TIN ĐƠN HÀNG MỚI (TELEGRAM)**\n\n"
          f"🏷️ **Tên sản phẩm:** {product['name']}\n"
          f"📦 **Số lượng mua:** {qty}\n"
          f"👤 **Tên / Username:** {buyer_username} ({buyer_name})\n"
          f"💵 **Số tiền:** {total_price:,} VNĐ\n"
          f"⏰ **Thời gian:** {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
      )
      shop_bot.send_message(
          DON_MUA_GROUP_CHAT_ID, log_msg, parse_mode="Markdown"
      )
    except Exception:
      pass

  elif cat == "umo":
    codes_given = []
    for _ in range(qty):
      if product["stock"]:
        codes_given.append(product["stock"].pop(0))
    save_data()

    codes_text = "\n".join(codes_given)
    msg = (
        f"🎉 **MUA CODE UMO THÀNH CÔNG!**\n"
        f"📦 Số lượng: {qty}\n"
        f"💵 Trừ: {total_price:,} VNĐ\n\n"
        f"🔑 **DANH SÁCH MÃ:**\n`{codes_text}`\n\n"
        f"👉 Nếu gặp lỗi hãy liên hệ Admin: @Vuaanbam250"
    )
    shop_bot.send_message(
        uid, msg, reply_markup=get_main_keyboard(), parse_mode="Markdown"
    )

    try:
      log_msg = (
          f"🛒 **THÔNG TIN ĐƠN HÀNG MỚI (CODE UMO)**\n\n"
          f"🏷️ **Tên sản phẩm:** {product['name']}\n"
          f"📦 **Số lượng mua:** {qty}\n"
          f"👤 **Tên / Username:** {buyer_username} ({buyer_name})\n"
          f"💵 **Số tiền:** {total_price:,} VNĐ\n"
          f"⏰ **Thời gian:** {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
      )
      shop_bot.send_message(
          DON_MUA_GROUP_CHAT_ID, log_msg, parse_mode="Markdown"
      )
    except Exception:
      pass

  elif cat == "ff":
    ff_given = []
    for _ in range(qty):
      if product["stock"]:
        ff_given.append(product["stock"].pop(0))
    save_data()

    acc_text = "\n".join([f"• `{item}`" for item in ff_given])
    msg = (
        f"🎉 **MUA ACC FF LV15 THÀNH CÔNG!**\n"
        f"📦 Số lượng: {qty}\n"
        f"💵 Trừ: {total_price:,} VNĐ\n\n"
        f"🔐 **THÔNG TIN TÀI KHOẢN:**\n{acc_text}\n\n"
        f"👉 Nếu gặp lỗi hãy liên hệ Admin: @Vuaanbam250"
    )
    shop_bot.send_message(
        uid, msg, reply_markup=get_main_keyboard(), parse_mode="Markdown"
    )

    try:
      log_msg = (
          f"🛒 **THÔNG TIN ĐƠN HÀNG MỚI (ACC FF)**\n\n"
          f"🏷️ **Tên sản phẩm:** {product['name']}\n"
          f"📦 **Số lượng mua:** {qty}\n"
          f"👤 **Tên / Username:** {buyer_username} ({buyer_name})\n"
          f"💵 **Số tiền:** {total_price:,} VNĐ\n"
          f"⏰ **Thời gian:** {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
      )
      shop_bot.send_message(
          DON_MUA_GROUP_CHAT_ID, log_msg, parse_mode="Markdown"
      )
    except Exception:
      pass


# --- ADMIN BOT ---


def get_admin_menu():
  markup = types.InlineKeyboardMarkup(row_width=2)
  markup.add(
      types.InlineKeyboardButton("➕ Cộng Tiền", callback_data="adm_add_menu"),
      types.InlineKeyboardButton("➖ Trừ Tiền", callback_data="adm_sub_menu"),
  )
  markup.add(
      types.InlineKeyboardButton(
          "📱 Thêm Acc Telegram", callback_data="adm_add_tele"
      ),
      types.InlineKeyboardButton(
          "➕ Thêm Code Umo & FF", callback_data="adm_add_umo_ff_menu"
      ),
  )
  markup.add(
      types.InlineKeyboardButton("📦 Kho Hàng", callback_data="adm_stock"),
      types.InlineKeyboardButton("🧹 Lọc Acc", callback_data="adm_filter_acc"),
  )
  markup.add(
      types.InlineKeyboardButton("🎁 Thêm Giftcode", callback_data="adm_add_code"),
      types.InlineKeyboardButton("🚫 Ban User", callback_data="adm_ban"),
  )
  markup.add(
      types.InlineKeyboardButton("✅ Unban User", callback_data="adm_unban")
  )
  return markup


# Lệnh /tb gửi thông báo toàn bộ người dùng
def admin_broadcast(message):
  msg_text = message.text.replace("/tb", "").strip()
  if not msg_text:
    admin_bot.reply_to(
        message, "⚠️ Cú pháp: `/tb <Nội dung thông báo>`", parse_mode="Markdown"
    )
    return

  success = 0
  failed = 0
  all_users = set(user_balances.keys()).union(verified_users)

  admin_bot.send_message(
      message.chat.id, f"⏳ Đang gửi thông báo tới {len(all_users)} user..."
  )

  for u_id in all_users:
    try:
      shop_bot.send_message(
          u_id,
          f"📢 **THÔNG BÁO TỪ HỆ THỐNG:**\n\n{msg_text}",
          parse_mode="Markdown",
      )
      success += 1
    except Exception:
      failed += 1

  admin_bot.send_message(
      message.chat.id,
      f"✅ **ĐÃ GỬI THÔNG BÁO XONG!**\n- Thành công: **{success}**\n- Thất"
      f" bại: **{failed}**",
      parse_mode="Markdown",
  )


def admin_resettop(message):
  args = message.text.split()
  if len(args) < 2:
    admin_bot.reply_to(
        message,
        "⚠️ Cú pháp không đúng!\n- `/resettop ref` (Reset top giới thiệu)\n- `/resettop dep` (Reset top nạp tiền)\n- `/resettop all` (Reset cả hai)",
        parse_mode="Markdown"
    )
    return

  target = args[1].lower()
  global user_refs_count, user_total_deposits

  event_name = ""
  if target == "ref":
    user_refs_count.clear()
    event_name = "Top Giới Thiệu (Ref)"
  elif target == "dep":
    user_total_deposits.clear()
    event_name = "Top Nạp Tiền"
  elif target == "all":
    user_refs_count.clear()
    user_total_deposits.clear()
    event_name = "Toàn Bộ Bảng Xếp Hạng (Ref & Nạp)"
  else:
    admin_bot.reply_to(message, "❌ Lựa chọn không hợp lệ! Dùng: `ref`, `dep` hoặc `all`.", parse_mode="Markdown")
    return

  save_data()
  admin_bot.reply_to(message, f"✅ Đã reset thành công **{event_name}**!")

  all_users = set(user_balances.keys()).union(verified_users)
  notif_text = (
      f"🏆 **THÔNG BÁO SỰ KIỆN / RESET TOP!**\n\n"
      f"✨ Bảng vàng **{event_name}** đã chính thức được làm mới (Reset)!\n"
      f"🚀 Mời anh em tiếp tục đua top để nhận những phần quà hấp dẫn từ hệ thống nhé!"
  )
  for u_id in all_users:
    try:
      shop_bot.send_message(u_id, notif_text, parse_mode="Markdown")
    except Exception:
      pass


def admin_menu_cmd(message):
  text = (
      "📋 **DANH SÁCH LỆNH QUẢN LÝ:**\n\n"
      "• /start - Mở bảng điều khiển admin chính\n"
      "• /menu - Hiển thị danh sách lệnh này\n"
      "• /resettop ref - Reset bảng xếp hạng giới thiệu (Ref)\n"
      "• /resettop dep - Reset bảng xếp hạng nạp tiền\n"
      "• /resettop all - Reset toàn bộ bảng xếp hạng\n"
      "• /tb <nội dung> - Gửi thông báo đến toàn bộ người dùng"
  )
  try:
    admin_bot.send_message(message.chat.id, text, parse_mode="Markdown")
  except Exception:
    pass


def admin_start(message):
  if message.chat.type != "private" or message.from_user.id != ADMIN_ID:
    return
  try:
    admin_bot.send_message(
        message.chat.id,
        "👑 **BẢNG ĐIỀU KHIỂN ADMIN**",
        reply_markup=get_admin_menu(),
        parse_mode="Markdown",
    )
  except Exception:
    pass


def admin_menu_handler(call):
  if call.from_user.id != ADMIN_ID:
    return
  cid = call.message.chat.id
  mid = call.message.message_id
  action = call.data

  clean_empty_products()

  if action == "adm_stock":
    text = "📦 **KHO HÀNG HIỆN CÓ:**\n\n"
    for p_id, p_info in tele_products.items():
      text += f"• [Tele] {p_info['name']}: {len(p_info['stock'])} acc\n"
    for p_id, p_info in umo_products.items():
      text += f"• [Umo] {p_info['name']}: {len(p_info['stock'])} code\n"
    for p_id, p_info in ff_products.items():
      text += f"• [FF] {p_info['name']}: {len(p_info['stock'])} acc\n"
    try:
      admin_bot.answer_callback_query(call.id)
      admin_bot.edit_message_text(
          text, cid, mid, reply_markup=get_admin_menu(), parse_mode="Markdown"
      )
    except Exception:
      pass
    return

  if action == "adm_add_code":
    admin_states[ADMIN_ID] = {"step": "WAITING_CODE_NAME"}
    admin_bot.send_message(cid, "🎁 Nhập **Mã Giftcode** muốn tạo:")
    return

  if action == "adm_add_umo_ff_menu":
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton(
            "🎁 Thêm Code Umo", callback_data="add_umo_ff_umo"
        )
    )
    markup.add(
        types.InlineKeyboardButton(
            "🎮 Thêm Acc FF", callback_data="add_umo_ff_ff"
        )
    )
    admin_bot.edit_message_text(
        "➕ **CHỌN LOẠI CẦN THÊM:**", cid, mid, reply_markup=markup
    )
    return

  if action == "add_umo_ff_umo":
    admin_states[ADMIN_ID] = {"type": "umo", "step": "WAITING_NAME"}
    admin_bot.send_message(cid, "🎁 Nhập **Tên mục Code Umo**:")
    return

  if action == "add_umo_ff_ff":
    admin_states[ADMIN_ID] = {"type": "ff", "step": "WAITING_NAME"}
    admin_bot.send_message(
        cid, "🎮 Nhập **Tên sản phẩm Acc FF** (log Google):"
    )
    return

  if action == "adm_filter_acc":
    if not tele_products:
      admin_bot.answer_callback_query(
          call.id, "❌ Kho hiện không có mục Tele nào!", show_alert=True
      )
      return
    markup = types.InlineKeyboardMarkup()
    for p_id, p_info in tele_products.items():
      markup.add(
          types.InlineKeyboardButton(
              f"📱 {p_info['name']} ({len(p_info['stock'])} acc)",
              callback_data=f"filter_target_{p_id}",
          )
      )
    admin_bot.edit_message_text(
        "🧹 **CHỌN MỤC ACC TELEGRAM CẦN LỌC:**", cid, mid, reply_markup=markup
    )
    return

  if action.startswith("filter_target_"):
    p_id = action.replace("filter_target_", "")
    if p_id not in tele_products:
      admin_bot.answer_callback_query(call.id, "❌ Mục không tồn tại!")
      return
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton(
            "🔍 Lọc & Xóa Acc Die", callback_data=f"act_filter_die_{p_id}"
        )
    )
    markup.add(
        types.InlineKeyboardButton(
            "✂️ Xóa Theo Số Lượng", callback_data=f"act_filter_qty_{p_id}"
        )
    )
    admin_bot.edit_message_text(
        f"⚙️ Tùy chọn lọc cho mục: **{tele_products[p_id]['name']}**",
        cid,
        mid,
        reply_markup=markup,
        parse_mode="Markdown",
    )
    return

  if action.startswith("act_filter_die_"):
    p_id = action.replace("act_filter_die_", "")
    admin_bot.answer_callback_query(call.id, "⏳ Đang quét acc die...")
    product = tele_products[p_id]
    live_stock = []
    die_count = 0
    for acc in product["stock"]:
      if os.path.exists(acc["session_path"]):
        api_id, api_hash = DEFAULT_API_ID, DEFAULT_API_HASH
        if os.path.exists(acc["json_path"]):
          try:
            with open(acc["json_path"], "r") as jf:
              jdata = json.load(jf)
              api_id = jdata.get("app_id", DEFAULT_API_ID)
              api_hash = jdata.get("app_hash", DEFAULT_API_HASH)
          except Exception:
            pass
        is_live, _ = run_async_check(acc["session_path"], api_id, api_hash)
        if is_live:
          live_stock.append(acc)
        else:
          die_count += 1
      else:
        die_count += 1
    product["stock"] = live_stock
    save_data()
    admin_bot.send_message(
        cid,
        f"✅ Đã quét xong! Sống: {len(live_stock)} | Xóa die: {die_count}",
    )
    return

  if action.startswith("act_filter_qty_"):
    p_id = action.replace("act_filter_qty_", "")
    admin_states[ADMIN_ID] = {"step": "WAITING_DEL_QTY", "p_id": p_id}
    admin_bot.send_message(cid, "👉 Nhập **số lượng** acc muốn xóa bớt:")
    return

  if action == "adm_add_tele":
    admin_states[ADMIN_ID] = {"step": "WAITING_TELE_NAME"}
    admin_bot.send_message(cid, "📱 Nhập **Tên gói Acc Telegram**:")
    return

  if action == "adm_add_menu":
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("💵 Cộng VNĐ", callback_data="step_add_vnd"),
    )
    admin_bot.edit_message_text(
        "➕ Chọn loại tiền:", cid, mid, reply_markup=markup
    )
    return

  if action == "adm_sub_menu":
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("💵 Trừ VNĐ", callback_data="step_sub_vnd"),
    )
    admin_bot.edit_message_text(
        "➖ Chọn loại tiền:", cid, mid, reply_markup=markup
    )
    return

  states_map = {
      "step_add_vnd": ("WAITING_UID", "ADD_VND", "💵 CỘNG VNĐ"),
      "step_sub_vnd": ("WAITING_UID", "SUB_VND", "💵 TRỪ VNĐ"),
  }
  if action in states_map:
    step, op_type, title = states_map[action]
    admin_states[ADMIN_ID] = {"step": step, "type": op_type}
    admin_bot.send_message(cid, f"📌 **{title}**\n👉 Nhập ID user:")
    return

  if action == "adm_ban":
    admin_states[ADMIN_ID] = {"step": "WAITING_BAN"}
    admin_bot.send_message(cid, "🚫 Nhập ID cần Ban:")
  elif action == "adm_unban":
    admin_states[ADMIN_ID] = {"step": "WAITING_UNBAN"}
    admin_bot.send_message(cid, "✅ Nhập ID cần Unban:")


def admin_input_handler(message):
  if message.chat.type != "private":
    return
  st = admin_states.get(ADMIN_ID)
  if not st:
    return

  step = st.get("step")
  text = message.text.strip() if message.text else ""

  try:
    if step == "WAITING_CODE_NAME":
      st["code_name"] = text
      st["step"] = "WAITING_CODE_AMT"
      admin_bot.send_message(
          message.chat.id, "👉 Nhập **số tiền thưởng** cho mã này (VNĐ):"
      )
      return

    if step == "WAITING_CODE_AMT":
      st["code_amt"] = int(text.replace(".", "").replace(",", "").strip())
      st["step"] = "WAITING_CODE_LIMIT"
      admin_bot.send_message(
          message.chat.id, "👉 Nhập **số lượt sử dụng tối đa**:"
      )
      return

    if step == "WAITING_CODE_LIMIT":
      limit = int(text)
      code_name = st.get("code_name")
      code_amt = st.get("code_amt")
      admin_states.pop(ADMIN_ID, None)

      giftcodes[code_name] = {
          "amount": code_amt,
          "usage_left": limit,
          "used_by": [],
      }
      save_data()
      admin_bot.send_message(
          message.chat.id,
          f"🎉 **ĐÃ TẠO GIFTCODE THÀNH CÔNG!**\n\n- Mã: `{code_name}`\n-"
          f" Thưởng: **{code_amt:,} VNĐ**\n- Số lượt dùng: **{limit}**",
          parse_mode="Markdown",
      )
      return

    if step == "WAITING_DEL_QTY":
      del_qty = int(text)
      p_id = st.get("p_id")
      admin_states.pop(ADMIN_ID, None)
      if p_id in tele_products:
        prod = tele_products[p_id]
        for _ in range(del_qty):
          if prod["stock"]:
            prod["stock"].pop(0)
        save_data()
        admin_bot.send_message(
            message.chat.id, f"✅ Đã xóa bớt {del_qty} acc khỏi mục Telegram!"
        )
      return

    if step == "WAITING_NAME":
      st["name"] = text
      st["step"] = "WAITING_PRICE"
      admin_bot.send_message(message.chat.id, "👉 Nhập **giá bán (VNĐ)**:")
      return

    if step == "WAITING_PRICE":
      st["price"] = int(text.replace(".", "").replace(",", "").strip())
      prod_type = st.get("type")
      if prod_type == "umo":
        st["step"] = "WAITING_UMO_LIST"
        admin_bot.send_message(
            message.chat.id,
            "👉 Gửi danh sách mã code Umo (Mỗi dòng 1 mã):",
            parse_mode="Markdown",
        )
      elif prod_type == "ff":
        st["step"] = "WAITING_FF_LIST"
        admin_bot.send_message(
            message.chat.id,
            "👉 Gửi danh sách tài khoản & mật khẩu FF log Google (Mỗi dòng 1"
            " tài khoản):",
            parse_mode="Markdown",
        )
      return

    if step == "WAITING_UMO_LIST":
      name = st.get("name")
      price = st.get("price")
      admin_states.pop(ADMIN_ID, None)
      codes = [line.strip() for line in text.split("\n") if line.strip()]
      p_id = str(int(datetime.now().timestamp()))
      umo_products[p_id] = {"name": name, "price": price, "stock": codes}
      save_data()
      admin_bot.send_message(
          message.chat.id,
          f"🎉 Thêm thành công **{len(codes)}** mã Code Umo vào shop!",
          parse_mode="Markdown",
      )
      return

    if step == "WAITING_FF_LIST":
      name = st.get("name")
      price = st.get("price")
      admin_states.pop(ADMIN_ID, None)
      accs = [line.strip() for line in text.split("\n") if line.strip()]
      p_id = str(int(datetime.now().timestamp()))
      ff_products[p_id] = {"name": name, "price": price, "stock": accs}
      save_data()
      admin_bot.send_message(
          message.chat.id,
          f"🎉 Thêm thành công **{len(accs)}** tài khoản FF vào shop!",
          parse_mode="Markdown",
      )
      return

    if step == "WAITING_TELE_NAME":
      st["tele_name"] = text
      st["step"] = "WAITING_TELE_PRICE"
      admin_bot.send_message(message.chat.id, "👉 Nhập **giá bán / 1 acc (VNĐ)**:")
      return

    if step == "WAITING_TELE_PRICE":
      st["tele_price"] = int(text.replace(".", "").replace(",", "").strip())
      st["step"] = "WAITING_TELE_ZIP"
      admin_bot.send_message(
          message.chat.id, "👉 Gửi file **.zip** chứa session & json:"
      )
      return

    if step == "WAITING_TELE_ZIP":
      if (
          message.content_type != "document"
          or not message.document.file_name.endswith(".zip")
      ):
        admin_bot.send_message(message.chat.id, "❌ Vui lòng gửi file .zip!")
        return
      name = st.get("tele_name")
      price = st.get("tele_price")
      admin_states.pop(ADMIN_ID, None)

      file_info = admin_bot.get_file(message.document.file_id)
      temp_dir = os.path.join(SESSIONS_DIR, "temp_admin")
      os.makedirs(temp_dir, exist_ok=True)
      zip_ref = zipfile.ZipFile(
          io.BytesIO(admin_bot.download_file(file_info.file_path))
      )
      zip_ref.extractall(temp_dir)

      p_id = str(int(datetime.now().timestamp()))
      dest_dir = os.path.join(SESSIONS_DIR, p_id)
      os.makedirs(dest_dir, exist_ok=True)

      extracted_accs = []
      for root, dirs, files in os.walk(temp_dir):
        for f in files:
          if f.endswith(".session"):
            base = os.path.splitext(f)[0]
            sess_p = os.path.join(root, f)
            json_p = os.path.join(root, f"{base}.json")
            if os.path.exists(json_p):
              d_sess = os.path.join(dest_dir, f)
              d_json = os.path.join(dest_dir, f"{base}.json")
              shutil.copy(sess_p, d_sess)
              shutil.copy(json_p, d_json)
              extracted_accs.append(
                  {"session_path": d_sess, "json_path": d_json}
              )

      shutil.rmtree(temp_dir, ignore_errors=True)
      total_up = len(extracted_accs)

      progress_msg = admin_bot.send_message(
          message.chat.id,
          f"Đã kiểm tra 0/{total_up} (vòng 1)\nCòn sống vòng này: 0 | Hỏng:"
          f" 0\nTổng sống: 0, mục tiêu {total_up}",
      )

      live_accs = []
      die_accs = []

      for idx, acc in enumerate(extracted_accs, 1):
        api_id, api_hash = DEFAULT_API_ID, DEFAULT_API_HASH
        if os.path.exists(acc["json_path"]):
          try:
            with open(acc["json_path"], "r") as jf:
              jdata = json.load(jf)
              api_id = jdata.get("app_id", DEFAULT_API_ID)
              api_hash = jdata.get("app_hash", DEFAULT_API_HASH)
          except Exception:
            pass

        is_live, _ = run_async_check(acc["session_path"], api_id, api_hash)
        if is_live:
          live_accs.append(acc)
        else:
          die_accs.append(acc)

        msg_text = (
            f"Đã kiểm tra {idx}/{total_up} (vòng 1)\n"
            f"Còn sống vòng này: {len(live_accs)} | Hỏng: {len(die_accs)}\n"
            f"Tổng sống: {len(live_accs)}, mục tiêu {total_up}"
        )
        try:
          admin_bot.edit_message_text(
              msg_text,
              chat_id=message.chat.id,
              message_id=progress_msg.message_id,
          )
        except Exception:
          pass

      if die_accs:
        die_zip_buffer = io.BytesIO()
        with zipfile.ZipFile(die_zip_buffer, "w", zipfile.ZIP_DEFLATED) as die_zip:
          for d_acc in die_accs:
            if os.path.exists(d_acc["session_path"]):
              die_zip.write(d_acc["session_path"], arcname=os.path.basename(d_acc["session_path"]))
            if os.path.exists(d_acc["json_path"]):
              die_zip.write(d_acc["json_path"], arcname=os.path.basename(d_acc["json_path"]))
        die_zip_buffer.seek(0)
        
        current_time_str = datetime.now().strftime("%d%m_%H%M%S")
        die_zip_name = f"acc_die_{current_time_str}.zip"
        try:
          admin_bot.send_document(
              message.chat.id,
              die_zip_buffer,
              visible_file_name=die_zip_name,
              caption=f"⚠️ Danh sách **{len(die_accs)}** tài khoản bị hỏng / đóng băng / không hợp lệ:",
              parse_mode="Markdown"
          )
        except Exception:
          pass

        for d_acc in die_accs:
          try:
            if os.path.exists(d_acc["session_path"]):
              os.remove(d_acc["session_path"])
            if os.path.exists(d_acc["json_path"]):
              os.remove(d_acc["json_path"])
          except Exception:
            pass

      tele_products[p_id] = {"name": name, "price": price, "stock": live_accs}
      save_data()

      admin_bot.send_message(
          message.chat.id,
          f"🎉 **ĐÃ TẢI LÊN HOÀN TẤT!**\n- Tổng nhận: **{total_up}** acc\n- Acc"
          f" sống (đã thêm vào shop): **{len(live_accs)}**\n- Acc die/đóng"
          f" băng (đã gửi file riêng): **{len(die_accs)}**",
          parse_mode="Markdown",
      )
      return

    if step == "WAITING_UID":
      st["target_uid"] = int(text)
      st["step"] = "WAITING_AMOUNT"
      admin_bot.send_message(message.chat.id, "👉 Nhập số lượng cần cộng/trừ:")
      return

    if step == "WAITING_AMOUNT":
      amount = int(text)
      op = st.get("type")
      uid = st.get("target_uid")
      admin_states.pop(ADMIN_ID, None)

      if op == "ADD_VND":
        user_balances[uid] = user_balances.get(uid, 0) + amount
        try:
          shop_bot.send_message(
              uid,
              f"💰 **THÔNG BÁO TÀI KHOẢN:** Bạn vừa được Admin **cộng"
              f" +{amount:,} VNĐ**.\n💵 Số dư hiện tại: **{user_balances[uid]:,}"
              " VNĐ**",
              parse_mode="Markdown",
          )
        except Exception:
          pass
      elif op == "SUB_VND":
        user_balances[uid] = max(0, user_balances.get(uid, 0) - amount)
        try:
          shop_bot.send_message(
              uid,
              f"⚠️ **THÔNG BÁO TÀI KHOẢN:** Tài khoản của bạn vừa bị Admin **trừ"
              f" -{amount:,} VNĐ**.\n💵 Số dư hiện tại: **{user_balances[uid]:,}"
              " VNĐ**",
              parse_mode="Markdown",
          )
        except Exception:
          pass

      save_data()
      admin_bot.send_message(message.chat.id, "✅ Thực hiện thành công!")
      return

    if step == "WAITING_BAN":
      target_ban_uid = int(text)
      banned_users.add(target_ban_uid)
      save_data()
      admin_bot.send_message(
          message.chat.id, f"✅ Đã ban thành công user `{target_ban_uid}`"
      )
      try:
        shop_bot.send_message(
            target_ban_uid,
            "⛔ **Tài khoản của bạn đã bị khóa (Ban) bởi quản trị viên!**",
            parse_mode="Markdown",
        )
      except Exception:
        pass
    elif step == "WAITING_UNBAN":
      target_unban_uid = int(text)
      banned_users.discard(target_unban_uid)
      save_data()
      admin_bot.send_message(
          message.chat.id, f"✅ Đã unban thành công user `{target_unban_uid}`"
      )
      try:
        shop_bot.send_message(
            target_unban_uid,
            "✅ **Tài khoản của bạn đã được mở khóa (Unban)!** Chúc bạn mua sắm"
            " vui vẻ.",
            parse_mode="Markdown",
        )
      except Exception:
        pass
    admin_states.pop(ADMIN_ID, None)

  except Exception as e:
    admin_bot.send_message(message.chat.id, f"❌ Lỗi: {e}")
    admin_states.pop(ADMIN_ID, None)


def admin_handle_approvals(call):
  if call.from_user.id != ADMIN_ID:
    return
  parts = call.data.split("_")
  action = parts[0]
  target_uid = int(parts[1])
  amount = int(parts[2])

  try:
    admin_bot.answer_callback_query(call.id)
  except Exception:
    pass

  if action == "approve":
    user_balances[target_uid] = user_balances.get(target_uid, 0) + amount
    user_total_deposits[target_uid] = (
        user_total_deposits.get(target_uid, 0) + amount
    )

    u_info = user_data.get(target_uid, {})
    u_username = u_info.get("username", "Không có")

    ref_by = u_info.get("referred_by")
    if ref_by:
      if amount >= 10000:
        user_ref_deposit_valid[ref_by] = (
            user_ref_deposit_valid.get(ref_by, 0) + 1
        )

    save_data()
    try:
      formatted_approval_msg = (
          f"✅ **ĐÃ DUYỆT NẠP THÀNH CÔNG**\n\n"
          f"👤 **Khách hàng:** `{u_username}`\n"
          f"🆔 **ID Telegram:** `{target_uid}`\n"
          f"💵 **Số tiền nạp:** `{amount:,} VNĐ`"
      )
      admin_bot.edit_message_caption(
          formatted_approval_msg,
          call.message.chat.id,
          call.message.message_id,
          parse_mode="Markdown",
      )
      shop_bot.send_message(
          target_uid,
          f"🎉 **NẠP TIỀN THÀNH CÔNG!**\n\n💵 Cộng vào tài khoản: **+{amount:,} VNĐ**",
          reply_markup=get_main_keyboard(),
          parse_mode="Markdown"
      )
    except Exception:
      pass
  else:
    try:
      u_info = user_data.get(target_uid, {})
      u_username = u_info.get("username", "Không có")
      admin_bot.edit_message_caption(
          f"❌ **ĐÃ TỪ CHỐI NẠP TIỀN**\n\n👤 Khách hàng: `{u_username}`\n🆔 ID: `{target_uid}`",
          call.message.chat.id,
          call.message.message_id,
          parse_mode="Markdown",
      )
      shop_bot.send_message(
          target_uid,
          "❌ Đơn nạp tiền của bạn đã bị từ chối bởi Quản trị viên.",
          reply_markup=get_main_keyboard(),
      )
    except Exception:
      pass



class BotConfigurationError(RuntimeError):
  def __init__(self, code):
    super().__init__('Bot runtime configuration failed.')
    self.code = code


def required_environment(name):
  value = os.environ.get(name, '').strip()
  if not value:
    raise BotConfigurationError('bot_config_missing')
  return value


def ensure_runtime():
  """Initialize once, with no Telegram requests and no local module imports."""
  global _runtime_initialized, _processing_update
  global SHOP_TOKEN, ADMIN_TOKEN, ADMIN_ID, DEFAULT_API_ID, DEFAULT_API_HASH
  global BANK_NAME, BANK_STK, ACCOUNT_NAME, QR_IMAGE_PATH, DATA_FILE, SESSIONS_DIR
  global DON_MUA_GROUP_CHAT_ID
  global shop_bot, admin_bot, store, qrcode, types
  global TelegramClient, AuthKeyUnregisteredError, UserDeactivatedBanError
  with _runtime_lock:
    if _runtime_initialized:
      return
    SHOP_TOKEN = required_environment('SHOP_TOKEN')
    ADMIN_TOKEN = required_environment('ADMIN_TOKEN')
    try:
      ADMIN_ID = int(required_environment('ADMIN_ID'))
      DEFAULT_API_ID = int(os.environ.get('TELEGRAM_API_ID') or 0)
    except ValueError:
      raise BotConfigurationError('bot_config_invalid') from None
    DEFAULT_API_HASH = os.environ.get('TELEGRAM_API_HASH', '')
    BANK_NAME = os.environ.get('BANK_NAME', 'Vietcombank')
    BANK_STK = os.environ.get('BANK_STK', '9974775827')
    ACCOUNT_NAME = os.environ.get('ACCOUNT_NAME', 'VO QUOC HUY')
    DON_MUA_GROUP_CHAT_ID = os.environ.get('DON_MUA_GROUP_CHAT_ID', '-1004462598264')
    QR_IMAGE_PATH = str(PROJECT_DIR / 'tl' / 'qr.jpg')
    data_dir = Path(os.environ.get('BOT_DATA_DIR') or (
        '/tmp/tl' if os.environ.get('VERCEL') else str(PROJECT_DIR / 'tl')
    ))
    DATA_FILE = str(data_dir / 'bot_data.json')
    SESSIONS_DIR = str(data_dir / 'sessions_storage')
    try:
      store = StateStore(DATA_FILE, SESSIONS_DIR)
    except StorageError as exc:
      exc.stage = 'runtime_init'
      raise
    import qrcode
    import telebot
    from telebot import types
    from telethon import TelegramClient
    from telethon.errors import (
        AuthKeyUnregisteredError,
        SessionRevokedError,
        UserDeactivatedBanError,
    )
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    restore_data(default_data())
    _processing_update = False
    shop_bot = telebot.TeleBot(SHOP_TOKEN, threaded=False)
    admin_bot = telebot.TeleBot(ADMIN_TOKEN, threaded=False)
    shop_bot.message_handler(commands=["link"])(shop_link_info)
    shop_bot.message_handler(commands=["donacc", "donmua"])(shop_donacc)
    shop_bot.message_handler(commands=["start"])(shop_start)
    shop_bot.callback_query_handler(func=lambda call: call.data == "verify_join")(verify_join_callback)
    shop_bot.message_handler(content_types=["text", "photo"])(handle_shop_text)
    shop_bot.callback_query_handler(
        func=lambda call: call.data.startswith(("shop_cat_", "buy_cat_"))
    )(callback_shop_categories)
    shop_bot.callback_query_handler(
        func=lambda call: call.data in ["confirm_buy_yes", "confirm_buy_no"]
    )(callback_confirm_buy)
    admin_bot.message_handler(
        commands=["tb"], func=lambda message: message.from_user.id == ADMIN_ID
    )(admin_broadcast)
    admin_bot.message_handler(
        commands=["resettop"], func=lambda message: message.from_user.id == ADMIN_ID
    )(admin_resettop)
    admin_bot.message_handler(
        commands=["menu"], func=lambda message: message.from_user.id == ADMIN_ID
    )(admin_menu_cmd)
    admin_bot.message_handler(commands=["start"])(admin_start)
    admin_bot.callback_query_handler(
        func=lambda call: call.data.startswith(
            (
                "adm_",
                "step_",
                "gencode_",
                "filter_target_",
                "act_filter_",
                "add_umo_ff_",
                "approve_wd_",
                "reject_wd_",
            )
        )
    )(admin_menu_handler)
    admin_bot.message_handler(
        content_types=["text", "document"],
        func=lambda message: message.from_user.id == ADMIN_ID,
    )(admin_input_handler)
    admin_bot.callback_query_handler(
        func=lambda call: call.data.startswith(("approve_", "reject_"))
    )(admin_handle_approvals)
    _runtime_initialized = True


# --- HTTP WEBHOOK ---

def webhook_secret():
  secret = os.environ.get('WEBHOOK_SECRET', '')
  if not re.fullmatch(r'[A-Za-z0-9_-]{1,256}', secret):
    raise ValueError('Configure WEBHOOK_SECRET using Telegram-supported characters.')
  return secret


class handler(BaseHTTPRequestHandler):
  def authenticated(self):
    try:
      expected_secret = webhook_secret()
    except ValueError:
      logger.error('Webhook configuration failure: webhook_secret_not_configured')
      self.respond(503, {'ok': False, 'error': 'Webhook secret is not configured', 'code': 'webhook_secret_not_configured'})
      return False
    supplied = self.headers.get('X-Telegram-Bot-Api-Secret-Token', '')
    if not hmac.compare_digest(supplied.encode('utf-8'), expected_secret.encode('utf-8')):
      self.respond(403, {'ok': False, 'error': 'Forbidden'})
      return False
    return True

  def respond(self, status, payload, headers=None, head=False):
    body = json.dumps(payload).encode('utf-8')
    self.send_response(status)
    self.send_header('Content-Type', 'application/json; charset=utf-8')
    self.send_header('Content-Length', str(len(body)))
    self.send_header('Cache-Control', 'no-store')
    self.send_header('X-Bot-Version', APP_VERSION)
    for name, value in (headers or {}).items():
      self.send_header(name, value)
    self.end_headers()
    if not head:
      self.wfile.write(body)

  def do_GET(self, head=False):
    parsed = urlsplit(self.path)
    if parsed.path.rstrip('/') not in ('', '/api', '/api/health', '/api/index', '/api/index.py'):
      self.respond(404, {'ok': False, 'error': 'Not found'}, head=head)
      return
    if not head and parse_qs(parsed.query).get('check') == ['storage']:
      if self.authenticated():
        self.check_storage()
      return
    # Liveness only: does not import the bot, expose secrets, or call Telegram.
    self.respond(200, {'ok': True, 'service': 'telegram-webhooks', 'version': APP_VERSION}, head=head)

  def check_storage(self):
    # Authenticated, read-only check of the deployed environment. Do not import
    # the bots, create files, acquire the production lock, or modify bot state.
    try:
      root = Path('/tmp/tl')
      store = StateStore(root / 'bot_data.json', root / 'sessions_storage')
      if not store.url:
        raise StorageError('Redis is not configured.', code='redis_not_configured')
      if store.command('PING') != 'PONG' or store.command('EVAL', 'return 1', 0) != 1:
        raise StorageError('Redis returned an unexpected result.', code='redis_response_invalid')
    except StorageError as exc:
      logger.error('Storage check failed: %s', exc.code)
      self.respond(503, {'ok': False, 'error': 'Storage check failed', 'code': exc.code, 'version': APP_VERSION})
      return
    self.respond(200, {'ok': True, 'storage': 'redis', 'check': 'read-only', 'version': APP_VERSION})

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
    if not self.authenticated():
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
      processed = process_webhook(bot_name, update)
    except StorageError as exc:
      logger.error('Webhook storage failure: bot=%s code=%s stage=%s update_id=%s', bot_name, exc.code, exc.stage, update.update_id)
      self.respond(503, {'ok': False, 'error': 'Storage temporarily unavailable', 'code': exc.code, 'stage': exc.stage}, {'Retry-After': '2'})
      return
    except BotConfigurationError as exc:
      logger.error('Bot initialization failed: code=%s', exc.code)
      self.respond(503, {'ok': False, 'error': 'Bot initialization failed', 'code': exc.code, 'stage': 'runtime_init'})
      return
    except (KeyError, ValueError, ImportError) as exc:
      logger.error('Bot initialization or update handling failed: bot=%s exception=%s update_id=%s', bot_name, type(exc).__name__, update.update_id)
      self.respond(500, {'ok': False, 'error': 'Bot configuration or update handling failed'})
      return
    except Exception as exc:
      # Do not log Telegram API exception text: it may contain credentials or user data.
      logger.error('Webhook processing failed: bot=%s exception=%s update_id=%s', bot_name, type(exc).__name__, update.update_id)
      self.respond(500, {'ok': False, 'error': 'Update processing failed'})
      return
    self.respond(200, {'ok': True, 'duplicate': not processed})


# --- WEBHOOK SETUP (LOCAL CLI ONLY) ---

def telegram_request(token, method, payload):
  request = Request(
      f'https://api.telegram.org/bot{token}/{method}',
      data=json.dumps(payload).encode('utf-8'),
      headers={'Content-Type': 'application/json'}, method='POST',
  )
  with urlopen(request, timeout=30) as response:
    result = json.load(response)
  if not isinstance(result, dict) or not result.get('ok'):
    raise RuntimeError('Telegram did not accept the request.')
  return result.get('result')


def public_origin(base):
  base = base.strip().rstrip('/')
  parsed = urlsplit(base)
  if (parsed.scheme != 'https' or not parsed.netloc or parsed.path
      or parsed.query or parsed.fragment or parsed.username or parsed.password):
    raise ValueError('The webhook URL must be a public HTTPS origin.')
  return base


def deployment_preflight(base, secret):
  """Verify this version and Redis before changing Telegram registration."""
  request = Request(
      base + '/api/health?check=storage',
      headers={'X-Telegram-Bot-Api-Secret-Token': secret},
  )
  with urlopen(request, timeout=30) as response:
    result = json.load(response)
  if not isinstance(result, dict) or result.get('version') != APP_VERSION:
    raise RuntimeError('Deploy the single-file version before registering its webhooks.')
  if not result.get('ok') or result.get('storage') != 'redis':
    raise RuntimeError('The deployed storage check did not pass.')


def show_webhook_status(name, status):
  # Telegram's provider text may include sensitive URLs; print only safe fields.
  parsed = urlsplit(status.get('url', ''))
  url = f'{parsed.scheme}://{parsed.hostname}{parsed.path}' if parsed.hostname else '(not set)'
  pending = status.get('pending_update_count', 0)
  print(f'{name}: url={url}, pending={pending}')
  if status.get('last_error_message'):
    print(f"{name}: Telegram recorded a delivery error at Unix time {status.get('last_error_date', 'unknown')}; inspect the pending count and Vercel logs.")


def configure_webhooks(base, drop_pending_updates=False):
  base = public_origin(base)
  secret = webhook_secret()
  tokens = {name: required_environment(f'{name.upper()}_TOKEN') for name in ('shop', 'admin')}
  deployment_preflight(base, secret)
  for name, token in tokens.items():
    telegram_request(token, 'setWebhook', {
        'url': f'{base}/api/webhook/{name}',
        'secret_token': secret,
        'allowed_updates': ['message', 'callback_query'],
        'max_connections': 1,
        'drop_pending_updates': drop_pending_updates,
    })
    if name == 'shop':
      telegram_request(token, 'setMyCommands', {'commands': [
          {'command': 'start', 'description': 'Mở Cửa Hàng'},
          {'command': 'link', 'description': 'Cách nuôi acc'},
      ]})
    show_webhook_status(name, telegram_request(token, 'getWebhookInfo', {}))


def main(argv=None):
  parser = argparse.ArgumentParser(description=__doc__)
  actions = parser.add_mutually_exclusive_group()
  actions.add_argument('--set-webhooks', action='store_true', help='Verify the deployment, then register both bots.')
  actions.add_argument('--webhook-info', action='store_true', help='Read current registration and queue status.')
  parser.add_argument('--url', help='Public production origin; defaults to WEBHOOK_BASE_URL.')
  parser.add_argument('--drop-pending-updates', action='store_true', help='Explicitly discard queued updates during registration.')
  args = parser.parse_args(argv)
  if args.drop_pending_updates and not args.set_webhooks:
    parser.error('--drop-pending-updates requires --set-webhooks')
  if args.url and not args.set_webhooks:
    parser.error('--url requires --set-webhooks')
  if args.set_webhooks or args.webhook_info:
    try:
      if args.set_webhooks:
        configure_webhooks(args.url or required_environment('WEBHOOK_BASE_URL'), args.drop_pending_updates)
      else:
        for name in ('shop', 'admin'):
          token = required_environment(f'{name.upper()}_TOKEN')
          show_webhook_status(name, telegram_request(token, 'getWebhookInfo', {}))
    except Exception as exc:
      # Neither exception URLs nor response bodies are safe to echo here.
      raise SystemExit(f'Webhook action failed ({type(exc).__name__}). Check deployment version, safe storage diagnostics, and connectivity.') from None
    return
  port = int(os.environ.get('PORT', '8000'))
  print(f'Webhook server listening on http://127.0.0.1:{port}')
  ThreadingHTTPServer(('127.0.0.1', port), handler).serve_forever()


if __name__ == '__main__':
  main()
