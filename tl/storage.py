"""Local JSON storage, or durable Redis REST storage on Vercel."""

import base64
from contextlib import contextmanager
import json
import os
from pathlib import Path
import threading
import uuid
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class StorageError(RuntimeError):
  pass


class StorageBusy(StorageError):
  pass


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
    self.url = os.environ.get('UPSTASH_REDIS_REST_URL', '').rstrip('/')
    self.token = os.environ.get('UPSTASH_REDIS_REST_TOKEN', '')
    self.key = os.environ.get('BOT_STORAGE_KEY', 'tl:state')
    self._mutex = threading.Lock()
    self._lock_token = None
    if bool(self.url) != bool(self.token):
      raise StorageError('Set both UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN.')
    if os.environ.get('VERCEL') and not self.url:
      raise StorageError('Vercel requires durable Redis REST storage; local files are temporary.')
    if self.url and not self.url.startswith('https://'):
      raise StorageError('UPSTASH_REDIS_REST_URL must use HTTPS.')

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
    except (HTTPError, URLError, TimeoutError, ValueError) as exc:
      raise StorageError('Redis REST request failed.') from exc
    if not isinstance(payload, dict) or 'error' in payload or 'result' not in payload:
      raise StorageError('Redis REST returned an error or an invalid response.')
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
        raise StorageError('The state lock expired; refusing to overwrite newer data.')
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
    try:
      if self.url:
        token = uuid.uuid4().hex
        if self.command('SET', self.key + ':lock', token, 'NX', 'EX', self.LOCK_SECONDS) != 'OK':
          raise StorageBusy('Another instance is processing an update.')
        self._lock_token = token
        acquired = True
      yield
    finally:
      try:
        if acquired:
          self.command('EVAL', self.RELEASE, 1, self.key + ':lock', self._lock_token)
      finally:
        self._lock_token = None
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