"""Import an existing JSON database and its session files into Redis REST storage."""

import argparse
import json
import os
from pathlib import Path

from tl.storage import StateStore


def migrate(data_file, session_root, overwrite=False):
  data_file = Path(data_file).resolve()
  session_root = Path(session_root).resolve()
  store = StateStore(data_file, session_root)
  if not store.url:
    raise ValueError('Configure UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN.')
  data = json.loads(data_file.read_text(encoding='utf-8'))
  # Vercel always recreates files below this writable temporary directory.
  remote_root = Path('/tmp/tl/sessions_storage')
  files = []
  for product in data.get('tele_products', {}).values():
    for account in product.get('stock', []):
      for field in ('session_path', 'json_path'):
        source = Path(account[field])
        if not source.is_absolute():
          source = data_file.parent / source
        source = source.resolve()
        relative = source.relative_to(session_root)
        if not source.is_file():
          raise FileNotFoundError(source)
        files.append(source)
        account[field] = str(remote_root / relative)
  with store.transaction():
    if store.load() is not None and not overwrite:
      raise ValueError('Redis already contains state. Use --overwrite only for an intentional replacement.')
    for source in files:
      store.save_file(source)
    store.save(data)
  return len(files)


def main():
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('data_file', type=Path)
  parser.add_argument('--sessions-dir', type=Path)
  parser.add_argument('--overwrite', action='store_true')
  args = parser.parse_args()
  if os.environ.get('BOT_DATA_DIR'):
    parser.error('Unset BOT_DATA_DIR when migrating to the default Vercel /tmp/tl location.')
  root = args.sessions_dir or args.data_file.parent / 'sessions_storage'
  count = migrate(args.data_file, root, args.overwrite)
  print(f'Migrated bot state and {count} files. Deploy and register webhooks next.')


if __name__ == '__main__':
  main()