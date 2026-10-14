# Single-file Telegram bots on Vercel

All application code is now in **`api/index.py`**: the shop/admin handlers,
durable storage, HTTP webhook server, diagnostics, and webhook registration
command. There are no imports from separate local bot/storage modules, no
compressed/embedded source, and no polling loop. Vercel loads the standard
library `handler` class and dispatches updates synchronously to the right bot:

| URL | Purpose |
| --- | --- |
| `GET /api/health` | Liveness check (does not verify Telegram or Redis connectivity) |
| `GET /api/health?check=storage` | Authenticated, read-only Redis connectivity/scripting check |
| `POST /api/webhook/shop` | Shop bot updates |
| `POST /api/webhook/admin` | Admin bot updates |

The current production URLs are:

- Shop: `https://tl-wine.vercel.app/api/webhook/shop`
- Admin: `https://tl-wine.vercel.app/api/webhook/admin`

`requirements.txt` installs the existing third-party dependencies, and
`vercel.json` maps both URLs to this one function. `tl/qr.jpg` is an optional
payment QR asset; if it is absent, the existing QR generation fallback is used.
The scripts directory contains only optional compatibility/migration utilities,
not runtime dependencies.

## 1. Replace exposed credentials

**The original source and `tl.zip` contained hard-coded bot tokens. Revoke both
tokens with BotFather and create replacements before deploying.** They remain
in the original archive/Git history even though the active source now reads
environment variables. The original archive is excluded from the function
bundle and CLI uploads; only the empty `public/` directory is used for static
hosting, so source files and archives are not served. Do not publish the
archive elsewhere. Review the exposed Telegram API credentials as well.

## 2. Configure durable storage and environment variables

Create an Upstash Redis database (the Vercel Marketplace integration also
works). Use its HTTPS REST URL and **read/write** REST token. Do not use its
read-only token. Set the following in Vercel **Project Settings → Environment
Variables** for the production environment:

| Variable | Required | Meaning |
| --- | --- | --- |
| `SHOP_TOKEN` | Yes | New shop bot token |
| `ADMIN_TOKEN` | Yes | New admin bot token |
| `ADMIN_ID` | Yes | Your numeric Telegram user ID |
| `WEBHOOK_SECRET` | Yes | Secret Telegram sends in its webhook header |
| `UPSTASH_REDIS_REST_URL` | Yes on Vercel | Redis HTTPS REST endpoint |
| `UPSTASH_REDIS_REST_TOKEN` | Yes on Vercel | Redis read/write REST token |
| `BOT_STORAGE_KEY` | No | State namespace; defaults to `tl:state` |
| `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` | For session checks | Telethon credentials |
| `BANK_NAME`, `BANK_STK`, `ACCOUNT_NAME` | No | Override the existing bank settings |

The alternative `KV_REST_API_URL` / `KV_REST_API_TOKEN` pair is also supported
when neither `UPSTASH_REDIS_REST_*` variable is set. A partially configured
primary pair is rejected rather than mixed with another database's credentials.
Use an HTTPS REST origin, not a `redis://` connection string.

**Local `.env` values are not uploaded to Vercel.** Set these variables for
**Production** and redeploy after every environment-variable change. Values
configured only for Development or Preview do not affect the production bot.

Generate `WEBHOOK_SECRET` with:

```sh
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

The secret may contain only letters, digits, `_` and `-`, and must be 1–256
characters. Keep it and the Redis credentials private. Use separate Redis
namespaces for preview/test deployments; never point test bots at production
balances or inventory.

Balances, inventory, user data, **all pending conversation steps**, and the last
2,000 successfully committed update IDs per bot are stored together in Redis.
Redis locking serializes changes across both bots and across Vercel instances.
Busy requests return HTTP 503 so Telegram can retry. A write verifies lock
ownership before committing; storage failures are not silently ignored.

Session/JSON files uploaded through the existing admin flow are stored as
base64 values in Redis and restored to `/tmp/tl/sessions_storage` when needed.
This is suitable for modest inventories; large file collections should use
dedicated private object storage and a worker. Do not set `BOT_DATA_DIR` on
Vercel unless you also intentionally migrate stored paths to that location.

### Existing data

If you already run these bots, stop the polling process before migrating so it
cannot continue changing local balances. From the repository root, with the
Redis variables exported, run:

```sh
python -m pip install -r requirements.txt
python -m scripts.migrate_data /absolute/path/to/bot_data.json \
  --sessions-dir /absolute/path/to/sessions_storage
```

This preserves balances and inventory, uploads the referenced files, and
normalizes session paths for Vercel. It refuses to replace existing Redis state
unless you explicitly pass `--overwrite`. Keep a backup. Old in-memory pending
conversations cannot be recovered; users should restart those flows. If you
have no existing data, no migration is needed: the original initial state
(including the admin's 1,000,000 starting balance) is preserved.

## 3. Deploy

Commit the new single-file source **and configuration**, including removal of
the old `tl/tl.py` and `tl/storage.py`, before deploying. Uploading or registering
a URL does not update the code running at that URL.

Import the repository into Vercel with:

- **Root directory:** the repository root containing `vercel.json`, `api/`,
  `requirements.txt`, and `public/`—not the inner `api/` or `tl/` folder.
- **Framework preset:** Other.
- **Build command:** leave unset. The static output directory is set to
  `public` by `vercel.json`; do not change it to the repository root.
- Python 3.12 is selected by `.python-version`.

Deploy after setting the environment variables. Check:

```sh
curl https://your-project.vercel.app/api/health
```

Expected response includes `"ok": true`, `"service": "telegram-webhooks"`,
and **`"version": "single-file-webhook-v1"`**. If the version is missing, the
old multi-file deployment is still serving requests.

The production webhook URLs must be publicly accessible. Disable Vercel
Deployment Protection for these production endpoints, or configure an
appropriate bypass; Telegram cannot sign in to Vercel. Keep the webhook
secret validation enabled.

## 4. Register both webhooks once

Run this locally **from the repository root**, with your new tokens and the
same `WEBHOOK_SECRET` exported. You can copy `.env.example` to `.env`, fill it
in, and load it into your shell (`set -a; . ./.env; set +a`). The application
does not automatically load `.env` files.

```sh
python -m api.index --set-webhooks --url https://tl-wine.vercel.app
python -m api.index --webhook-info
```

Webhook setup is built into the **same file**. It first checks the deployed
version and authenticated Redis diagnostic; if the old code is still deployed
or storage fails, it refuses to change Telegram's registration. Use your stable
production domain, not a temporary preview URL. The command registers both
endpoints with `secret_token`, limits each bot to one webhook connection, and
subscribes to `message` and `callback_query`.
It also configures the shop's command menu. Queued updates are preserved by
default. Use `--drop-pending-updates` only if you intentionally want to discard
them. Registering a webhook switches Telegram away from `getUpdates`; keep
all old polling instances stopped. `WEBHOOK_BASE_URL` is an alternative to
`--url` for this local command, not a runtime requirement.

The old `python -m scripts.set_webhooks` / `--info` commands remain wrappers
for compatibility. Do not put tokens into webhook URL query strings or browser
address bars; this command sends them only to Telegram's official HTTPS API.

Webhook registration is **not** performed on every cold start. Re-run setup
if your public URL, bot tokens, or webhook secret changes. Send `/start` to each
bot afterward, and check `--webhook-info` plus Vercel logs for delivery failures.

### Troubleshooting 503 and queued updates

`pending=129` means Telegram has 129 updates waiting for successful delivery;
it does not mean registration failed. Fix the endpoint instead of discarding
the queue. A historical `last_error_message` may remain after recovery, so
also watch whether the pending count decreases.

Repeated requests approximately every second can be Telegram retries of the
same failed update; there is no scheduled polling loop in the application.
Correct credentials do not rule out runtime failures. Inspect the webhook
response or function log rather than assuming an environment-variable problem.
New logs identify both the safe `code` and `stage` (`lock_acquire`, `state_load`,
`handler`, `state_commit`, or `lock_release`). No credentials or message text
are included. A successful durable commit is acknowledged even if lock cleanup
fails; the remaining lock expires automatically. A cleanup failure no longer
masks the original handler exception.

Verify that the diagnostic update actually reached production: `/api/health`
must include `"version": "single-file-webhook-v1"`, and responses
also include an `X-Bot-Version` header. Older versions silently ignore the
`check=storage` parameter and return liveness, which is **not** a Redis test.

With the same `WEBHOOK_SECRET` exported locally, check Redis from the **deployed**
environment without changing balances, inventory, locks, or queued messages:

```sh
curl --silent --show-error \
  -H "X-Telegram-Bot-Api-Secret-Token: $WEBHOOK_SECRET" \
  'https://your-project.vercel.app/api/health?check=storage'
```

Successful response includes `"ok": true`, `"storage": "redis"`,
`"check": "read-only"`, and the diagnostic `version`.
This verifies connectivity and access to scripting, **not write permission**.
The token must still be the read/write token to process bot updates. The
unqualified `/api/health` remains a lightweight liveness check.

Failures return a credential-safe `code`, also recorded in Vercel function logs:

| Code | Action |
| --- | --- |
| `webhook_secret_not_configured` | Set a valid `WEBHOOK_SECRET` in Production and redeploy. |
| `bot_config_missing` / `bot_config_invalid` | Inspect SHOP_TOKEN, ADMIN_TOKEN and numeric ADMIN_ID; logs never include their values. |
| `redis_not_configured` / `redis_config_incomplete` | Set a complete REST URL/token pair in Production and redeploy. |
| `redis_url_invalid` | Use the database's HTTPS REST origin, without a path/query or surrounding quotes. |
| `redis_auth_failed` | Check that the REST token belongs to that database and is current. |
| `redis_permission_denied` | Replace a read-only/restricted token with one permitted to use GET, SET and EVAL. |
| `redis_rate_limited` | Check the provider's quota/rate limits and allow retries. |
| `redis_request_failed` / `redis_http_error` | Check database availability, endpoint and outbound connectivity. |
| `storage_busy` | Another update is holding the lock; allow Telegram to retry. Do not delete a live lock. |
| `storage_lock_expired` | Processing ran past its lock lease; inspect slow operations and worker requirements. |

Never paste bot tokens, Redis tokens, or the webhook secret into public logs or
issues. Share only the safe response code and relevant redacted Vercel log line.

## Local development and tests

With the required bot variables and webhook secret exported:

```sh
python -m pip install -r requirements.txt
python -m api.index
```

The local server listens on `http://127.0.0.1:8000`; set `PORT` to override it.
Without Redis, local state is saved in `tl/bot_data.json` and session files in
`tl/sessions_storage`. Set `BOT_DATA_DIR` for a different writable directory.
Vercel refuses this local-only fallback because its filesystem is ephemeral.
Telegram requires a public HTTPS endpoint, so localhost is for local testing
unless you provide your own HTTPS tunnel and register its URL.

Run the offline tests (Telegram and Redis network calls are mocked):

```sh
python -m unittest discover -s tests -v
```

## Serverless limits and delivery guarantees

- The function has a **60-second maximum duration**. Large `/tb` broadcasts,
  bulk Telethon scans, or large archive uploads may exceed it. Keep these jobs
  small; move longer work to a durable queue/worker rather than background
  threads inside a Vercel request. If you change the duration, also increase
  `StateStore.LOCK_SECONDS` so it remains longer than the invocation limit.
- Each request waits for its handler before acknowledging the update. A
  successful commit is deduplicated on delivery retries. This is **not
  exactly-once messaging**: a crash between sending a Telegram message and
  committing Redis state can repeat that message on a retry. The original
  handlers also catch some Telegram API failures; this change does not redesign
  their transaction/approval logic. Review that logic before treating this as
  a production payment system.
- Redis/file storage contains private account/session data. Restrict access,
  retain backups, monitor size and costs, and delete retired file objects as
  part of your inventory-retention policy. Automatic orphan-file cleanup is
  not included.

References: [Vercel Python functions](https://vercel.com/docs/functions/runtimes/python),
[Upstash Redis REST API](https://upstash.com/docs/redis/features/restapi),
[Telegram setWebhook](https://core.telegram.org/bots/api#setwebhook).