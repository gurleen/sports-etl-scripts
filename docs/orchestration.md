# Prefect orchestration (scheduled jobs, UI, run metadata)

This repo uses [Prefect 3](https://docs.prefect.io/) for scheduled Statcast loads (and future ETL jobs). Flows live under [`flows/`](../flows/); shared logic is under [`etl_scripts/`](../etl_scripts/). Deployments are declared in [`prefect.yaml`](../prefect.yaml).

## Stack choice

Use **self-hosted Prefect** (Docker Compose in [`docker-compose.yml`](../docker-compose.yml)) unless you already pay for Prefect Cloud. Keep the **Prefect server image tag**, **`prefect` Python package version** in `pyproject.toml`, and **`prefect-version` in `prefect.yaml`** aligned on the same minor release.

## Environment variables

| Variable | Purpose |
|----------|---------|
| `PREFECT_API_URL` | Worker and CLI point at the API (Compose: `http://prefect-server:4200/api`; laptop: `http://127.0.0.1:4200/api`). |
| `ETL_REPO_ROOT` | Absolute path to this repo on the machine running the worker. Required for the `set_working_directory` pull step in `prefect.yaml`. Compose sets it to `/app` inside the worker image. |
| `DATABASE_URL` | Full Postgres URL for **your warehouse** (Statcast table). For Compose, put it in a repo-root **`.env`** file (see below); for local `uv run`, the app loads that file via `python-dotenv`. |
| `POSTGRES_PASSWORD` | If `DATABASE_URL` is unset, URL is built with `POSTGRES_HOST` (default `172.237.129.152`), `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_DB`. |

Optional: store the warehouse URL in a Prefect **Secret** block named `etl-database-url` (see below). Flows try `DATABASE_URL`, then that block, then `POSTGRES_*`.

### Secret block (`etl-database-url`)

```bash
export PREFECT_API_URL=http://127.0.0.1:4200/api
uv run python -c "
from prefect.blocks.system import Secret
Secret(value='postgresql://user:pass@host:5432/db').save('etl-database-url', overwrite=True)
"
```

## First-time setup

1. Start the Prefect API (Compose **or** `uv run prefect server start` for local experiments).
2. Create a **process** work pool named `etl-pool` (once):

   ```bash
   export PREFECT_API_URL=http://127.0.0.1:4200/api
   uv run prefect work-pool create etl-pool --type process
   ```

3. Register deployments from `prefect.yaml`:

   ```bash
   export ETL_REPO_ROOT="$(pwd)"
   uv run prefect deploy --all
   ```

   Or run [`scripts/prefect_bootstrap.sh`](../scripts/prefect_bootstrap.sh) (sets `ETL_REPO_ROOT` to the repo root; you must export `PREFECT_API_URL` first).

4. Start a **worker** subscribed to `etl-pool`:

   ```bash
   export PREFECT_API_URL=http://127.0.0.1:4200/api
   export ETL_REPO_ROOT="$(pwd)"
   uv run prefect worker start --pool etl-pool --type process
   ```

   In Docker Compose, the `prefect-worker` service runs this for you.

5. Open the UI (default [http://127.0.0.1:4200](http://127.0.0.1:4200)) and confirm deployments appear under **Deployments**.

## Docker Compose (server + metadata Postgres + worker)

From the repo root, add a **`.env`** file (same directory as `docker-compose.yml`) with your warehouse URL:

```bash
# .env (do not commit; already gitignored)
DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME
```

Then:

```bash
docker compose up -d --build
```

The worker service uses Compose **`env_file`** so the value is not parsed as `${...}` interpolation (which often strips or corrupts URLs that contain `$`, `#`, or spaces). Recreate the worker after changing `.env`: `docker compose up -d --force-recreate prefect-worker`.

You can still **override** from the shell for a one-off: `docker compose run --rm -e DATABASE_URL='...' prefect-worker ...` (not needed for normal use).

**Local Prefect (no Docker):** `uv run prefect worker` does not load `.env` for you; this repo calls `load_dotenv()` from [`etl_scripts/statcast`](../etl_scripts/statcast.py) so `DATABASE_URL` in repo-root `.env` is picked up when flows import that module.

- **Prefect UI**: host port `4200` on the machine running Compose.
- **Prefect metadata DB**: service `prefect-db` (user/password/db `prefect` / `prefect` / `prefect` in the default compose file—change these for production).

If the worker cannot reach your warehouse by hostname/IP from inside Docker, use `host.docker.internal` (Docker Desktop) or attach the worker to the correct Docker network.

### Backing up and upgrading the Prefect database

- **Backup** (run from a host that can reach `prefect-db`):

  ```bash
  docker compose exec prefect-db pg_dump -U prefect prefect > prefect_metadata_$(date +%F).sql
  ```

- **Upgrade**: bump the `prefecthq/prefect:3-python3.*` image tag and the `prefect` package in `pyproject.toml` together, rebuild, then run migrations before serving:

  ```bash
  docker compose run --rm prefect-server prefect server database upgrade --yes
  ```

  (The bundled server command in `docker-compose.yml` already runs `upgrade` on startup.)

## Schedules and cron migration

- The scheduled deployment **`statcast-update-recent`** uses cron `0 6 * * *` UTC in `prefect.yaml`. Adjust there (or in the UI) to match your old cron.
- **Validate** by letting Prefect run alongside cron for a few days and comparing row counts / `max(game_date)` in the warehouse.
- **Disable cron** after you trust Prefect: remove the crontab line that called `update_statcast.py`.

Ad-hoc CLI runs remain available: `uv run python update_statcast.py update-recent --days 1`.

## Run metadata in the UI

Each Statcast flow records a **Markdown artifact** `statcast-run-summary` (date range, rows fetched/written, table row count and `max(game_date)` before vs after). Open a flow run in the UI and check **Artifacts**.

The **`statcast-extra-ingest-year`** deployment loads missing Savant data **one `game_date` at a time** (all games on that date before the next). Use **`start_date`** / **`end_date`** (inclusive `game_date` window) to align with a Statcast ingest; **`statcast-update-recent`** passes the same window as its ingest range. Without a window, **`days`** limits to the N **most recent** missing dates; omit both window and **`days`** to process every missing date in **`year`** (oldest first). Progress artifact: `statcast-extra-ingest`.

## Failure notifications (recommended)

Prefect OSS supports **Automations** in the UI (see [Automations](https://docs.prefect.io/latest/concepts/automations/)):

1. Create a **Webhook** or **Email** integration/block if needed.
2. Create an automation: trigger on **Flow run failed** (optionally filter by deployment tags such as `statcast`).
3. Add an action to send a notification (webhook, email, Slack via generic webhook, etc.).

This gives alerts without opening the UI.

## Adding a new scheduled job later

1. Put reusable logic in `etl_scripts/` (or another importable package).
2. Add a `@flow` in `flows/<name>_flow.py` with `@task` boundaries where failures should be visible.
3. Append a deployment stanza to `prefect.yaml` (entrypoint, `work_pool.name: etl-pool`, optional `schedule`).
4. Run `uv run prefect deploy --all` (or deploy a single deployment by name per Prefect CLI docs).

Downstream steps (for example **dbt marts** or **refresh materialized views** after a load) fit naturally as extra `@task`s in the same flow, chained after the ingest task. Prefer `dbt build --selector post_statcast_ingest` once transforms live in dbt (see [`docs/dbt.md`](dbt.md)).

## UI behind a reverse proxy or tunnel (Cloudflare, nginx, etc.)

If you open the UI at something like `https://sports-etl.gurleen.net/` but see **“Can't connect to Server API at http://127.0.0.1:4200/api”**, the SPA is still using Prefect’s default API base. Set the **public** API URL the browser should use (same scheme and host as the UI, usually with `/api`):

```bash
# In repo-root .env (loaded by Compose for prefect-server)
PREFECT_UI_API_URL=https://sports-etl.gurleen.net/api
```

Restart the server container after changing `.env`: `docker compose up -d --force-recreate prefect-server`.

Your tunnel must forward **`/`** (UI) and **`/api`** (and typically **`/api/`** websockets if used) to the Prefect server process. See Prefect’s [self-hosted](https://docs.prefect.io/v3/advanced/self-hosted) and [security / proxy](https://docs.prefect.io/v3/advanced/security-settings) notes for TLS and CORS if the UI and API are on different origins.

**Workers and CLI** on other machines must use the same public API, not `127.0.0.1`:

```bash
export PREFECT_API_URL=https://sports-etl.gurleen.net/api
```

In Docker Compose on the **same host** as the server, the worker keeps using `http://prefect-server:4200/api` (internal); only browsers and remote CLIs need the public URL.

## Troubleshooting

- **`Unable to read the specified config file ... prefect.yaml`**: run CLI commands from the repo root or pass `--prefect-file /path/to/prefect.yaml`.
- **Pull step / import errors**: ensure `ETL_REPO_ROOT` points at the directory that contains `flows/` and `etl_scripts/`, and that `uv sync` has been run so dependencies exist.
- **Warehouse connection errors from Docker**: verify `DATABASE_URL` from inside the worker (`docker compose exec prefect-worker env | grep DATABASE`).
