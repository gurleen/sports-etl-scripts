#!/usr/bin/env bash
# One-time (or CI) setup: work pool + register deployments from prefect.yaml.
set -euo pipefail
cd "$(dirname "$0")/.."
export ETL_REPO_ROOT="${ETL_REPO_ROOT:-$(pwd)}"
: "${PREFECT_API_URL:?Set PREFECT_API_URL (e.g. http://127.0.0.1:4200/api for local server)}"

if uv run prefect work-pool inspect etl-pool &>/dev/null; then
  echo "Work pool etl-pool already exists."
else
  uv run prefect work-pool create etl-pool --type process
fi

uv run prefect deploy --all
