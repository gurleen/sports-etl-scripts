#!/usr/bin/env bash
# Deploy worker image on the VPS. Pulls main, rebuilds the worker when HEAD changes, then restarts Compose.
# Uses BuildKit layer/cache mounts from the Dockerfile. Call only this script for deploys (do not git pull before it).
set -euo pipefail

export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1

REV_BEFORE="$(git rev-parse HEAD)"
git pull --ff-only
REV_AFTER="$(git rev-parse HEAD)"

if [ "${REV_BEFORE}" != "${REV_AFTER}" ] || [ "${FORCE_WORKER_REBUILD:-}" = "1" ]; then
  if [ "${REV_BEFORE}" = "${REV_AFTER}" ]; then
    echo "FORCE_WORKER_REBUILD=1: rebuilding prefect-worker at ${REV_AFTER:0:7}…"
  else
    echo "Building prefect-worker (${REV_BEFORE:0:7} -> ${REV_AFTER:0:7})…"
  fi
  docker compose build prefect-worker
else
  echo "Already up to date (${REV_AFTER:0:7}); skipping image build."
fi

docker compose up -d
