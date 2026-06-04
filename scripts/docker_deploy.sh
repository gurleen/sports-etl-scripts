#!/usr/bin/env bash
# Deploy worker image on the VPS after git pull. Uses BuildKit layer/cache mounts from the Dockerfile.
set -euo pipefail

export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1

REV_BEFORE="$(git rev-parse HEAD)"
git pull --ff-only
REV_AFTER="$(git rev-parse HEAD)"

if [ "${REV_BEFORE}" != "${REV_AFTER}" ]; then
  echo "Building prefect-worker (${REV_BEFORE:0:7} -> ${REV_AFTER:0:7})…"
  docker compose build prefect-worker
else
  echo "Already up to date (${REV_AFTER:0:7}); skipping image build."
fi

docker compose up -d
