# syntax=docker/dockerfile:1
# Worker image: Prefect process worker + project dependencies (Statcast ETL).
FROM prefecthq/prefect:3-python3.11

WORKDIR /app

RUN pip install --no-cache-dir uv

# Layer 1: Python dependencies (invalidates only when lockfiles change).
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked \
    uv sync --frozen --no-dev --extra dbt --no-install-project

# Layer 2: dbt package dependencies (invalidates when dbt package manifests change).
COPY dbt_project.yml packages.yml package-lock.yml ./
# Use the venv dbt binary so uv does not try to build etl-scripts before app code exists.
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked \
    --mount=type=cache,target=/app/dbt_packages,sharing=locked \
    /app/.venv/bin/dbt deps

# Layer 3: application code (fast rebuild when only Python/flow/dbt SQL changes).
COPY README.md prefect.yaml profiles.yml selectors.yml ./
COPY etl_scripts ./etl_scripts
COPY models ./models
COPY api_clients ./api_clients
COPY flows ./flows
COPY dbt ./dbt
COPY update_statcast.py ./

RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked \
    uv sync --frozen --no-dev --extra dbt

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app
ENV ETL_REPO_ROOT=/app
ENV DBT_PROFILES_DIR=/app
