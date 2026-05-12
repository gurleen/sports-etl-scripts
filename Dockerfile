# Worker image: Prefect process worker + project dependencies (Statcast ETL).
FROM prefecthq/prefect:3-python3.11

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock prefect.yaml ./
COPY etl_scripts ./etl_scripts
COPY models ./models
COPY flows ./flows
COPY update_statcast.py ./

RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app
ENV ETL_REPO_ROOT=/app
