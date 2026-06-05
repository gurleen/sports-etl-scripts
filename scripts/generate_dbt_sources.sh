#!/usr/bin/env bash
# Write dbt source YAML from the warehouse (dbt-codegen generate_source).
# Logs go to stderr; only YAML is written to the output file.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

OUT="${1:-dbt/models/_sources.generated.yml}"
# Comma-separated list, or "all" / "*" / empty to introspect every table in public.
TABLES="${DBT_SOURCE_TABLES:-statcast,players,teams,weights,park_factors}"
TABLE_PATTERN="${DBT_SOURCE_TABLE_PATTERN:-%}"
EXCLUDE="${DBT_SOURCE_EXCLUDE:-}"

export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$ROOT}"

if [ -z "${POSTGRES_PASSWORD:-}" ] && [ -z "${DATABASE_URL:-}" ]; then
  echo "Set POSTGRES_* or DATABASE_URL (e.g. source .env or scripts/parse_database_url.sh)." >&2
  exit 1
fi

mkdir -p "$(dirname "$OUT")" logs
TMP="$(mktemp "${OUT}.XXXXXX")"
trap 'rm -f "$TMP"' EXIT

# dbt still parses the project; keep a minimal _sources.generated.yml in git for first-time clones.
ARGS="$(python3 - <<'PY'
import json, os

tables = os.environ.get("TABLES", "").strip()
pattern = os.environ.get("TABLE_PATTERN", "%")
exclude = os.environ.get("EXCLUDE", "")

args = {
    "schema_name": "public",
    "name": "warehouse",
    "table_pattern": pattern,
    "generate_columns": True,
    "include_data_types": True,
}
if exclude:
    args["exclude"] = exclude

if tables.lower() in ("", "all", "*"):
    pass  # omit table_names → codegen lists every relation in the schema
else:
    args["table_names"] = [t.strip() for t in tables.split(",") if t.strip()]

print(json.dumps(args))
PY
)"

NO_COLOR=1 TABLES="$TABLES" TABLE_PATTERN="$TABLE_PATTERN" EXCLUDE="$EXCLUDE" \
  uv run dbt --no-use-colors --quiet run-operation generate_source --args "$ARGS" \
  >"$TMP" 2>logs/dbt_generate_source.log

if ! head -1 "$TMP" | grep -q '^version:'; then
  echo "generate_source did not write YAML. See logs/dbt_generate_source.log:" >&2
  tail -30 logs/dbt_generate_source.log >&2
  if [ -s "$TMP" ]; then
    echo "--- stdout ---" >&2
    tail -15 "$TMP" >&2
  fi
  exit 1
fi

mv "$TMP" "$OUT"
trap - EXIT
echo "Wrote $OUT"
