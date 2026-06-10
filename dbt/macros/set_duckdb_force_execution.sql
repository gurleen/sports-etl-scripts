{#
  pg_duckdb session preflight. Besides forcing DuckDB execution, this caps DuckDB
  memory and internal threads so a single analytical query can't exhaust the small
  (2 GB) Postgres VPS — without a cap DuckDB defaults to a 4 GB limit and OOMs /
  thrashes. With memory_limit set, DuckDB spills to its temp dir instead. These are
  superuser-settable per session, applied on every model/test connection.
#}
{% macro set_duckdb_force_execution() %}
  set duckdb.force_execution = true;
  set duckdb.memory_limit = '512MB';
  set duckdb.threads = 2
{% endmacro %}
