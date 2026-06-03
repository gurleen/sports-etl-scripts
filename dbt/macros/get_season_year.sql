{% macro get_season_year() %}
  {% if var('season_year') is not none %}
    {{ var('season_year') }}
  {% else %}
    extract(year from current_date)::int
  {% endif %}
{% endmacro %}
