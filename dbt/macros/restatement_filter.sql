{#
    Incremental filter used by every model that consumes restated history.

    Rather than "rows newer than the max I have", which permanently freezes a
    day at whatever the producer had published the first time, we rebuild the
    last `restatement_window_days` days on each run. Combined with a merge on
    the grain's key this is idempotent: re-running yesterday corrects it, it
    never duplicates it.
#}
{% macro restatement_filter(column_name, days=none) -%}
    {%- set window = days if days is not none else var('restatement_window_days') -%}
    {{ column_name }} >= (
        SELECT DATEADD('day', -{{ window }}, COALESCE(MAX({{ column_name }}), '1900-01-01'::date))
        FROM {{ this }}
    )
{%- endmacro %}
