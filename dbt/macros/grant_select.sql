{#
    Re-grant SELECT after a full refresh recreates a relation. Wired up as an
    on-run-end hook would be nicer, but keeping it an explicit macro makes it
    obvious in the model that the grant exists.
#}
{% macro grant_select(role='ANALYST') %}
    {% if target.name == 'prod' %}
        GRANT SELECT ON {{ this }} TO ROLE {{ role }};
    {% endif %}
{% endmacro %}
