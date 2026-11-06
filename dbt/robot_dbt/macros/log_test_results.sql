{% macro log_test_results(results) %}
  {% if execute %}
    {% set rows = [] %}
    {% for r in results %}
      {% if r.node.resource_type == 'test' %}
        {% do rows.append(
            "('" ~ invocation_id ~ "', '"
            ~ r.node.name ~ "', '"
            ~ r.status ~ "', "
            ~ (r.failures if r.failures is not none else 0)
            ~ ", sysdate())"
        ) %}
      {% endif %}
    {% endfor %}
    {% if rows | length > 0 %}
      insert into {{ target.database }}.{{ target.schema }}.DBT_TEST_RESULTS
        (INVOCATION_ID, TEST_NAME, STATUS, FAILURES, RUN_AT)
      values {{ rows | join(', ') }}
    {% else %}
      select 1
    {% endif %}
  {% else %}
    select 1
  {% endif %}
{% endmacro %}