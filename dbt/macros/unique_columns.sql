{#- Generic test: no two rows share the same values of these columns. -#}
{% test unique_columns(model, columns) %}
select {{ columns | join(', ') }}
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
