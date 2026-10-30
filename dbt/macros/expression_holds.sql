{#- Generic test: every row of the model satisfies the SQL expression
    (rows where it is false or null fail). -#}
{% test expression_holds(model, expression) %}
select *
from {{ model }}
where not coalesce(({{ expression }}), false)
{% endtest %}
