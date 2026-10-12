{#- part / whole rounded to 6 decimals, 0 when whole is 0 (as metrics.py did). -#}
{% macro score(part, whole) -%}
    case when ({{ whole }}) > 0
        then round(({{ part }})::numeric / ({{ whole }}), 6)
        else 0
    end
{%- endmacro %}
