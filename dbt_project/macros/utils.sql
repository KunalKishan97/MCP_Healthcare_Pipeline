{% macro clean_nulls(col) %}
    case 
        when {{ col }} is null or {{ col }} = '' or lower({{ col }}) in ('na', 'nan', 'null') then 'NA'
        else {{ col }}
    end
{% endmacro %}
