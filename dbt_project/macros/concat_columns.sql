{% macro concat_columns(column_list, separator=' ') %}
   {%- set cols = [] -%}
    {%- for col in column_list -%}
        {%- set safe_col = "coalesce(" ~ col ~ ", '')" -%}
        {%- do cols.append(safe_col) -%}
    {%- endfor -%}

    trim({{ cols | join(" || '" ~ separator ~ "' || ") }})
{% endmacro %}
