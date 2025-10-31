{% macro deduplicate(model_ref, partition_by) %}
    (
        select *
        from (
            select *,
                row_number() over (partition by {{ partition_by }} order by load_dtm desc) as row_num
            from {{ model_ref }}
        ) t
        where row_num = 1
    )
{% endmacro %}
