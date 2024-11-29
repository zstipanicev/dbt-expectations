{% test expect_column_values_to_be_decreasing(model, column_name,
                                                   sort_column=None,
                                                   strictly=True,
                                                   row_condition=None,
                                                   group_by=None,
                                                   step=None) %}

{%- set sort_column = column_name if not sort_column else sort_column -%}
{%- set operator = "<" if strictly else "<=" %}
{%- set partition_by = group_by | join(", ") if group_by else "" -%}
with all_values as (

    select
        {{ sort_column }} as sort_column,
        {%- if group_by -%}
        {{ group_by | join(", ") }},
        {%- endif %}
        {{ column_name }} as value_field
    from {{ model }}
    {% if row_condition %}
    where {{ row_condition }}
    {% endif %}

),
add_lag_values as (

    select
        sort_column,
        value_field,
        {{ dbt_expectations.lag(value_field="value_field", partition_by=partition_by, order_by="sort_column") }}
            as value_field_lag
    from
        all_values

),
validation_errors as (

    select
        *
    from
        add_lag_values
    where
        value_field_lag is not null 
        {% if target.type == 'clickhouse' -%}and value_field_lag <> 0 -- clickhouse will return data type default value which is not NULL and to get NULL we would need to know the data type of the value column to set the default to NULL, therefor this ugly hack {%- endif %}
        and
        not (
            (value_field {{ operator }} value_field_lag)
            {%- if step %}
            and ((value_field_lag - value_field) = {{ step }})
            {%- endif %}
        )

)
select *
from validation_errors
{% endtest %}

