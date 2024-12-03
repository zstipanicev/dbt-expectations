{% test expect_column_values_to_be_increasing(model, column_name,
                                                   sort_column=None,
                                                   strictly=True,
                                                   row_condition=None,
                                                   group_by=None,
                                                   step=None) %}

{{ adapter.dispatch('test_expect_column_values_to_be_increasing', 'dbt_expectations')(model, column_name, 
                                                                                        sort_column, 
                                                                                        strictly, 
                                                                                        row_condition, 
                                                                                        group_by, 
                                                                                        step) }}

{% endtest %}

{% macro default__test_expect_column_values_to_be_increasing(model, column_name, sort_column, strictly, row_condition, group_by, step) %}

{%- set sort_column = column_name if not sort_column else sort_column -%}
{%- set operator = ">" if strictly else ">=" -%}
{%- set partition_by = group_by | join(", ") if group_by else "" -%}
with all_values as (

    select
        {{ sort_column }} as sort_column,
        {%- if group_by -%}
        {{ partition_by }},
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
        {%- if group_by -%}
        {{ partition_by }},
        {%- endif %}
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
        and
        not (
            (value_field {{ operator }} value_field_lag)
            {%- if step %}
            and ((value_field - value_field_lag) = {{ step }})
            {%- endif %}
        )

)
select *
from validation_errors
{% endmacro %}
