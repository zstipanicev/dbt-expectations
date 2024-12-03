{%- macro lag(value_field, offset=None, default_value=None, partition_by=None, order_by=None, window_definition=None) -%}
    {{ return(adapter.dispatch('lag', 'dbt_expectations')(value_field, offset, default_value, partition_by, order_by, window_definition)) }}
{% endmacro %}

{%- macro default__lag(value_field, offset, default_value, partition_by, order_by, window_definition) -%}
{%- set partition_by_clause = "partition by " ~ partition_by if partition_by else "" -%}
{%- set order_by_clause = "order by " ~ order_by if order_by else "" -%}
{%- set window_definition_clause = window_definition if window_definition else "" -%}
{%- set offset_clause = ", " ~ offset if offset else "" -%}
{%- set default_value_clause = ", " ~ default_value if default_value else "" -%}

  lag({{ value_field }} {{ offset_clause }} {{ default_value_clause }}) over({{ partition_by_clause }} {{ order_by_clause }} {{ window_definition_clause }})

{%- endmacro -%}
