{%- macro create_event_type_flags() %}
        {%- set event_types = dbt_utils.get_query_results_as_dict(
            "select distinct event_type as event_type from"
            ~ ref('stg_public__events')) -%}

        {%- for event_type in event_types['event_type'] %}
        , case when event_type = '{{event_type}}' then 1 else 0 end as {{event_type}}
        {%- endfor -%}
{%- endmacro %}