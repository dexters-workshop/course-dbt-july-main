

{% snapshot orders_snapshot %}

{{
    config(
      target_database = 'dbt',
      target_schema   = 'snapshots',
      unique_key      = 'order_id',

      strategy        = 'check',
      check_cols      = ['status'],
    )
}}

select * from {{ source('public', 'orders') }}

{% endsnapshot %}