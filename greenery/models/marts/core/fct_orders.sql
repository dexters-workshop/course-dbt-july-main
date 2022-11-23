with

orders_promos_joined as (

    select * from {{ ref('int_orders_promos_joined') }}

)

select * from orders_promos_joined