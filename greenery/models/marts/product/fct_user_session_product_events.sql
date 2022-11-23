with

user_sessions_by_event as (

    -- bring in intermediate/prepped event data
    select * from {{ ref('int_user_sessions_detail_by_event_type')}}

),


session_orders as (

    -- prep data to deal w/checkout/package_shipped not having order_guid/product_guid info in events table
    select
          session_guid
        , max(order_guid)      as order_guid
        , max(checkout)        as checkout
        , max(package_shipped) as package_shipped

    from user_sessions_by_event
    group by 1
    order by session_guid

),

user_session_products_aggregated as (

    -- aggreate to product-level + ensure event type flags are sensible at the product-level
    select
        
        -- grouping variables
        user_session_num
        , user_guid
        , session_orders.order_guid
        , session_guid
        , session_start
        , session_end
        , session_min
        , product_guid
        , product_name

        -- event flags
        , session_orders.checkout
        , session_orders.package_shipped
        , max(case when event_type = 'page_view'       then 1 else 0 end) as page_view
        , max(case when event_type = 'add_to_cart'     then 1 else 0 end) as add_to_cart

    from user_sessions_by_event
    left join session_orders using(session_guid)
    where product_guid is not null

    group by 1,2,3,4,5,6,7,8,9,10,11

    order by user_guid, user_session_num

),

final as (

    select
          user_session_num
        , user_guid
        , order_guid
        , session_guid
        , session_start
        , session_end
        , round(session_min::numeric, 1) as session_mins
        , product_guid
        , product_name
        , page_view
        , add_to_cart

        -- fix checkout/package_shipped to be correct at the product-level
        --, case when add_to_cart = 0 then 0 else 1 end as checkout
        --, case when add_to_cart = 0 then 0 else 1 end as package_shipped
        , checkout
        , package_shipped

    from user_session_products_aggregated

)

select * from final