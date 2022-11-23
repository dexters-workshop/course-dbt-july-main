with

staged_events as (

    -- bring in events to access sessions detail
    select * from {{ ref('stg_public__events') }}

),

staged_products as (

    -- bring in products table to access product details e.g., product name
    select * from {{ ref('stg_public__products') }}

),

sessions_aggregated as (

    -- session level start/end to have at event level in next table
    select
          session_guid
        , min(created_at_utc) as session_start
        , max(created_at_utc) as session_end

    from staged_events

    -- package_shipped happens after users online 'session' so remove before max(created_at_utc)
    where event_type != 'package_shipped'

    group by 1

),

user_sessions_by_event as (

    -- prep/clean data at event-level so that it can be handled properly in later facts table(s)
    select

        -- session identifier by user
          dense_rank() over(
            partition by user_guid 
            order by session_start, session_guid)                as user_session_num

        -- session detail
        , session_guid
        , created_at_utc
        , session_start
        , session_end
        , extract(epoch from (session_end - session_start)) / 60 as session_min
        , event_type
        , row_number() over(
                        partition by user_guid, session_guid
                        order by created_at_utc)                 as event_num

        -- product detail
        , staged_products.product_name
        , staged_products.product_price

        -- event flags (w/macro)
        {{ create_event_type_flags() }}

        -- other guids
        , user_guid
        , order_guid
        , product_guid

    from staged_events
    left join sessions_aggregated using(session_guid)
    left join staged_products     using(product_guid)

)

select * from user_sessions_by_event