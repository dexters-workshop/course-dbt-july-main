with

user_sessions_by_event as (

    select * from {{ ref('int_user_sessions_detail_by_event_type')}}

),

user_sessions_aggregated as (

    select
        user_session_num
        , user_guid
        , session_guid
        , session_start
        , session_end
        
        , max(round(session_min::numeric,1))      as session_min
        , max(event_num)        as total_events
        , sum(page_view)        as total_page_views
        , sum(add_to_cart)      as total_cart_adds
        , max(page_view)        as page_view
        , max(add_to_cart)      as add_to_cart
        , max(checkout)         as checkout
        , max(package_shipped)  as package_shipped
    from user_sessions_by_event
    group by 1,2,3,4,5
    order by user_guid, user_session_num asc

)

select * from user_sessions_aggregated