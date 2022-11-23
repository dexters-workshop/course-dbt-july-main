
with

user_sessions_by_event as (

    select * from {{ ref('int_user_sessions_detail_by_event_type')}}

),


products_by_session as (

    select
        session_guid
        , product_guid
        , product_name
        , event_type
        , page_view
        , add_to_cart
        , max(checkout) over(partition by session_guid) as checkout
    from user_sessions_by_event
    order by session_guid

),

tidy_page_views as (

    select
          session_guid
        , product_guid
        , product_name
        
        , max(page_view)        as page_view
        , max(add_to_cart)      as add_to_cart
        , max(checkout)         as checkout

    from products_by_session
    where event_type not in ('checkout', 'package_shipped')

    group by 1,2,3
    order by session_guid

)

select * from tidy_page_views