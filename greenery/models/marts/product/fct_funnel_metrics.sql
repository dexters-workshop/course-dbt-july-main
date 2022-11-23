with

user_sessions_by_event as (

    -- bring in intermediate/prepped event data
    select * from {{ ref('int_user_sessions_detail_by_event_type')}}

),

user_sessions as (

    -- prep data at session/user level
    select
    session_guid
    , user_guid
    , max(page_view) as page_view
    , max(add_to_cart) as add_to_cart
    , max(checkout)    as checkout
    from user_sessions_by_event
    group by 1,2

),

page_views as (

    select
        'A' as funnel_stage
        , 'page_view' as event_type
        --, count(distinct user_guid) as unique_users
        , sum(page_view) as event_count
    from user_sessions
    group by 1

),

add_to_carts as (

    select
        'B' as funnel_stage
        , 'add_to_cart' as event_type
        --, count(distinct user_guid) as unique_users
        , sum(add_to_cart) as event_count
    from user_sessions
    group by 1

),

checkout as (

    select
        'C' as funnel_stage
        , 'checkouts' as event_type
        --, count(distinct user_guid) as unique_users
        , sum(checkout) as event_count
    from user_sessions
    group by 1

),

stacked_metrics as (

    select * from page_views
    union
    select * from add_to_carts
    union
    select * from checkout
    order by funnel_stage

)

-- calculate metrics
select
    stacked_metrics.*
    , round( event_count::numeric / 
           ( max(event_count) over() ), 2) as pct_conversion
    , round(event_count::numeric / 
           ( lag(event_count) over(order by funnel_stage) ), 2) as pct_of_previous
    , round( (event_count - lag(event_count) over (order by funnel_stage) )::numeric /
            ( lag(event_count) over (order by funnel_stage) ), 2) as pct_dropoff
from stacked_metrics