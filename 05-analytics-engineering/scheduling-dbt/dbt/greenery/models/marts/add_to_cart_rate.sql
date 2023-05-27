with 
events as (
    select * from {{ ref('stg_greenery__events') }}
), 
unique_sessions as (

    select count(distinct session_guid) as number_of_unique_sessions
    from events
    where event_type like 'checkout'
),
add_to_cart as (
    select count(distinct session_guid)  as number_of_add_to_cart
    from events
    where event_type like 'add_to_cart'

),

final as (
    select number_of_add_to_cart
    , number_of_unique_sessions 
    , cast(number_of_add_to_cart as float64) / cast(number_of_unique_sessions as float64) as add_to_cart_rate
    from add_to_cart ,  unique_sessions
)

select * from final