with

int_orders_addresses__joined as (

		select * from {{ ref('int_orders_addresses__joined') }}

)

, final as (

    select
        state
        , count(o.order_guid) as number_of_orders

    from int_orders_addresses__joined
    group by state
    order by 2 desc
    limit 1

)

select * from final