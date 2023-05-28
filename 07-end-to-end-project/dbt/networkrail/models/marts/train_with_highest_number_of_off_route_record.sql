with

movements as (

    select * from {{ ref('stg_networkrail__movements') }}

),
final as (

    select count(variation_status) as count_off_route , train_guid
    from movements
    where variation_status like 'OFF ROUTE'
    group by train_guid
    order by count_off_route desc
    limit 1

)

select * from final