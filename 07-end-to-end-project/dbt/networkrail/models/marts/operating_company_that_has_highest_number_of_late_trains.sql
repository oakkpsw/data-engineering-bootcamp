with

fct_movements as (

    select * from {{ ref('fct_movements') }}

),
final as (

    select company_name , count(variation_status) as count_late_number  
    from fct_movements
    where variation_status like 'LATE'
    and date(actual_timestamp_utc) >= date_add(current_date(), interval -3 day) 
    group by company_name
    order by count_late_number desc
    limit 1

)

select * from final
