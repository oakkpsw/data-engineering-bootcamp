with

movements as (

    select * from {{ ref('stg_networkrail__movements') }}

)

, operating_companies as (

    select * from {{ ref('stg_networkrail__operating_companies') }}

)

, joined as (

    select
          event_type
        , actual_timestamp_utc
        , event_source
        , train_guid as train_id
        , variation_status
        , m.toc_guid as toc_id
        , oc.company_name as company_name
       
    from movements as m
    left join operating_companies as oc
        on m.toc_guid = oc.toc_id

)

select * from joined