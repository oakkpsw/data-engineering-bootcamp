with

source as (

-- Your code here
    select * from {{ source('networkrail', 'movements') }}

)

, renamed_recasted as (

    select 
        event_type
        , gbtt_timestamp as gbtt_timestamp_utc
        , original_loc_stanox
        , planned_timestamp as planned_timestamp_utc
        , timetable_variation
        , original_loc_timestamp as original_loc_timestamp_utc
        , current_train_id as current_train_guid
        , delay_monitoring_point
        , next_report_run_time
        , reporting_stanox
        , actual_timestamp as actual_timestamp_utc
        , correction_ind
        , event_source
        , train_file_address
        , platform
        , division_code
        , train_terminated
        , train_id as train_guid
        , offroute_ind
        , variation_status
        , train_service_code
        , toc_id as toc_guid
        , loc_stanox
        , auto_expected
        , direction_ind
        , route
        , planned_event_type
        , next_report_stanox
        , line_ind
    from source
-- Your code here

)

, final as (
     select
        event_type
        -- , gbtt_timestamp_utc
        -- , original_loc_stanox
        -- , planned_timestamp_utc
        -- , timetable_variation
        -- , original_loc_timestamp_utc
        -- , current_train_id
        -- , delay_monitoring_point
        -- , next_report_run_time
        -- , reporting_stanox
        , actual_timestamp_utc
        -- , correction_ind
        , event_source
        -- , train_file_address
        -- , platform
        -- , division_code
        -- , train_terminated
        , train_guid
        , offroute_ind
        , variation_status
        -- , train_service_code
        , toc_guid
        -- , loc_stanox
        -- , auto_expected
        -- , direction_ind
        -- , route
        -- , planned_event_type
        -- , next_report_stanox
        -- , line_ind

        
    from renamed_recasted

-- Your code here

) 

select * from final