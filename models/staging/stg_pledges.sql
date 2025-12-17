with source as (
    select * from {{ ref('pledges') }}
),

staged as (
    select
        pledge_id,
        patron_id,
        creator_id,
        tier_id,
        pledge_amount_usd,
        pledge_status,
        coalesce(is_first_pledge, false) as is_first_pledge,
        started_at,
        ended_at,
        pause_started_at,
        churn_reason,
        
        -- Derived fields
        date_trunc('month', started_at) as pledge_month,
        case 
            when pledge_status = 'active' then null
            else datediff(day, started_at, coalesce(ended_at, current_timestamp()))
        end as pledge_duration_days,
        
        -- Is currently active (for point-in-time analysis)
        case 
            when pledge_status = 'active' and pause_started_at is null then true
            else false
        end as is_currently_active,
        
        current_timestamp() as _stg_loaded_at
        
    from source
)

select * from staged
