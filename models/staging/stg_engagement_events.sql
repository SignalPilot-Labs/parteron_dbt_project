with source as (
    select * from {{ ref('engagement_events') }}
),

staged as (
    select
        event_id,
        patron_id,
        creator_id,
        post_id,
        event_type,
        event_at,
        
        -- Derived fields
        date_trunc('month', event_at) as event_month,
        date_trunc('day', event_at) as event_date,
        
        -- Engagement weighting (for composite scores)
        case event_type
            when 'view' then 1
            when 'like' then 3
            when 'comment' then 5
            when 'share' then 7
            else 1
        end as engagement_weight,
        
        current_timestamp() as _stg_loaded_at
        
    from source
)

select * from staged
