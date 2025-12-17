with source as (
    select * from {{ ref('posts') }}
),

staged as (
    select
        post_id,
        creator_id,
        title,
        post_type,
        access_level,
        minimum_tier_id,
        published_at,
        coalesce(is_pinned, false) as is_pinned,
        
        -- Derived fields
        date_trunc('month', published_at) as published_month,
        date_trunc('day', published_at) as published_date,
        
        -- Content categorization
        case 
            when access_level = 'public' then 'free'
            when access_level = 'patrons_only' then 'paywalled'
            when access_level = 'tier_specific' then 'premium'
            else 'unknown'
        end as content_access_type,
        
        current_timestamp() as _stg_loaded_at
        
    from source
)

select * from staged
