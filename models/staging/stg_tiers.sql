with source as (
    select * from {{ ref('tiers') }}
),

staged as (
    select
        tier_id,
        creator_id,
        tier_name,
        tier_rank,
        price_usd,
        description,
        coalesce(is_active, true) as is_active,
        created_at,
        archived_at,
        
        -- Price bucket for analysis
        case 
            when price_usd <= 5 then 'micro'
            when price_usd <= 15 then 'standard'
            when price_usd <= 30 then 'premium'
            else 'whale'
        end as price_bucket,
        
        current_timestamp() as _stg_loaded_at
        
    from source
)

select * from staged
