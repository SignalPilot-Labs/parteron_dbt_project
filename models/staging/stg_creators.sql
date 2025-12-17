with source as (
    select * from {{ ref('creators') }}
),

staged as (
    select
        creator_id,
        creator_name,
        email,
        category,
        subcategory,
        country_code,
        coalesce(currency_code, 'USD') as currency_code,
        coalesce(plan_type, 'pro') as plan_type,
        coalesce(is_nsfw, false) as is_nsfw,
        coalesce(is_verified, false) as is_verified,
        created_at,
        first_pledge_received_at,
        last_post_at,
        coalesce(status, 'active') as status,
        
        -- Derived fields
        datediff(day, created_at, coalesce(first_pledge_received_at, current_timestamp())) as days_to_first_pledge,
        datediff(month, created_at, current_timestamp()) as account_age_months,
        
        current_timestamp() as _stg_loaded_at
        
    from source
)

select * from staged
