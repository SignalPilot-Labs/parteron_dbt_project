with source as (
    select * from {{ ref('patrons') }}
),

staged as (
    select
        patron_id,
        patron_name,
        email,
        country_code,
        created_at,
        first_pledge_at,
        coalesce(lifetime_spend_usd, 0) as lifetime_spend_usd,
        coalesce(status, 'active') as status,
        
        -- Derived fields
        datediff(month, created_at, current_timestamp()) as account_age_months,
        case 
            when lifetime_spend_usd >= 1000 then 'whale'
            when lifetime_spend_usd >= 500 then 'high_value'
            when lifetime_spend_usd >= 100 then 'regular'
            else 'casual'
        end as patron_value_tier,
        
        current_timestamp() as _stg_loaded_at
        
    from source
)

select * from staged
