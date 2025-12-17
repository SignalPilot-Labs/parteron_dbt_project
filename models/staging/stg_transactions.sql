with source as (
    select * from {{ ref('transactions') }}
),

staged as (
    select
        transaction_id,
        pledge_id,
        patron_id,
        creator_id,
        transaction_type,
        transaction_status,
        gross_amount_usd,
        platform_fee_usd,
        processing_fee_usd,
        net_amount_usd,
        payment_method,
        failure_reason,
        transaction_at,
        
        -- Derived fields
        date_trunc('month', transaction_at) as transaction_month,
        date_trunc('day', transaction_at) as transaction_date,
        
        -- Fee analysis
        case 
            when gross_amount_usd > 0 and transaction_status = 'succeeded'
            then round((platform_fee_usd / gross_amount_usd) * 100, 2)
            else null
        end as platform_fee_rate_pct,
        
        case 
            when gross_amount_usd > 0 and transaction_status = 'succeeded'
            then round((processing_fee_usd / gross_amount_usd) * 100, 2)
            else null
        end as processing_fee_rate_pct,
        
        -- Success flag
        case when transaction_status = 'succeeded' then 1 else 0 end as is_successful,
        
        current_timestamp() as _stg_loaded_at
        
    from source
)

select * from staged
