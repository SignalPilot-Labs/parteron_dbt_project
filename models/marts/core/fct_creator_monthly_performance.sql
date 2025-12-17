{{
    config(
        materialized='table',
        unique_key='creator_month_key'
    )
}}

with creators as (
    select * from {{ ref('stg_creators') }}
),

pledges as (
    select * from {{ ref('stg_pledges') }}
),

transactions as (
    select * from {{ ref('stg_transactions') }}
),

tiers as (
    select * from {{ ref('stg_tiers') }}
),

posts as (
    select * from {{ ref('stg_posts') }}
),

engagement as (
    select * from {{ ref('stg_engagement_events') }}
),

-- Generate month spine from earliest pledge to current month
months as (
    select distinct date_trunc('month', transaction_at)::date as month_start_date
    from transactions
),

-- Create creator-month combinations
creator_months as (
    select 
        c.creator_id,
        m.month_start_date,
        {{ dbt_utils.generate_surrogate_key(['c.creator_id', 'm.month_start_date']) }} as creator_month_key
    from creators c
    cross join months m
    where m.month_start_date >= date_trunc('month', c.first_pledge_received_at)
),

-- Monthly pledge metrics
pledge_metrics as (
    select
        p.creator_id,
        date_trunc('month', t.transaction_at)::date as month_start_date,
        
        -- Patron counts
        count(distinct p.patron_id) as total_patrons,
        count(distinct case when p.pledge_status = 'active' then p.patron_id end) as active_patrons,
        count(distinct case when p.is_first_pledge = true then p.patron_id end) as new_patrons,
        count(distinct case when p.pledge_status = 'churned' then p.patron_id end) as churned_patrons,
        
        -- MRR
        sum(case when p.pledge_status = 'active' then p.pledge_amount_usd else 0 end) as gross_mrr_usd,
        sum(case when p.is_first_pledge = true then p.pledge_amount_usd else 0 end) as new_mrr_usd,
        sum(case when p.pledge_status = 'churned' then p.pledge_amount_usd else 0 end) as churned_mrr_usd,
        
        -- Tier distribution
        count(distinct case when ti.tier_rank = 1 then p.patron_id end) as tier_1_patrons,
        count(distinct case when ti.tier_rank = 2 then p.patron_id end) as tier_2_patrons,
        count(distinct case when ti.tier_rank >= 3 then p.patron_id end) as tier_3_plus_patrons,
        
        -- ARPP
        avg(p.pledge_amount_usd) as avg_pledge_amount_usd
        
    from pledges p
    inner join transactions t on p.pledge_id = t.pledge_id
    left join tiers ti on p.tier_id = ti.tier_id
    group by 1, 2
),

-- Monthly revenue (actual collections)
revenue_metrics as (
    select
        creator_id,
        date_trunc('month', transaction_at)::date as month_start_date,
        
        sum(case when transaction_status = 'succeeded' then gross_amount_usd else 0 end) as gross_revenue_usd,
        sum(case when transaction_status = 'succeeded' then platform_fee_usd else 0 end) as platform_fees_usd,
        sum(case when transaction_status = 'succeeded' then processing_fee_usd else 0 end) as processing_fees_usd,
        sum(case when transaction_status = 'succeeded' then net_amount_usd else 0 end) as net_creator_earnings_usd,
        
        count(case when transaction_status = 'succeeded' then 1 end) as successful_transactions,
        count(case when transaction_status = 'failed' then 1 end) as failed_transactions,
        sum(case when transaction_status = 'failed' then gross_amount_usd else 0 end) as declined_amount_usd
        
    from transactions
    where transaction_type = 'pledge_payment'
    group by 1, 2
),

-- Monthly content metrics
content_metrics as (
    select
        creator_id,
        published_month as month_start_date,
        
        count(distinct post_id) as posts_published,
        count(distinct case when content_access_type = 'paywalled' then post_id end) as paywalled_posts,
        count(distinct case when content_access_type = 'free' then post_id end) as free_posts
        
    from posts
    group by 1, 2
),

-- Monthly engagement metrics
engagement_metrics as (
    select
        creator_id,
        event_month as month_start_date,
        
        count(case when event_type = 'view' then 1 end) as total_views,
        count(case when event_type = 'like' then 1 end) as total_likes,
        count(case when event_type = 'comment' then 1 end) as total_comments,
        count(distinct patron_id) as engaged_patrons,
        sum(engagement_weight) as total_engagement_score
        
    from engagement
    group by 1, 2
),

-- Previous month for growth calculations
lagged as (
    select
        creator_id,
        month_start_date,
        lag(gross_mrr_usd) over (partition by creator_id order by month_start_date) as prev_mrr,
        lag(active_patrons) over (partition by creator_id order by month_start_date) as prev_patrons
    from pledge_metrics
)

select
    cm.creator_month_key,
    cm.creator_id,
    cm.month_start_date,
    
    -- Creator attributes
    c.creator_name,
    c.category as creator_category,
    c.plan_type,
    c.country_code as creator_country,
    
    -- Patron metrics
    coalesce(pm.total_patrons, 0) as total_patrons,
    coalesce(pm.active_patrons, 0) as active_patrons,
    coalesce(pm.new_patrons, 0) as new_patrons,
    coalesce(pm.churned_patrons, 0) as churned_patrons,
    coalesce(pm.active_patrons, 0) - coalesce(l.prev_patrons, 0) as net_patron_change,
    
    case 
        when coalesce(l.prev_patrons, 0) > 0 
        then round((pm.churned_patrons * 100.0 / l.prev_patrons), 2)
        else 0 
    end as patron_churn_rate_pct,
    
    -- MRR metrics
    coalesce(pm.gross_mrr_usd, 0) as gross_mrr_usd,
    coalesce(pm.new_mrr_usd, 0) as new_mrr_usd,
    coalesce(pm.churned_mrr_usd, 0) as churned_mrr_usd,
    coalesce(pm.gross_mrr_usd, 0) - coalesce(l.prev_mrr, 0) as mrr_change_usd,
    
    case 
        when coalesce(l.prev_mrr, 0) > 0 
        then round(((pm.gross_mrr_usd - l.prev_mrr) * 100.0 / l.prev_mrr), 2)
        else null 
    end as mrr_growth_rate_pct,
    
    -- Revenue metrics
    coalesce(rm.gross_revenue_usd, 0) as gross_revenue_usd,
    coalesce(rm.platform_fees_usd, 0) as platform_fees_usd,
    coalesce(rm.processing_fees_usd, 0) as processing_fees_usd,
    coalesce(rm.net_creator_earnings_usd, 0) as net_creator_earnings_usd,
    
    case 
        when coalesce(pm.gross_mrr_usd, 0) > 0 
        then round((rm.gross_revenue_usd * 100.0 / pm.gross_mrr_usd), 2)
        else null 
    end as collection_rate_pct,
    
    -- Payment health
    coalesce(rm.successful_transactions, 0) as successful_transactions,
    coalesce(rm.failed_transactions, 0) as failed_transactions,
    coalesce(rm.declined_amount_usd, 0) as declined_amount_usd,
    
    case 
        when coalesce(rm.successful_transactions, 0) + coalesce(rm.failed_transactions, 0) > 0
        then round((rm.failed_transactions * 100.0 / (rm.successful_transactions + rm.failed_transactions)), 2)
        else 0 
    end as decline_rate_pct,
    
    -- Tier distribution
    coalesce(pm.tier_1_patrons, 0) as tier_1_patrons,
    coalesce(pm.tier_2_patrons, 0) as tier_2_patrons,
    coalesce(pm.tier_3_plus_patrons, 0) as tier_3_plus_patrons,
    coalesce(pm.avg_pledge_amount_usd, 0) as avg_pledge_amount_usd,
    
    -- Content metrics
    coalesce(cnt.posts_published, 0) as posts_published,
    coalesce(cnt.paywalled_posts, 0) as paywalled_posts,
    coalesce(cnt.free_posts, 0) as free_posts,
    
    -- Engagement metrics
    coalesce(eng.total_views, 0) as total_views,
    coalesce(eng.total_likes, 0) as total_likes,
    coalesce(eng.total_comments, 0) as total_comments,
    coalesce(eng.engaged_patrons, 0) as engaged_patrons,
    coalesce(eng.total_engagement_score, 0) as total_engagement_score,
    
    case 
        when coalesce(pm.active_patrons, 0) > 0 
        then round((eng.engaged_patrons * 100.0 / pm.active_patrons), 2)
        else null 
    end as patron_engagement_rate_pct,
    
    current_timestamp() as updated_at

from creator_months cm
left join creators c on cm.creator_id = c.creator_id
left join pledge_metrics pm on cm.creator_id = pm.creator_id and cm.month_start_date = pm.month_start_date
left join revenue_metrics rm on cm.creator_id = rm.creator_id and cm.month_start_date = rm.month_start_date
left join content_metrics cnt on cm.creator_id = cnt.creator_id and cm.month_start_date = cnt.month_start_date
left join engagement_metrics eng on cm.creator_id = eng.creator_id and cm.month_start_date = eng.month_start_date
left join lagged l on cm.creator_id = l.creator_id and cm.month_start_date = l.month_start_date
