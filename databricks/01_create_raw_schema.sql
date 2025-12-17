-- ============================================================================
-- PATREON ANALYTICS: RAW SCHEMA DDL
-- Target: Databricks Unity Catalog with Delta Lake
-- Run this in Databricks SQL or notebook to create the raw layer
-- ============================================================================

-- Create catalog and schemas
CREATE CATALOG IF NOT EXISTS patreon_dev;
USE CATALOG patreon_dev;

CREATE SCHEMA IF NOT EXISTS raw COMMENT 'Raw ingested data from source systems';
CREATE SCHEMA IF NOT EXISTS staging COMMENT 'Cleaned and standardized data';
CREATE SCHEMA IF NOT EXISTS marts COMMENT 'Business-ready analytical models';

USE SCHEMA raw;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Creators: The supply side of the marketplace
CREATE OR REPLACE TABLE creators (
    creator_id STRING NOT NULL COMMENT 'Unique identifier for creator account',
    creator_name STRING NOT NULL COMMENT 'Display name chosen by creator',
    email STRING COMMENT 'Creator email address',
    category STRING COMMENT 'Primary content category: podcasts, video, writing, music, visual_art, games, education, other',
    subcategory STRING COMMENT 'More specific content niche',
    country_code STRING COMMENT 'ISO 3166-1 alpha-2 country code',
    currency_code STRING DEFAULT 'USD' COMMENT 'Preferred payout currency',
    plan_type STRING DEFAULT 'pro' COMMENT 'Patreon plan: lite, pro, premium',
    is_nsfw BOOLEAN DEFAULT FALSE COMMENT 'Adult content flag',
    is_verified BOOLEAN DEFAULT FALSE COMMENT 'Identity verified by platform',
    created_at TIMESTAMP NOT NULL COMMENT 'Account creation timestamp',
    first_pledge_received_at TIMESTAMP COMMENT 'First successful pledge timestamp',
    last_post_at TIMESTAMP COMMENT 'Most recent content post',
    status STRING DEFAULT 'active' COMMENT 'Account status: active, paused, suspended, deleted',
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'ETL load timestamp'
)
USING DELTA
COMMENT 'Creator accounts - one row per creator'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Patrons: The demand side
CREATE OR REPLACE TABLE patrons (
    patron_id STRING NOT NULL COMMENT 'Unique identifier for patron account',
    patron_name STRING COMMENT 'Display name',
    email STRING COMMENT 'Patron email address',
    country_code STRING COMMENT 'ISO country code',
    created_at TIMESTAMP NOT NULL COMMENT 'Account creation timestamp',
    first_pledge_at TIMESTAMP COMMENT 'First pledge made on platform',
    lifetime_spend_usd DECIMAL(12,2) DEFAULT 0 COMMENT 'Total amount spent across all creators',
    status STRING DEFAULT 'active' COMMENT 'Account status',
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Patron accounts - one row per patron';

-- Tiers: Membership levels offered by creators
CREATE OR REPLACE TABLE tiers (
    tier_id STRING NOT NULL COMMENT 'Unique identifier for tier',
    creator_id STRING NOT NULL COMMENT 'FK to creators',
    tier_name STRING NOT NULL COMMENT 'Display name (e.g., "Supporter", "Super Fan")',
    tier_rank INT NOT NULL COMMENT 'Ordering: 1 = lowest tier, higher = premium',
    price_usd DECIMAL(10,2) NOT NULL COMMENT 'Monthly price in USD',
    description STRING COMMENT 'Benefits description',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL,
    archived_at TIMESTAMP COMMENT 'When tier was discontinued',
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Membership tiers - creators can have up to 8 tiers';

-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- Pledges: The subscription relationship (SCD Type 2 style)
CREATE OR REPLACE TABLE pledges (
    pledge_id STRING NOT NULL COMMENT 'Unique identifier for this pledge record',
    patron_id STRING NOT NULL COMMENT 'FK to patrons',
    creator_id STRING NOT NULL COMMENT 'FK to creators',
    tier_id STRING COMMENT 'FK to tiers (null if custom amount)',
    pledge_amount_usd DECIMAL(10,2) NOT NULL COMMENT 'Monthly pledge amount',
    pledge_status STRING NOT NULL COMMENT 'active, paused, churned, declined',
    is_first_pledge BOOLEAN DEFAULT FALSE COMMENT 'First pledge from this patron to this creator',
    started_at TIMESTAMP NOT NULL COMMENT 'Pledge start date',
    ended_at TIMESTAMP COMMENT 'Pledge end date (null if active)',
    pause_started_at TIMESTAMP COMMENT 'Current pause start',
    churn_reason STRING COMMENT 'voluntary, payment_failed, creator_removed',
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Pledge subscriptions - patron to creator relationships'
PARTITIONED BY (DATE_TRUNC('MONTH', started_at));

-- Transactions: Actual money movement
CREATE OR REPLACE TABLE transactions (
    transaction_id STRING NOT NULL COMMENT 'Unique transaction identifier',
    pledge_id STRING NOT NULL COMMENT 'FK to pledges',
    patron_id STRING NOT NULL COMMENT 'FK to patrons (denormalized)',
    creator_id STRING NOT NULL COMMENT 'FK to creators (denormalized)',
    transaction_type STRING NOT NULL COMMENT 'pledge_payment, refund, chargeback',
    transaction_status STRING NOT NULL COMMENT 'pending, succeeded, failed, refunded',
    gross_amount_usd DECIMAL(10,2) NOT NULL COMMENT 'Total charge amount',
    platform_fee_usd DECIMAL(10,2) COMMENT 'Patreon platform fee',
    processing_fee_usd DECIMAL(10,2) COMMENT 'Stripe/PayPal fee',
    net_amount_usd DECIMAL(10,2) COMMENT 'Creator payout amount',
    payment_method STRING COMMENT 'card, paypal, bank',
    failure_reason STRING COMMENT 'insufficient_funds, card_expired, etc.',
    transaction_at TIMESTAMP NOT NULL COMMENT 'Transaction timestamp',
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Payment transactions'
PARTITIONED BY (DATE_TRUNC('MONTH', transaction_at));

-- Posts: Creator content
CREATE OR REPLACE TABLE posts (
    post_id STRING NOT NULL,
    creator_id STRING NOT NULL,
    title STRING,
    post_type STRING COMMENT 'text, image, video, audio, poll, link',
    access_level STRING COMMENT 'public, patrons_only, tier_specific',
    minimum_tier_id STRING COMMENT 'Minimum tier required (if tier_specific)',
    published_at TIMESTAMP,
    is_pinned BOOLEAN DEFAULT FALSE,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Creator posts and content';

-- Engagement: Patron interactions with content
CREATE OR REPLACE TABLE engagement_events (
    event_id STRING NOT NULL,
    patron_id STRING NOT NULL,
    creator_id STRING NOT NULL,
    post_id STRING,
    event_type STRING NOT NULL COMMENT 'view, like, unlike, comment, share',
    event_at TIMESTAMP NOT NULL,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Patron engagement events'
PARTITIONED BY (DATE_TRUNC('MONTH', event_at));

-- ============================================================================
-- REFERENCE/LOOKUP TABLES
-- ============================================================================

CREATE OR REPLACE TABLE dim_date (
    date_key INT NOT NULL,
    full_date DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    month_name STRING,
    week_of_year INT,
    day_of_month INT,
    day_of_week INT,
    day_name STRING,
    is_weekend BOOLEAN,
    is_month_start BOOLEAN,
    is_month_end BOOLEAN,
    month_start_date DATE,
    month_end_date DATE
)
USING DELTA
COMMENT 'Date dimension for time-based analysis';

-- Populate date dimension (2020-2030)
INSERT INTO dim_date
SELECT 
    CAST(DATE_FORMAT(d, 'yyyyMMdd') AS INT) as date_key,
    d as full_date,
    YEAR(d) as year,
    QUARTER(d) as quarter,
    MONTH(d) as month,
    DATE_FORMAT(d, 'MMMM') as month_name,
    WEEKOFYEAR(d) as week_of_year,
    DAY(d) as day_of_month,
    DAYOFWEEK(d) as day_of_week,
    DATE_FORMAT(d, 'EEEE') as day_name,
    DAYOFWEEK(d) IN (1, 7) as is_weekend,
    DAY(d) = 1 as is_month_start,
    d = LAST_DAY(d) as is_month_end,
    DATE_TRUNC('MONTH', d) as month_start_date,
    LAST_DAY(d) as month_end_date
FROM (
    SELECT EXPLODE(SEQUENCE(DATE('2020-01-01'), DATE('2030-12-31'), INTERVAL 1 DAY)) as d
);

-- ============================================================================
-- CREATE INDEXES / OPTIMIZE
-- ============================================================================

-- Optimize tables with Z-ORDER for common query patterns
OPTIMIZE creators ZORDER BY (creator_id, category);
OPTIMIZE patrons ZORDER BY (patron_id);
OPTIMIZE pledges ZORDER BY (creator_id, patron_id, started_at);
OPTIMIZE transactions ZORDER BY (creator_id, transaction_at);
