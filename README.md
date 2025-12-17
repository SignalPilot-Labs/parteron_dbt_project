# Patreon Analytics dbt Project

A comprehensive dbt project for analyzing Patreon creator and patron data on Databricks. This project transforms raw platform data into actionable analytics for creator success, revenue tracking, and platform health monitoring.

## Table of Contents

- [Project Overview](#project-overview)
- [Data Architecture](#data-architecture)
- [Data Entities](#data-entities)
- [Setup Instructions](#setup-instructions)
- [Running dbt Commands](#running-dbt-commands)
- [Checking Data in dbt](#checking-data-in-dbt)
- [Checking Data in Databricks](#checking-data-in-databricks)
- [Key Metrics and KPIs](#key-metrics-and-kpis)
- [Testing and Data Quality](#testing-and-data-quality)

---

## Project Overview

This project models a Patreon-like membership platform with:
- **500 creators** across various content categories
- **15,000 patrons** supporting creators
- **21,310 pledges** (subscriptions)
- **185,141 transactions** (payments)
- **8,000 posts** published by creators
- **60,226 engagement events** (views, likes, comments)
- **1,412 membership tiers**

### Primary Use Cases

- Creator success dashboards
- Revenue forecasting and analysis
- Churn prediction features
- Platform health monitoring
- Fee and take-rate analysis

---

## Data Architecture

The project follows a **medallion architecture** with three layers:

```
+---------------------------------------------------------------------+
|                         DATABRICKS CATALOG                          |
+---------------------------------------------------------------------+
|                                                                     |
|  +----------------+   +------------------+   +-------------------+   |
|  | analytics_raw  |   |analytics_staging |   | analytics_marts   |   |
|  |    (Seeds)     |-->|     (Views)      |-->|     (Tables)      |   |
|  |                |   |                  |   |                   |   |
|  |  - creators    |   |  - stg_creators  |   | - fct_creator_    |   |
|  |  - patrons     |   |  - stg_patrons   |   |   monthly_        |   |
|  |  - pledges     |   |  - stg_pledges   |   |   performance     |   |
|  |  - transactions|   |  - stg_trans...  |   |                   |   |
|  |  - tiers       |   |  - stg_tiers     |   |                   |   |
|  |  - posts       |   |  - stg_posts     |   |                   |   |
|  |  - engagement_ |   |  - stg_engage... |   |                   |   |
|  |    events      |   |                  |   |                   |   |
|  +----------------+   +------------------+   +-------------------+   |
|                                                                     |
+---------------------------------------------------------------------+
```

### Layer Descriptions

| Layer | Schema | Materialization | Description |
|-------|--------|-----------------|-------------|
| **Raw** | `analytics_raw` | Seed (Table) | Source data loaded from CSV files |
| **Staging** | `analytics_staging` | View | Cleaned, typed, and enriched source data |
| **Marts** | `analytics_marts` | Table | Business-ready aggregated metrics |

---

## Data Entities

### 1. Creators (`seeds/creators.csv` -> `stg_creators`)

Content creators on the platform who receive financial support from patrons.

| Column | Type | Description |
|--------|------|-------------|
| `creator_id` | STRING | Unique identifier (e.g., `cr_0001`) |
| `creator_name` | STRING | Display name of the creator |
| `email` | STRING | Creator's email address |
| `category` | STRING | Primary content category (video, podcasts, writing, etc.) |
| `subcategory` | STRING | Content subcategory (tech, interviews, comedy, etc.) |
| `country_code` | STRING | ISO country code (US, CA, DE, NL, etc.) |
| `currency_code` | STRING | Preferred currency (USD, EUR) |
| `plan_type` | STRING | Patreon subscription plan: `lite`, `pro`, or `premium` |
| `is_nsfw` | BOOLEAN | Whether content is age-restricted |
| `is_verified` | BOOLEAN | Verification status |
| `created_at` | TIMESTAMP | Account creation date |
| `first_pledge_received_at` | TIMESTAMP | Date of first patron pledge |
| `last_post_at` | TIMESTAMP | Most recent content post date |
| `status` | STRING | Account status: `active`, `paused`, `suspended`, `deleted` |

**Derived fields in staging:**
- `days_to_first_pledge` - Days between account creation and first pledge
- `account_age_months` - Total months since account creation

---

### 2. Patrons (`seeds/patrons.csv` -> `stg_patrons`)

Users who financially support creators through subscriptions.

| Column | Type | Description |
|--------|------|-------------|
| `patron_id` | STRING | Unique identifier (e.g., `pa_00001`) |
| `patron_name` | STRING | Display name |
| `email` | STRING | Patron's email address |
| `country_code` | STRING | ISO country code |
| `created_at` | TIMESTAMP | Account creation date |
| `first_pledge_at` | TIMESTAMP | Date of first pledge |
| `lifetime_spend_usd` | DECIMAL | Total amount spent on the platform |
| `status` | STRING | Account status |

**Derived fields in staging:**
- `patron_value_tier` - Classification based on lifetime spend: `whale`, `high_value`, `regular`, `casual`

---

### 3. Tiers (`seeds/tiers.csv` -> `stg_tiers`)

Membership levels created by creators with different price points and benefits.

| Column | Type | Description |
|--------|------|-------------|
| `tier_id` | STRING | Unique identifier (e.g., `ti_00001`) |
| `creator_id` | STRING | FK to creator |
| `tier_name` | STRING | Name (e.g., "Viewer", "Plus", "VIP") |
| `tier_rank` | INTEGER | Order/rank within creator's tiers (1 = lowest) |
| `price_usd` | DECIMAL | Monthly subscription price |
| `description` | STRING | Tier benefits description |
| `is_active` | BOOLEAN | Whether tier is currently available |
| `created_at` | TIMESTAMP | Creation date |
| `archived_at` | TIMESTAMP | Archive date (if inactive) |

---

### 4. Pledges (`seeds/pledges.csv` -> `stg_pledges`)

Subscription relationships between patrons and creators.

| Column | Type | Description |
|--------|------|-------------|
| `pledge_id` | STRING | Unique identifier (e.g., `pl_000001`) |
| `patron_id` | STRING | FK to patron |
| `creator_id` | STRING | FK to creator |
| `tier_id` | STRING | FK to tier |
| `pledge_amount_usd` | DECIMAL | Monthly pledge amount |
| `pledge_status` | STRING | Status: `active`, `paused`, `churned`, `declined` |
| `is_first_pledge` | BOOLEAN | Whether this is the patron's first pledge to any creator |
| `started_at` | TIMESTAMP | Subscription start date |
| `ended_at` | TIMESTAMP | Subscription end date (if churned) |
| `pause_started_at` | TIMESTAMP | Pause date (if paused) |
| `churn_reason` | STRING | Reason for cancellation (e.g., `payment_failed`, `voluntary`) |

**Derived fields in staging:**
- `pledge_month` - Month the pledge started
- `pledge_duration_days` - Length of subscription
- `is_currently_active` - Real-time active status check

---

### 5. Transactions (`seeds/transactions.csv` -> `stg_transactions`)

Payment records for pledge subscriptions.

| Column | Type | Description |
|--------|------|-------------|
| `transaction_id` | STRING | Unique identifier (e.g., `tx_0000001`) |
| `pledge_id` | STRING | FK to pledge |
| `patron_id` | STRING | FK to patron |
| `creator_id` | STRING | FK to creator |
| `transaction_type` | STRING | Type: `pledge_payment`, `refund`, `chargeback` |
| `transaction_status` | STRING | Status: `pending`, `succeeded`, `failed`, `refunded` |
| `gross_amount_usd` | DECIMAL | Total amount charged |
| `platform_fee_usd` | DECIMAL | Patreon's fee |
| `processing_fee_usd` | DECIMAL | Payment processor fee |
| `net_amount_usd` | DECIMAL | Amount paid to creator |
| `payment_method` | STRING | Method: `card`, `paypal` |
| `failure_reason` | STRING | Reason if failed |
| `transaction_at` | TIMESTAMP | Transaction timestamp |

**Derived fields in staging:**
- `transaction_month` / `transaction_date` - Time dimensions
- `platform_fee_rate_pct` - Platform fee as percentage
- `processing_fee_rate_pct` - Processing fee as percentage
- `is_successful` - Binary success flag

---

### 6. Posts (`seeds/posts.csv` -> `stg_posts`)

Content published by creators.

| Column | Type | Description |
|--------|------|-------------|
| `post_id` | STRING | Unique identifier (e.g., `po_00001`) |
| `creator_id` | STRING | FK to creator |
| `title` | STRING | Post title |
| `post_type` | STRING | Content type: `article`, `image`, `video` |
| `access_level` | STRING | Visibility: `public`, `patron_only`, `tier_specific` |
| `minimum_tier_id` | STRING | FK to minimum tier required (if paywalled) |
| `published_at` | TIMESTAMP | Publication timestamp |
| `is_pinned` | BOOLEAN | Whether post is pinned to profile |

**Derived fields in staging:**
- `published_month` / `published_date` - Time dimensions
- `content_access_type` - Simplified access classification: `free`, `paywalled`, `premium`

---

### 7. Engagement Events (`seeds/engagement_events.csv` -> `stg_engagement_events`)

Patron interactions with creator content.

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | STRING | Unique identifier (e.g., `ev_0000001`) |
| `patron_id` | STRING | FK to patron |
| `creator_id` | STRING | FK to creator |
| `post_id` | STRING | FK to post |
| `event_type` | STRING | Type: `view`, `like`, `unlike`, `comment`, `share` |
| `event_at` | TIMESTAMP | Event timestamp |

**Derived fields in staging:**
- `event_month` / `event_date` - Time dimensions
- `engagement_weight` - Weighted score for engagement scoring:
  - `view` = 1
  - `like` = 3
  - `comment` = 5
  - `share` = 7

---

### 8. Creator Monthly Performance (`fct_creator_monthly_performance`)

**Grain:** One row per creator per month

Aggregated mart table combining all metrics for comprehensive creator analytics.

| Metric Category | Columns |
|-----------------|---------|
| **Patron Metrics** | `total_patrons`, `active_patrons`, `new_patrons`, `churned_patrons`, `net_patron_change`, `patron_churn_rate_pct` |
| **MRR Metrics** | `gross_mrr_usd`, `new_mrr_usd`, `churned_mrr_usd`, `mrr_change_usd`, `mrr_growth_rate_pct` |
| **Revenue Metrics** | `gross_revenue_usd`, `platform_fees_usd`, `processing_fees_usd`, `net_creator_earnings_usd`, `collection_rate_pct` |
| **Payment Health** | `successful_transactions`, `failed_transactions`, `declined_amount_usd`, `decline_rate_pct` |
| **Tier Distribution** | `tier_1_patrons`, `tier_2_patrons`, `tier_3_plus_patrons`, `avg_pledge_amount_usd` |
| **Content Metrics** | `posts_published`, `paywalled_posts`, `free_posts` |
| **Engagement Metrics** | `total_views`, `total_likes`, `total_comments`, `engaged_patrons`, `total_engagement_score`, `patron_engagement_rate_pct` |

---

## Setup Instructions

### Prerequisites

1. **Python 3.8+** with pip
2. **Databricks workspace** with SQL warehouse access
3. **dbt-core** and **dbt-databricks** adapter

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd patreon_dbt_project

# Install dbt with Databricks adapter
pip install dbt-databricks

# Install project dependencies
dbt deps
```

### Configure Databricks Connection

Set environment variables for authentication:

```bash
export DATABRICKS_HOST="your-workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
export DATABRICKS_TOKEN="your-personal-access-token"
```

Or create a `~/.dbt/profiles.yml`:

```yaml
patreon_databricks:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: patreon_dev
      schema: analytics
      host: "your-workspace.cloud.databricks.com"
      http_path: "/sql/1.0/warehouses/your-warehouse-id"
      token: "your-personal-access-token"
      threads: 4
```

### Verify Connection

```bash
dbt debug
```

---

## Running dbt Commands

### Initial Setup (Run in Order)

```bash
# 1. Install packages (dbt_utils, dbt_expectations, codegen)
dbt deps

# 2. Load seed data into Databricks [WARNING: ONLY FIRST TIME, DATA ALREADY THERE]
dbt seed

# 3. Build all models
dbt run
```

### Common Commands

```bash
# Run all models
dbt run

# Run specific model
dbt run --select fct_creator_monthly_performance

# Run staging models only
dbt run --select staging.*

# Run with full refresh (rebuild tables)
dbt run --full-refresh

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve

# Compile SQL without running
dbt compile
```

### Selective Runs

```bash
# Run a model and all its upstream dependencies
dbt run --select +fct_creator_monthly_performance

# Run a model and all downstream models
dbt run --select stg_creators+

# Run models with specific tag
dbt run --select tag:core
```

---

## Checking Data in dbt

### Preview Data with `dbt show`

```bash
# Preview staging model data
dbt show --select stg_creators --limit 10

# Preview mart data
dbt show --select fct_creator_monthly_performance --limit 5
```

### Run Ad-hoc Queries

Create a SQL file in `analyses/` folder:

```sql
-- analyses/top_creators_by_revenue.sql
select
    creator_name,
    sum(gross_revenue_usd) as total_revenue,
    sum(active_patrons) as total_patrons
from {{ ref('fct_creator_monthly_performance') }}
group by 1
order by 2 desc
limit 20
```

Then compile:
```bash
dbt compile --select analyses/top_creators_by_revenue
```

### Run Tests

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select stg_creators

# Show test failures only
dbt test --store-failures
```

### Check Data Freshness

```bash
dbt source freshness
```

---

## Checking Data in Databricks

### Access Data in Databricks SQL

After running `dbt seed` and `dbt run`, data is available in these schemas:

| Schema | Description | Example Query |
|--------|-------------|---------------|
| `patreon_dev.analytics_raw` | Raw seed data | `SELECT * FROM patreon_dev.analytics_raw.creators` |
| `patreon_dev.analytics_staging` | Staged views | `SELECT * FROM patreon_dev.analytics_staging.stg_creators` |
| `patreon_dev.analytics_marts` | Mart tables | `SELECT * FROM patreon_dev.analytics_marts.fct_creator_monthly_performance` |

### Sample Queries

#### 1. View Raw Data

```sql
-- Check raw creators
SELECT * FROM patreon_dev.analytics_raw.creators LIMIT 10;

-- Count records per table
SELECT 'creators' as table_name, COUNT(*) as row_count FROM patreon_dev.analytics_raw.creators
UNION ALL
SELECT 'patrons', COUNT(*) FROM patreon_dev.analytics_raw.patrons
UNION ALL
SELECT 'pledges', COUNT(*) FROM patreon_dev.analytics_raw.pledges
UNION ALL
SELECT 'transactions', COUNT(*) FROM patreon_dev.analytics_raw.transactions
UNION ALL
SELECT 'tiers', COUNT(*) FROM patreon_dev.analytics_raw.tiers
UNION ALL
SELECT 'posts', COUNT(*) FROM patreon_dev.analytics_raw.posts
UNION ALL
SELECT 'engagement_events', COUNT(*) FROM patreon_dev.analytics_raw.engagement_events;
```

#### 2. Explore Staging Data

```sql
-- View staged creators with derived fields
SELECT
    creator_id,
    creator_name,
    category,
    plan_type,
    days_to_first_pledge,
    account_age_months
FROM patreon_dev.analytics_staging.stg_creators
LIMIT 20;

-- Analyze pledge status distribution
SELECT
    pledge_status,
    COUNT(*) as pledge_count,
    ROUND(AVG(pledge_amount_usd), 2) as avg_pledge_amount
FROM patreon_dev.analytics_staging.stg_pledges
GROUP BY pledge_status
ORDER BY pledge_count DESC;

-- Transaction success rates
SELECT
    transaction_status,
    COUNT(*) as tx_count,
    ROUND(SUM(gross_amount_usd), 2) as total_amount
FROM patreon_dev.analytics_staging.stg_transactions
GROUP BY transaction_status;
```

#### 3. Analyze Mart Data

```sql
-- Top 10 creators by MRR (latest month)
SELECT
    creator_name,
    creator_category,
    plan_type,
    gross_mrr_usd,
    active_patrons,
    patron_engagement_rate_pct
FROM patreon_dev.analytics_marts.fct_creator_monthly_performance
WHERE month_start_date = (
    SELECT MAX(month_start_date)
    FROM patreon_dev.analytics_marts.fct_creator_monthly_performance
)
ORDER BY gross_mrr_usd DESC
LIMIT 10;

-- Monthly platform trends
SELECT
    month_start_date,
    SUM(gross_revenue_usd) as total_revenue,
    SUM(platform_fees_usd) as total_platform_fees,
    SUM(active_patrons) as total_patrons,
    COUNT(DISTINCT creator_id) as active_creators
FROM patreon_dev.analytics_marts.fct_creator_monthly_performance
GROUP BY month_start_date
ORDER BY month_start_date;

-- Churn analysis by category
SELECT
    creator_category,
    ROUND(AVG(patron_churn_rate_pct), 2) as avg_churn_rate,
    ROUND(AVG(collection_rate_pct), 2) as avg_collection_rate,
    SUM(churned_patrons) as total_churned
FROM patreon_dev.analytics_marts.fct_creator_monthly_performance
GROUP BY creator_category
ORDER BY avg_churn_rate DESC;
```

#### 4. Revenue and Fee Analysis

```sql
-- Platform take rate by plan type
SELECT
    plan_type,
    ROUND(SUM(gross_revenue_usd), 2) as gross_revenue,
    ROUND(SUM(platform_fees_usd), 2) as platform_fees,
    ROUND(SUM(platform_fees_usd) / SUM(gross_revenue_usd) * 100, 2) as take_rate_pct
FROM patreon_dev.analytics_marts.fct_creator_monthly_performance
GROUP BY plan_type;

-- Transaction success rate over time
SELECT
    month_start_date,
    SUM(successful_transactions) as successful,
    SUM(failed_transactions) as failed,
    ROUND(SUM(successful_transactions) * 100.0 /
          NULLIF(SUM(successful_transactions) + SUM(failed_transactions), 0), 2) as success_rate_pct
FROM patreon_dev.analytics_marts.fct_creator_monthly_performance
GROUP BY month_start_date
ORDER BY month_start_date;
```

#### 5. Engagement Analysis

```sql
-- Content engagement by category
SELECT
    creator_category,
    SUM(posts_published) as total_posts,
    SUM(total_views) as total_views,
    SUM(total_likes) as total_likes,
    SUM(total_comments) as total_comments,
    ROUND(AVG(patron_engagement_rate_pct), 2) as avg_engagement_rate
FROM patreon_dev.analytics_marts.fct_creator_monthly_performance
GROUP BY creator_category
ORDER BY total_views DESC;

-- High-performing creators (high engagement + revenue)
SELECT
    creator_name,
    creator_category,
    gross_mrr_usd,
    patron_engagement_rate_pct,
    posts_published
FROM patreon_dev.analytics_marts.fct_creator_monthly_performance
WHERE month_start_date = (
    SELECT MAX(month_start_date)
    FROM patreon_dev.analytics_marts.fct_creator_monthly_performance
)
  AND patron_engagement_rate_pct > 50
  AND gross_mrr_usd > 1000
ORDER BY gross_mrr_usd DESC;
```

### Using Databricks SQL Editor

1. Navigate to **Databricks Workspace** -> **SQL** -> **SQL Editor**
2. Select your SQL Warehouse from the dropdown
3. Choose the catalog `patreon_dev` in the schema browser
4. Run queries against any of the three schemas

### Creating Dashboards

1. Go to **SQL** -> **Dashboards** -> **Create Dashboard**
2. Add visualizations using the queries above
3. Suggested dashboards:
   - **Platform Overview**: Total MRR, patrons, revenue trends
   - **Creator Performance**: Top creators, category breakdown
   - **Health Metrics**: Churn rates, decline rates, collection rates
   - **Engagement Dashboard**: Views, likes, comments by category

---

## Key Metrics and KPIs

### Health Thresholds

| Metric | Healthy | Warning | Critical |
|--------|---------|---------|----------|
| `patron_churn_rate_pct` | < 5% | 5-10% | > 10% |
| `collection_rate_pct` | > 95% | 90-95% | < 90% |
| `decline_rate_pct` | < 10% | 10-15% | > 15% |

### Key Performance Indicators

| KPI | Description | Column |
|-----|-------------|--------|
| **Gross MRR** | Sum of all active pledge amounts | `gross_mrr_usd` |
| **Net Creator Earnings** | Creator payout after all fees | `net_creator_earnings_usd` |
| **Patron Engagement Rate** | Leading indicator for retention | `patron_engagement_rate_pct` |
| **Platform Take Rate** | Platform fees as % of gross revenue | Calculated: `platform_fees_usd / gross_revenue_usd` |
| **Active Patrons** | Currently subscribing patrons | `active_patrons` |
| **Collection Rate** | Revenue collected vs pledged | `collection_rate_pct` |

---

## Testing and Data Quality

### Built-in Tests

The project includes 36 data tests:

| Test Type | Description | Example |
|-----------|-------------|---------|
| `unique` | Ensures primary keys are unique | `creator_id`, `pledge_id` |
| `not_null` | Validates required fields | All primary/foreign keys |
| `accepted_values` | Validates enum fields | `plan_type`, `pledge_status` |
| `relationships` | Validates foreign key integrity | `pledges.creator_id` -> `creators.creator_id` |

### Run Tests

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select stg_pledges

# Show test failures with details
dbt test --store-failures
```

### Data Quality Checks in Databricks

```sql
-- Check for orphaned pledges (no matching creator)
SELECT p.pledge_id, p.creator_id
FROM patreon_dev.analytics_staging.stg_pledges p
LEFT JOIN patreon_dev.analytics_staging.stg_creators c ON p.creator_id = c.creator_id
WHERE c.creator_id IS NULL;

-- Check transaction amount consistency
SELECT
    transaction_id,
    gross_amount_usd,
    platform_fee_usd + processing_fee_usd + net_amount_usd as calculated_gross,
    gross_amount_usd - (platform_fee_usd + processing_fee_usd + net_amount_usd) as discrepancy
FROM patreon_dev.analytics_staging.stg_transactions
WHERE ABS(gross_amount_usd - (platform_fee_usd + processing_fee_usd + net_amount_usd)) > 0.01;
```

---

## Project Structure

```
patreon_dbt_project/
|-- dbt_project.yml          # Project configuration
|-- profiles.yml             # Connection profiles
|-- packages.yml             # Package dependencies
|-- seeds/                   # CSV source data
|   |-- creators.csv
|   |-- patrons.csv
|   |-- pledges.csv
|   |-- transactions.csv
|   |-- tiers.csv
|   |-- posts.csv
|   +-- engagement_events.csv
|-- models/
|   |-- staging/             # Staging views
|   |   |-- schema.yml
|   |   |-- stg_creators.sql
|   |   |-- stg_patrons.sql
|   |   |-- stg_pledges.sql
|   |   |-- stg_transactions.sql
|   |   |-- stg_tiers.sql
|   |   |-- stg_posts.sql
|   |   +-- stg_engagement_events.sql
|   |-- marts/
|   |   +-- core/
|   |       |-- schema.yml
|   |       +-- fct_creator_monthly_performance.sql
|   +-- utilities/
|       +-- metricflow_time_spine.sql
|-- macros/                  # Custom macros
|-- tests/                   # Custom tests
|-- analyses/                # Ad-hoc queries
+-- snapshots/               # SCD tracking
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `Connection refused` | Check DATABRICKS_HOST doesn't have `https://` prefix |
| `Catalog not found` | Ensure catalog exists or update `profiles.yml` |
| `Permission denied` | Ensure token has CREATE TABLE permissions |
| `dbt deps fails` | Check internet connectivity, try `dbt clean` first |
| `Seed fails on types` | Check `dbt_project.yml` column_types configuration |
| `Model compilation error` | Run `dbt compile` to see detailed SQL errors |

---

## Additional Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt-databricks Adapter](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup)
- [Databricks SQL Documentation](https://docs.databricks.com/sql/index.html)
- [dbt Best Practices](https://docs.getdbt.com/best-practices)
