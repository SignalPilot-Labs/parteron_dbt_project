# Synthetic Data Generation Prompt for SignalPilot

## Overview
Use this prompt with SignalPilot to generate realistic, statistically coherent synthetic data for the Patreon analytics platform. The data should exhibit real-world patterns including seasonality, cohort behavior, churn dynamics, and business-relevant anomalies for testing root cause analysis.

---

## PROMPT FOR SIGNALPILOT

```
Generate synthetic data for a Patreon-like creator economy platform with the following specifications:

### SCALE PARAMETERS
- Creators: 500 (distribution: 60% emerging, 30% established, 10% top-tier)
- Patrons: 15,000 unique accounts
- Time range: 24 months (January 2023 - December 2024)
- Pledges: ~25,000 total pledge records (including churned)
- Transactions: ~200,000 payment records
- Posts: ~8,000 content pieces
- Engagement events: ~500,000 interactions

### CREATOR DISTRIBUTION
Generate creators across these categories with realistic power-law distribution:
- podcasts (25%): History, true crime, comedy, interview shows
- video (20%): Tech explainers, gaming, tutorials, vlogs
- visual_art (18%): Digital art, comics, illustrations, photography
- education (15%): Programming, languages, music lessons, fitness
- writing (10%): Fiction, journalism, newsletters
- music (7%): Covers, original music, production tutorials
- games (5%): Game development, let's plays

Creator size tiers (by MRR):
- emerging: $0-$1,000 MRR, 1-50 patrons (60% of creators)
- established: $1,000-$10,000 MRR, 50-500 patrons (30% of creators)
- top_creator: $10,000+ MRR, 500+ patrons (10% of creators)

### PRICING TIERS
Each creator has 2-4 tiers following this pattern:
- Tier 1: $2-$5 (entry level, 50% of patrons)
- Tier 2: $7-$15 (mid tier, 35% of patrons)  
- Tier 3: $20-$50 (premium, 12% of patrons)
- Tier 4: $75-$150 (whale tier, 3% of patrons, only top creators)

### TEMPORAL PATTERNS

**Seasonality:**
- Q4 boost: 15-25% increase in new pledges (holiday giving)
- January surge: 10% spike in new patrons (New Year resolutions)
- Summer dip: 5-10% lower engagement (June-August)
- Monthly billing cycle: 70% of transactions on 1st-3rd of month

**Growth trajectories by creator tier:**
- Emerging: High volatility, 40% fail to reach 20 patrons within 6 months
- Established: 3-8% monthly growth, occasional viral spikes
- Top creators: Stable 1-3% growth, very low churn

### CHURN DYNAMICS

**Patron churn rates (monthly):**
- Overall: 6-8% monthly churn
- By patron tenure: 
  - Month 1: 25% churn (highest)
  - Months 2-3: 12% churn
  - Months 4-12: 6% churn
  - 12+ months: 3% churn (loyal base)

**Churn reasons distribution:**
- voluntary: 55% (patron decision)
- payment_failed: 35% (card declined, expired)
- creator_removed: 10% (creator removed patron)

**Payment failure patterns:**
- 8% of payment attempts fail initially
- 60% of failures recover within 7 days (retry success)
- Card expiration spikes in December/January

### ENGAGEMENT CORRELATIONS

**Content to retention correlation:**
- Creators posting 4+ times/month: 40% lower churn
- Creators posting <1 time/month: 2x higher churn
- Video content: highest engagement
- Engagement rate predicts next-month churn (inverse correlation)

**Engagement rates by tier:**
- Tier 1 patrons: 30% monthly engagement rate
- Tier 2 patrons: 50% monthly engagement rate
- Tier 3+ patrons: 70% monthly engagement rate

### ANOMALIES TO INJECT (for testing root cause analysis)

Include these realistic anomalies:
1. **Viral creator spike** (Month 8): One creator goes viral, gains 500 patrons in 2 weeks
2. **Payment processor outage** (Month 14): 40% decline rate for 3 days
3. **Creator exodus** (Month 18): 3 top creators leave platform, taking 2,000 patrons
4. **Seasonal churn spike** (Month 12): Post-holiday 15% higher churn
5. **Category collapse** (Month 20): Gaming category sees 30% MRR drop (market shift)

### REFERENTIAL INTEGRITY RULES

1. Every pledge must reference valid patron_id and creator_id
2. Every transaction must reference valid pledge_id
3. Patron first_pledge_at <= earliest pledge started_at
4. Creator first_pledge_received_at = earliest pledge to that creator
5. Transaction amounts match pledge amounts (with occasional tier upgrades)
6. Engagement events only for patrons with active pledges to that creator
7. Posts published_at must be after creator created_at

### OUTPUT FORMAT

Generate CSV files matching these schemas:

**creators.csv**: creator_id, creator_name, email, category, subcategory, country_code, currency_code, plan_type, is_nsfw, is_verified, created_at, first_pledge_received_at, last_post_at, status

**patrons.csv**: patron_id, patron_name, email, country_code, created_at, first_pledge_at, lifetime_spend_usd, status

**tiers.csv**: tier_id, creator_id, tier_name, tier_rank, price_usd, description, is_active, created_at, archived_at

**pledges.csv**: pledge_id, patron_id, creator_id, tier_id, pledge_amount_usd, pledge_status, is_first_pledge, started_at, ended_at, pause_started_at, churn_reason

**transactions.csv**: transaction_id, pledge_id, patron_id, creator_id, transaction_type, transaction_status, gross_amount_usd, platform_fee_usd, processing_fee_usd, net_amount_usd, payment_method, failure_reason, transaction_at

**posts.csv**: post_id, creator_id, title, post_type, access_level, minimum_tier_id, published_at, is_pinned

**engagement_events.csv**: event_id, patron_id, creator_id, post_id, event_type, event_at

### STATISTICAL COHERENCE CHECKS

After generation, validate:
- [ ] Total MRR grows ~50% over 24 months
- [ ] Churn rates average 6-8% monthly
- [ ] Payment success rate ~92%
- [ ] Engagement rate correlates negatively with churn
- [ ] Power law distribution in creator earnings (top 10% = 60% of revenue)
- [ ] Seasonal patterns visible in time series
- [ ] Anomalies detectable but not obvious
```

---

## USAGE NOTES

### For Testing SignalPilot Root Cause Analysis

The injected anomalies create investigation scenarios:

| Anomaly | Investigation Question | Expected Root Cause |
|---------|----------------------|-------------------|
| Viral spike | "Why did platform MRR jump 8% in Month 8?" | Single creator acquisition |
| Payment outage | "Why did October 14th revenue drop 40%?" | Payment processor failure |
| Creator exodus | "Why is Q2 2024 MRR declining?" | Top creator departures |
| Holiday churn | "Why did January churn spike?" | Post-holiday budget cuts |
| Gaming collapse | "Why is gaming category underperforming?" | Market/competition shift |

### Scaling Recommendations

For production-scale testing:
- 10x scale: 5,000 creators, 150,000 patrons, 2M transactions
- 100x scale: 50,000 creators, 1.5M patrons, 20M transactions

### Data Quality Hooks

The generated data should pass these dbt tests:
- Referential integrity on all foreign keys
- No future dates
- Amounts always positive
- Status values in allowed sets
- Timestamps in logical order

---

## ALTERNATIVE: PYTHON GENERATION SCRIPT

If you prefer programmatic generation, here's a starter:

```python
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
np.random.seed(42)

def generate_creators(n=500):
    categories = ['podcasts', 'video', 'visual_art', 'education', 'writing', 'music', 'games']
    weights = [0.25, 0.20, 0.18, 0.15, 0.10, 0.07, 0.05]
    
    # Power law for follower counts
    sizes = np.random.pareto(a=1.5, size=n) * 100
    sizes = np.clip(sizes, 5, 5000).astype(int)
    
    return pd.DataFrame({
        'creator_id': [f'cr_{i:04d}' for i in range(n)],
        'creator_name': [fake.company() for _ in range(n)],
        'category': np.random.choice(categories, n, p=weights),
        'created_at': [fake.date_time_between(start_date='-3y', end_date='-1y') for _ in range(n)],
        # ... continue with other fields
    })

# Similar functions for patrons, tiers, pledges, transactions, etc.
```
