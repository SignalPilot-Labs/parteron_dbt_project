# Bootstrapping dbt from Existing Databricks Tables

A practical guide for constructing dbt structures from existing Databricks tables, combining automated tooling with LLM-assisted enrichment.

---

## Overview

```
Databricks Tables → Metadata Extraction → dbt Scaffolding → Enrichment → Validation
```

This workflow takes you from raw Databricks tables to a fully documented dbt project with:
- Source definitions (`sources.yml`)
- Staging models (`stg_*.sql`)
- Schema documentation (`schema.yml`)
- Tests and semantic metadata

---

## Step 1: Discover Your Tables

Query Databricks `INFORMATION_SCHEMA` to understand what you're working with.

### List All Tables in a Schema

```sql
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type,
    created,
    last_altered
FROM system.information_schema.tables
WHERE table_schema = 'your_raw_schema'
ORDER BY table_name;
```

### Get Column Details for a Table

```sql
SELECT
    column_name,
    data_type,
    is_nullable,
    column_default,
    comment  -- Databricks column comments become descriptions
FROM system.information_schema.columns
WHERE table_schema = 'your_raw_schema'
  AND table_name = 'your_table'
ORDER BY ordinal_position;
```

### Get Table Statistics

```sql
-- Row counts and basic stats
SELECT
    table_name,
    COUNT(*) as row_count
FROM your_catalog.your_schema.your_table;

-- Column cardinality (useful for detecting enums/dimensions)
SELECT
    COUNT(DISTINCT column_name) as cardinality,
    COUNT(*) as total_rows
FROM your_catalog.your_schema.your_table;
```

---

## Step 2: Generate Sources (Using dbt-codegen)

The `dbt-labs/codegen` package automates source generation.

### Install codegen

Add to `packages.yml`:

```yaml
packages:
  - package: dbt-labs/codegen
    version: ">=0.12.0"
```

Then run:

```bash
dbt deps
```

### Generate Sources YAML

```bash
dbt run-operation generate_source \
  --args '{
    "schema_name": "raw",
    "database_name": "your_catalog",
    "generate_columns": true,
    "include_descriptions": true,
    "include_data_types": true
  }'
```

This outputs YAML to paste into `models/staging/sources.yml`:

```yaml
sources:
  - name: raw_data
    database: your_catalog
    schema: raw
    tables:
      - name: customers
        columns:
          - name: customer_id
            data_type: bigint
          - name: email
            data_type: string
          - name: created_at
            data_type: timestamp
      - name: orders
        columns:
          - name: order_id
            data_type: bigint
          - name: customer_id
            data_type: bigint
          - name: total_amount
            data_type: decimal(10,2)
```

---

## Step 3: Generate Staging Models

For each source table, generate a base staging model.

### Generate a Single Model

```bash
dbt run-operation generate_base_model \
  --args '{"source_name": "raw_data", "table_name": "customers"}'
```

### Output Example

Creates `models/staging/stg_customers.sql`:

```sql
with source as (
    select * from {{ source('raw_data', 'customers') }}
),

renamed as (
    select
        customer_id,
        email,
        created_at
    from source
)

select * from renamed
```

### Enhance with Transformations

After generation, enhance with derived fields:

```sql
with source as (
    select * from {{ source('raw_data', 'customers') }}
),

transformed as (
    select
        -- Primary key
        customer_id,

        -- Dimensions
        email,
        lower(email) as email_normalized,
        split(email, '@')[1] as email_domain,

        -- Timestamps
        created_at,
        date(created_at) as created_date,

        -- Derived fields
        datediff(current_date(), date(created_at)) as account_age_days,

        -- Metadata
        current_timestamp() as _loaded_at
    from source
)

select * from transformed
```

---

## Step 4: Generate Schema YAML

After creating staging models, generate the schema documentation.

```bash
dbt run-operation generate_model_yaml \
  --args '{"model_names": ["stg_customers", "stg_orders", "stg_products"]}'
```

### Output Example

```yaml
models:
  - name: stg_customers
    columns:
      - name: customer_id
      - name: email
      - name: email_normalized
      - name: email_domain
      - name: created_at
      - name: created_date
      - name: account_age_days
      - name: _loaded_at
```

---

## Step 5: Enrich with Semantic Metadata

This is where LLM assistance becomes valuable. Enhance the generated YAML with:

### Semantic Type Tags

```yaml
columns:
  - name: customer_id
    description: Unique identifier for the customer
    data_tests:
      - unique
      - not_null
    meta:
      semantic_type: primary_key

  - name: order_id
    description: Foreign key to orders table
    data_tests:
      - not_null
      - relationships:
          to: ref('stg_orders')
          field: order_id
    meta:
      semantic_type: foreign_key

  - name: total_revenue
    description: Total revenue in USD
    meta:
      semantic_type: measure
      aggregation: sum
      unit: usd
      kpi: true

  - name: customer_segment
    description: Customer classification
    data_tests:
      - accepted_values:
          values: ['enterprise', 'mid_market', 'smb', 'consumer']
    meta:
      semantic_type: dimension
```

### Alert Thresholds for KPIs

```yaml
  - name: churn_rate_pct
    description: Monthly churn rate - healthy is < 5%
    meta:
      semantic_type: measure
      aggregation: average
      unit: percent
      kpi: true
      alert_threshold_high: 10

  - name: collection_rate_pct
    description: Payment success rate - should be > 95%
    meta:
      semantic_type: measure
      unit: percent
      alert_threshold_low: 90
```

---

## Automated Metadata Extraction Script

For building a tool, use this Python script to extract rich metadata:

```python
# scripts/extract_databricks_metadata.py

from databricks import sql
import json
import os

def get_connection():
    """Create Databricks connection from environment variables."""
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )

def extract_table_metadata(catalog: str, schema: str) -> dict:
    """Extract comprehensive metadata for all tables in a schema."""

    conn = get_connection()
    cursor = conn.cursor()

    # Get all tables
    cursor.execute(f"""
        SELECT table_name, table_type
        FROM {catalog}.information_schema.tables
        WHERE table_schema = '{schema}'
    """)
    tables = cursor.fetchall()

    metadata = {}

    for table_name, table_type in tables:
        # Get columns
        cursor.execute(f"""
            SELECT
                column_name,
                data_type,
                is_nullable,
                comment
            FROM {catalog}.information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()

        # Get sample data
        cursor.execute(f"""
            SELECT * FROM {catalog}.{schema}.{table_name} LIMIT 5
        """)
        sample_data = cursor.fetchall()
        sample_columns = [desc[0] for desc in cursor.description]

        # Get row count
        cursor.execute(f"""
            SELECT COUNT(*) FROM {catalog}.{schema}.{table_name}
        """)
        row_count = cursor.fetchone()[0]

        # Get column cardinalities (for enum detection)
        cardinalities = {}
        for col in columns:
            col_name = col[0]
            try:
                cursor.execute(f"""
                    SELECT COUNT(DISTINCT {col_name})
                    FROM {catalog}.{schema}.{table_name}
                """)
                cardinalities[col_name] = cursor.fetchone()[0]
            except:
                cardinalities[col_name] = None

        metadata[table_name] = {
            "table_type": table_type,
            "row_count": row_count,
            "columns": [
                {
                    "name": c[0],
                    "data_type": c[1],
                    "nullable": c[2] == "YES",
                    "comment": c[3],
                    "cardinality": cardinalities.get(c[0])
                } for c in columns
            ],
            "sample_data": [
                dict(zip(sample_columns, row)) for row in sample_data
            ],
            "inferred": {
                "primary_key": infer_primary_key(table_name, columns),
                "foreign_keys": infer_foreign_keys(columns, [t[0] for t in tables]),
                "dimensions": infer_dimensions(columns, cardinalities),
                "measures": infer_measures(columns),
                "timestamps": infer_timestamps(columns)
            }
        }

    cursor.close()
    conn.close()

    return metadata


def infer_primary_key(table_name: str, columns: list) -> str | None:
    """Infer primary key from naming conventions."""
    col_names = [c[0].lower() for c in columns]

    # Check for 'id' column
    if 'id' in col_names:
        return 'id'

    # Check for '{table_singular}_id'
    singular = table_name.rstrip('s').lower()
    pk_candidate = f"{singular}_id"
    if pk_candidate in col_names:
        return pk_candidate

    # First column ending in '_id'
    for col in columns:
        if col[0].lower().endswith('_id'):
            return col[0]

    return None


def infer_foreign_keys(columns: list, all_tables: list) -> list:
    """Infer foreign keys by matching column names to table names."""
    fks = []
    table_names_lower = [t.lower() for t in all_tables]

    for col in columns:
        col_name = col[0].lower()

        if col_name.endswith('_id') and col_name != 'id':
            # e.g., customer_id -> customers
            base_name = col_name.replace('_id', '')

            # Try plural form
            if f"{base_name}s" in table_names_lower:
                fks.append({
                    "column": col[0],
                    "references_table": f"{base_name}s",
                    "references_column": col[0]
                })
            # Try singular form
            elif base_name in table_names_lower:
                fks.append({
                    "column": col[0],
                    "references_table": base_name,
                    "references_column": col[0]
                })

    return fks


def infer_dimensions(columns: list, cardinalities: dict) -> list:
    """Identify likely dimension columns."""
    dimensions = []

    for col in columns:
        col_name = col[0].lower()
        data_type = col[1].lower()
        cardinality = cardinalities.get(col[0])

        # String columns with low cardinality are likely dimensions
        if 'string' in data_type or 'varchar' in data_type:
            if cardinality and cardinality < 100:
                dimensions.append({
                    "column": col[0],
                    "cardinality": cardinality,
                    "likely_enum": cardinality < 20
                })

        # Boolean columns
        if 'boolean' in data_type:
            dimensions.append({
                "column": col[0],
                "cardinality": 2,
                "likely_enum": True
            })

        # Columns with dimension-like names
        dimension_patterns = ['_type', '_status', '_category', '_segment',
                            '_tier', '_level', '_group', '_class']
        if any(pattern in col_name for pattern in dimension_patterns):
            dimensions.append({
                "column": col[0],
                "cardinality": cardinality,
                "likely_enum": True
            })

    return dimensions


def infer_measures(columns: list) -> list:
    """Identify likely measure columns."""
    measures = []

    measure_patterns = ['_amount', '_total', '_sum', '_count', '_qty',
                       '_quantity', '_price', '_cost', '_revenue', '_fee',
                       '_rate', '_pct', '_percent', '_score', '_value']

    for col in columns:
        col_name = col[0].lower()
        data_type = col[1].lower()

        # Numeric columns with measure-like names
        if any(t in data_type for t in ['int', 'decimal', 'double', 'float', 'numeric']):
            if any(pattern in col_name for pattern in measure_patterns):
                # Infer aggregation type
                if '_pct' in col_name or '_percent' in col_name or '_rate' in col_name:
                    agg = 'average'
                    unit = 'percent'
                elif '_usd' in col_name or '_amount' in col_name or 'price' in col_name:
                    agg = 'sum'
                    unit = 'usd'
                elif '_count' in col_name or '_qty' in col_name:
                    agg = 'sum'
                    unit = 'count'
                else:
                    agg = 'sum'
                    unit = None

                measures.append({
                    "column": col[0],
                    "aggregation": agg,
                    "unit": unit
                })

    return measures


def infer_timestamps(columns: list) -> list:
    """Identify timestamp columns."""
    timestamps = []

    timestamp_patterns = ['_at', '_date', '_time', '_ts', '_timestamp',
                         'created', 'updated', 'modified', 'deleted']

    for col in columns:
        col_name = col[0].lower()
        data_type = col[1].lower()

        if 'timestamp' in data_type or 'date' in data_type:
            timestamps.append({"column": col[0], "data_type": col[1]})
        elif any(pattern in col_name for pattern in timestamp_patterns):
            timestamps.append({"column": col[0], "data_type": col[1]})

    return timestamps


def save_metadata(metadata: dict, output_path: str):
    """Save metadata to JSON file."""
    with open(output_path, 'w') as f:
        json.dump(metadata, f, indent=2, default=str)
    print(f"Metadata saved to {output_path}")


if __name__ == "__main__":
    import sys

    catalog = sys.argv[1] if len(sys.argv) > 1 else "your_catalog"
    schema = sys.argv[2] if len(sys.argv) > 2 else "raw"

    print(f"Extracting metadata from {catalog}.{schema}...")
    metadata = extract_table_metadata(catalog, schema)

    save_metadata(metadata, f"metadata_{catalog}_{schema}.json")

    # Print summary
    print(f"\nFound {len(metadata)} tables:")
    for table, info in metadata.items():
        print(f"  - {table}: {info['row_count']:,} rows, {len(info['columns'])} columns")
        if info['inferred']['primary_key']:
            print(f"      PK: {info['inferred']['primary_key']}")
        if info['inferred']['foreign_keys']:
            print(f"      FKs: {[fk['column'] for fk in info['inferred']['foreign_keys']]}")
```

---

## LLM Prompt for Schema Enrichment

Use extracted metadata to generate enriched schema via LLM:

```python
def generate_llm_prompt(table_name: str, metadata: dict) -> str:
    """Generate prompt for LLM to create enriched dbt schema."""

    table = metadata[table_name]

    return f"""
Generate a dbt schema.yml entry for this Databricks table.

Requirements:
1. Write clear, business-friendly column descriptions
2. Add semantic_type meta tags: primary_key, foreign_key, dimension, measure, timestamp
3. Add appropriate data_tests: unique, not_null, accepted_values, relationships
4. For measures, include aggregation (sum/average) and unit (usd/percent/count)
5. Mark important business metrics with kpi: true
6. Add alert_threshold_high or alert_threshold_low for KPIs where appropriate

TABLE: {table_name}
ROW COUNT: {table['row_count']:,}

COLUMNS:
{json.dumps(table['columns'], indent=2)}

SAMPLE DATA:
{json.dumps(table['sample_data'], indent=2)}

INFERRED METADATA:
- Primary Key: {table['inferred']['primary_key']}
- Foreign Keys: {json.dumps(table['inferred']['foreign_keys'])}
- Dimensions: {json.dumps(table['inferred']['dimensions'])}
- Measures: {json.dumps(table['inferred']['measures'])}
- Timestamps: {json.dumps(table['inferred']['timestamps'])}

Output format:
```yaml
models:
  - name: stg_{table_name}
    description: |
      [Business description of what this table represents]
    meta:
      owner: [team]
      tier: [bronze/silver/gold]
      domain: [domain]
    columns:
      - name: [column]
        description: [description]
        data_tests:
          - [tests]
        meta:
          semantic_type: [type]
          # additional meta as appropriate
```
"""
```

---

## Workflow Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    DATABRICKS → DBT BOOTSTRAPPING                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. EXTRACT                                                              │
│     ┌──────────────┐                                                    │
│     │  Databricks  │                                                    │
│     │  Tables      │                                                    │
│     └──────┬───────┘                                                    │
│            │                                                             │
│            ├──→ INFORMATION_SCHEMA.tables                               │
│            ├──→ INFORMATION_SCHEMA.columns                              │
│            ├──→ Sample data (LIMIT 5-10)                                │
│            └──→ Cardinality counts                                      │
│                                                                          │
│            ▼                                                             │
│  2. INFER                                                                │
│     ┌──────────────┐                                                    │
│     │  Pattern     │                                                    │
│     │  Matching    │                                                    │
│     └──────┬───────┘                                                    │
│            │                                                             │
│            ├──→ Primary keys (*_id naming)                              │
│            ├──→ Foreign keys (table name matching)                      │
│            ├──→ Dimensions (low cardinality strings)                    │
│            ├──→ Measures (numeric + *_amount, *_total)                  │
│            └──→ Timestamps (*_at, *_date columns)                       │
│                                                                          │
│            ▼                                                             │
│  3. GENERATE                                                             │
│     ┌──────────────┐                                                    │
│     │  dbt         │                                                    │
│     │  codegen     │                                                    │
│     └──────┬───────┘                                                    │
│            │                                                             │
│            ├──→ sources.yml (generate_source)                           │
│            ├──→ stg_*.sql   (generate_base_model)                       │
│            └──→ schema.yml  (generate_model_yaml)                       │
│                                                                          │
│            ▼                                                             │
│  4. ENRICH                                                               │
│     ┌──────────────┐                                                    │
│     │  LLM +       │                                                    │
│     │  Human       │                                                    │
│     └──────┬───────┘                                                    │
│            │                                                             │
│            ├──→ Column descriptions                                     │
│            ├──→ semantic_type meta tags                                 │
│            ├──→ data_tests (unique, not_null, relationships)            │
│            ├──→ kpi flags and alert_thresholds                          │
│            └──→ Owner, tier, domain metadata                            │
│                                                                          │
│            ▼                                                             │
│  5. VALIDATE                                                             │
│     ┌──────────────┐                                                    │
│     │  dbt build   │                                                    │
│     │  dbt test    │                                                    │
│     └──────────────┘                                                    │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Quick Reference Commands

```bash
# Install dependencies
dbt deps

# Generate sources for a schema
dbt run-operation generate_source \
  --args '{
    "schema_name": "raw",
    "database_name": "your_catalog",
    "generate_columns": true,
    "include_descriptions": true,
    "include_data_types": true
  }'

# Generate base model for a table
dbt run-operation generate_base_model \
  --args '{"source_name": "raw_data", "table_name": "customers"}'

# Generate schema YAML for models
dbt run-operation generate_model_yaml \
  --args '{"model_names": ["stg_customers", "stg_orders"]}'

# Compile and validate
dbt compile

# Run tests
dbt test --select staging

# Generate documentation
dbt docs generate
dbt docs serve
```

---

## Semantic Type Reference

| semantic_type | Usage | Example |
|---------------|-------|---------|
| `primary_key` | Unique row identifier | `customer_id`, `order_id` |
| `foreign_key` | Reference to another table | `customer_id` in orders |
| `dimension` | Categorical/grouping field | `status`, `category`, `country` |
| `measure` | Numeric aggregatable field | `revenue_usd`, `quantity` |
| `timestamp` | Date/time field | `created_at`, `updated_at` |
| `date` | Date field (no time) | `order_date`, `birth_date` |

## Meta Tags Reference

| Meta Tag | Purpose | Example Values |
|----------|---------|----------------|
| `aggregation` | How to aggregate measures | `sum`, `average`, `count`, `min`, `max` |
| `unit` | Unit of measurement | `usd`, `percent`, `count`, `days` |
| `kpi` | Mark as key metric | `true` |
| `alert_threshold_high` | Alert if value exceeds | `10` (for churn > 10%) |
| `alert_threshold_low` | Alert if value below | `90` (for success rate < 90%) |
| `owner` | Responsible team | `data-platform`, `analytics` |
| `tier` | Data quality tier | `bronze`, `silver`, `gold` |
| `domain` | Business domain | `finance`, `marketing`, `product` |

---

## Next Steps

1. Run metadata extraction on your Databricks tables
2. Generate initial dbt scaffolding with codegen
3. Use LLM to enrich with descriptions and semantic types
4. Review and validate with `dbt build`
5. Iterate on documentation and tests
