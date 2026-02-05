# Troubleshooting Issues

## Issue 1: dbt Package Dependency Error

**Error:**
```
No dbt_project.yml found at expected path /dbt-workspace/dbt-projects/finance_analytics/dbt_packages/dbt_utils/dbt_project.yml
```

**Reason:**
Corrupted or incomplete package installation in `dbt_packages/` directory.

**Fix:**
```bash
cd /dbt-workspace/dbt-projects/finance_analytics

# Remove corrupted packages
rm -rf dbt_packages/
rm -f package-lock.yml

# Reinstall packages
dbt deps --profiles-dir .
```

---

## Issue 2: Schema Name Concatenation (STAGING_staging)

**Error:**
```
Creating schema "FINANCE_DB.STAGING_staging"
Insufficient privileges to operate on database 'FINANCE_DB'
```

**Reason:**
dbt concatenates `profiles.yml` schema with `dbt_project.yml` `+schema` config:
```
profiles.yml schema: STAGING
+ dbt_project.yml +schema: staging
= Result: STAGING_staging ❌
```

**Fix:**
Create `macros/generate_schema_name.sql`:
```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

Update `profiles.yml`:
```yaml
schema: dbt_dev  # Generic default
```

Keep `dbt_project.yml`:
```yaml
staging:
  +schema: staging  # Creates FINANCE_DB.STAGING ✅
```

Then run:
```bash
dbt clean --profiles-dir .
dbt run --select stg_investors --profiles-dir .
```