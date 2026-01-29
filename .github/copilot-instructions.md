# Copilot Instructions - JIRA SLA Pipeline

## Project Overview

**JIRA SLA Data Engineering Pipeline** - A Python medallion architecture (Bronze → Silver → Gold) that processes JIRA issues data from Azure Blob Storage and generates SLA compliance analytics.

### Key Architecture
- **Bronze Layer** (`src/bronze/ingest_bronze.py`): Raw data ingestion from Azure Blob Storage via Service Principal authentication
- **Silver Layer** (`src/silver/transform_silver.py`): JSON parsing, nested structure flattening, SLA metric calculation
- **Gold Layer** (`src/gold/build_gold.py`): Aggregated analytics reports for compliance analysis
- **Orchestrator** (`src/sla_calculation.py`): Sequential pipeline execution with early-exit on stage failure

### Data Flow
```
Azure Blob Storage → Bronze (raw JSON) → Silver (Parquet) → Gold (CSV reports)
```

## Critical Patterns & Conventions

### Naming Conventions (from `convencao.md`)
- **Files**: `snake_case` (e.g., `transform_silver.py`, `build_gold.py`)
- **Data files**: `<layer>_<entity>.<format>` (e.g., `bronze_issues.json`, `gold_sla_by_analyst.csv`)
- **Functions**: Verb-first (e.g., `read_bronze_issues()`, `calculate_resolution_hours()`)
- **Variables**: `snake_case`, descriptive (e.g., `resolution_hours`, `sla_expected_hours`)
- **Code**: Python (PEP 8), English, 4-space indentation

### Dataframe Operations
- **Flattening**: Use `pd.json_normalize()` with `record_path` for nested arrays (assignees, timestamps)
- **Boolean columns**: Prefix with `is_` (e.g., `is_sla_met = resolution_hours <= sla_expected_hours`)
- **Aggregations**: Use `groupby().agg()` with dict specs, then flatten multi-level columns

### Module Structure
- Each layer has `main()` returning `bool` (success/failure) for pipeline orchestration
- Logging at function entry/error points with context (e.g., row counts, file paths)
- Error handling returns empty DataFrame/None with logged error, allowing pipeline to fail gracefully

### Azure Integration (`ingest_bronze.py`)
- **Authentication**: `ClientSecretCredential` with AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET from `.env`
- **Credentials validation**: Check all three vars present before creating credential object
- **Blob download pattern**: `BlobClient(account_url, container_name, blob_name, credential)` → `download_blob()` → `readall().decode('utf-8')`
- **Env loading**: Use `python-dotenv` with `load_dotenv()` at module level

## Developer Workflows

### Running the Pipeline
```bash
python src/sla_calculation.py  # Runs all three stages sequentially
```

### Key Dependencies
- `pandas` (2.2.3) - DataFrame operations and aggregations
- `pyarrow` (14.0.1) - Parquet I/O for silver/gold layers
- `azure-identity` + `azure-storage-blob` - Azure authentication and blob downloads
- `python-dotenv` - Environment variable management

### File Paths (relative to `src/`)
- **Input**: `../data/bronze/bronze_issues.json` (JSON) → `../data/silver/silver_issues.parquet` (Parquet)
- **Output**: `../data/gold/gold_sla_issues.csv`, `gold_sla_by_analyst.csv`, `gold_sla_by_issue_type.csv`

## Common Tasks

### Adding a New Metric Calculation
1. Add column calculation in `src/silver/transform_silver.py` after flattening (e.g., new resolution metric)
2. Add aggregation in relevant `build_gold_*()` function in `src/gold/build_gold.py`
3. Update CSV export with new column

### Modifying Blob Connection
Edit `src/bronze/ingest_bronze.py` - change `account_url`, `container_name`, `blob_name` environment variable names if needed

### Debugging Pipeline Failures
- Check logs: Each stage logs entry, file operations, row counts
- Pipeline exits early: First failing stage stops execution in `src/sla_calculation.py`
- Verify `.env` file for Azure credentials (all three required)

## Important Details
- JSON structure assumes `data['issues']` array with nested `assignee` and `timestamps[0]` objects
- Silver layer timestamps: Uses only first element (`[0]` index) for created/resolved times
- SLA compliance: Calculated in Silver, reported in Gold as boolean `is_sla_met`
- All stages use INFO-level logging; configure in each module's `basicConfig()`
