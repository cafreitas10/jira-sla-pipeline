# JIRA SLA Data Engineering Pipeline

## Overview

Python data engineering pipeline following the **Medallion Architecture** (Bronze → Silver → Gold) 
to process JIRA issues data, calculate SLA compliance metrics, and generate analytical reports.

## 📋 Pipeline Stages

### Bronze Layer
- **Purpose**: Ingest raw data from Azure Blob Storage
- **Input**: Azure Blob Storage (Service Principal authentication)
- **Output**: `data/bronze/bronze_issues.json` (raw JSON)
- **Processing**: None (raw data only)

### Silver Layer
- **Purpose**: Parse, flatten, and calculate business metrics
- **Input**: `data/bronze/bronze_issues.json`
- **Output**: `data/silver/silver_issues.parquet`
- **Processing**:
  - Parse nested JSON structures (issues, assignees, timestamps)
  - Flatten assignee and timestamp arrays
  - Calculate resolution hours
  - Map SLA expected hours by priority

### Gold Layer
- **Purpose**: Build analytical reports and aggregations
- **Input**: `data/silver/silver_issues.parquet`
- **Outputs**:
  - `data/gold/gold_sla_issues.csv` - Individual issue SLA compliance
  - `data/gold/gold_sla_by_analyst.csv` - Aggregated by analyst
  - `data/gold/gold_sla_by_issue_type.csv` - Aggregated by issue type

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- pip or conda

### Installation

1. **Clone repository**
   ```bash
   git clone <repository-url>
   cd jira-sla-pipeline
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   ```bash
   cp .env.example .env
   ```

   Edit `.env` with your Azure credentials:
   ```
   ACCOUNT_URL=https://stfasttracksdev.blob.core.windows.net
   CONTAINER_NAME=source-jira
   BLOB_NAME=jira_issues_raw.json
   AZURE_TENANT_ID=<your-tenant-id>
   AZURE_CLIENT_ID=<your-client-id>
   AZURE_CLIENT_SECRET=<your-client-secret>
   ```

### Running the Pipeline

```bash
python src/sla_calculation.py
```

Pipeline will execute all three stages:
1. Bronze: Download from Azure
2. Silver: Parse and transform
3. Gold: Build reports

## 📊 Output Data

### Bronze Issues Schema
```json
{
  "issues": [
    {
      "id": "JIRA-1234",
      "issue_type": "Bug",
      "status": "Done",
      "priority": "High",
      "assignee": [{"id": "A1", "name": "John", "email": "john@..."}],
      "timestamps": [{"created_at": "2025-01-10T08:30:00Z", "resolved_at": "2025-01-11T..."}]
    }
  ]
}
```

### Silver Issues Schema
| Column | Type | Example |
|--------|------|---------|
| issue_id | string | JIRA-1234 |
| issue_type | string | Bug |
| status | string | Done |
| priority | string | High |
| assignee_id | string | A1 |
| assignee_name | string | John Doe |
| assignee_email | string | john@example.com |
| created_at | datetime | 2025-01-10 08:30:00 |
| resolved_at | datetime | 2025-01-11 12:45:00 |
| resolution_hours | float | 28.25 |
| sla_expected_hours | int | 8 |

### Gold Reports

**gold_sla_issues.csv**: Individual issue compliance
**gold_sla_by_analyst.csv**: Analyst performance metrics
**gold_sla_by_issue_type.csv**: Issue type performance metrics

## 🔐 Security

- **Service Principal Authentication**: Uses Azure ClientSecretCredential
- **Environment Variables**: Credentials stored in `.env` (not versioned)
- **Read-Only Access**: Only reads from blob storage, no write operations

## 📝 Naming Conventions

- **Python Files**: `snake_case` (e.g., `ingest_bronze.py`)
- **Functions**: Verb-based, descriptive (e.g., `read_bronze_issues()`)
- **Variables**: `snake_case` (e.g., `resolution_hours`)
- **Dates**: ISO 8601 format (e.g., `2025-01-10T08:30:00Z`)
- **Boolean Columns**: Start with `is_` (e.g., `is_sla_met`)

See `convencao.md` for full naming conventions.

## 📦 Dependencies

- **pandas**: Data manipulation
- **pyarrow**: Parquet serialization
- **python-dotenv**: Environment variable management
- **azure-identity**: Service Principal authentication
- **azure-storage-blob**: Azure Blob Storage client

## 🛠️ Development

### Code Style
- PEP 8 compliance
- 4-space indentation
- English code and comments
- Type hints recommended

### Adding New Transformations

1. Create function in appropriate layer file
2. Follow naming convention: verb-based names
3. Include docstring with Args/Returns
4. Add logging for debugging
5. Update main() to call new function

## 📋 Troubleshooting

**Azure Authentication Error**
- Verify Service Principal credentials in `.env`
- Check Azure Tenant ID, Client ID, and Client Secret

**Blob Not Found**
- Confirm ACCOUNT_URL, CONTAINER_NAME, and BLOB_NAME
- Verify Service Principal has read access

**Parquet Read Error**
- Ensure silver layer was successfully created
- Check data/silver/ directory exists

## 📄 License

[Specify license]

## 👥 Contributors

[List contributors]
