# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an **Intercom NPS → BigQuery & Google Sheets Dataflow Flex Template** that exports Net Promoter Score (NPS) survey responses from Intercom and processes them into structured analytics data. The pipeline:

- Fetches NPS survey data from Intercom API using export jobs
- Calculates NPS metrics (promoters, detractors, neutral) by month/quarter and country
- Writes aggregated data to BigQuery
- Exports raw reviews to Google Sheets for qualitative analysis

**Key Architecture**: Single Apache Beam pipeline using Dataflow Flex Template, processing data for multiple languages (ENG/US, RU, JP) with country-specific survey configurations.

## Development Commands

### Local Development Setup
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# Optional dev tools:
pip install ruff black pytest
```

### Testing and Code Quality
```bash
# Run tests
python -m pytest -q

# Lint code
ruff nps_intercom.py
black --check nps_intercom.py

# Or use Makefile
make test
make lint
```

### Local Pipeline Execution
```bash
# Basic run (previous month)
python nps_intercom.py \
  --api_key=YOUR_INTERCOM_API_KEY \
  --target_table=project:dataset.nps \
  --spreadsheet_id=YOUR_SHEET_ID

# Custom date range
python nps_intercom.py \
  --api_key=YOUR_INTERCOM_API_KEY \
  --target_table=project:dataset.nps \
  --spreadsheet_id=YOUR_SHEET_ID \
  --start_date=2024-01-01 \
  --end_date=2024-02-01

# Debug mode (no date filtering)
python nps_intercom.py \
  --api_key=YOUR_INTERCOM_API_KEY \
  --target_table=project:dataset.nps \
  --spreadsheet_id=YOUR_SHEET_ID \
  --debug=true
```

### Deployment (Docker + GCP)
```bash
# Build, push, and deploy Flex Template
make build push template

# Individual steps:
make build    # Build Docker image
make push     # Push to GCR
make template # Create Dataflow Flex Template
```

## Core Architecture

### Pipeline Structure (nps_intercom.py)
- **FetchIntercomData**: Single DoFn that creates Intercom export job, polls for completion, downloads and parses CSV data
- **CalculateNPS**: Processes raw survey responses, calculates NPS metrics by period/country, formats reviews
- **WriteReviewsToSheets**: Batch writes review data to Google Sheets with error handling

### Data Flow
1. **Single API Request**: Uses Intercom export API (not REST pagination) to fetch all data in date range
2. **Multi-language Processing**: Processes ENG, RU, JP surveys with different question patterns and country mappings
3. **Dual Output**: NPS aggregates → BigQuery, Raw reviews → Google Sheets
4. **Period Filtering**: Supports monthly/quarterly grouping with configurable offsets

### Key Configuration (NPS_SURVEYS)
```python
NPS_SURVEYS = {
    'ENG': {
        'series_name': 'Measure NPS® and follow-up_ENG',
        'survey_name_pattern': r'Q\d+ NPS Survey$',
        'nps_question': 'Thank you for choosing HEALBE!...',
        'country_code': 'US'
    },
    # RU and JP configurations
}
```

### Dependencies
- **Apache Beam 2.66.0** with GCP extensions for Dataflow
- **Google APIs**: BigQuery, Sheets, Auth
- **Requests**: Intercom API communication
- **No pandas**: Uses stdlib csv module for Apache Beam compatibility

## Important Implementation Details

### Date/Time Parsing
- Custom `parse_datetime()` function handles various timestamp formats (Unix epochs, ISO-8601) without pandas
- Uses `received_at` timestamp from Intercom for message receipt times
- Supports fractional seconds and timezone handling

### Error Handling & Resilience
- Retry logic for Intercom API calls with exponential backoff
- Rate limiting handling (429 responses)
- Export job polling with timeout protection
- Batch processing for Google Sheets with partial failure recovery

### Data Processing
- Score extraction from multiple possible columns (`answer`, `rating`, `score`, `response`)
- NPS calculation: Promoters (9-10), Neutral (7-8), Detractors (0-6)
- Review text extraction from various feedback columns

### Google Sheets Integration
- Auto-creates "NPS Reviews" sheet if not exists
- Appends to existing data (finds next empty row)
- Batch writes (50 rows per batch) with progress logging

## Configuration Files

- **Dockerfile**: Multi-stage build for Dataflow Flex Template
- **metadata.json**: Template parameter definitions and validation
- **Makefile**: Build, push, template deployment automation
- **setup.py**: Python package configuration for Dataflow workers