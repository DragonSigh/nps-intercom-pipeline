# Intercom NPS → BigQuery & Google Sheets Dataflow Flex Template

Deployable Flex-Template that exports Net Promoter Score (NPS) survey responses from Intercom, aggregates metrics and writes the results to:

- **BigQuery**: aggregated NPS per month & country
- **Google Sheets**: raw user reviews for qualitative analysis

---

## 1 Local Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# optional: install dev tools
pip install ruff black pytest
pytest
```

## 2 Usage Examples

### Default (previous calendar month)

```bash
python nps_intercom.py \
  --api_key=YOUR_INTERCOM_API_KEY \
  --target_table=project:dataset.nps \
  --spreadsheet_id=YOUR_SHEET_ID
```

### Custom date range

```bash
python nps_intercom.py \
  --api_key=YOUR_INTERCOM_API_KEY \
  --target_table=project:dataset.nps \
  --spreadsheet_id=YOUR_SHEET_ID \
  --start_date=2024-01-01T00:00:00Z \
  --end_date=2024-02-01T00:00:00Z
```

### Debug mode (process all data without month filter)

```bash
python nps_intercom.py \
  --api_key=YOUR_INTERCOM_API_KEY \
  --target_table=project:dataset.nps \
  --spreadsheet_id=YOUR_SHEET_ID \
  --debug
```

## 3 Deployment

```bash
make build push template
```

## 4 BigQuery Schema

| Field       | Type    | Description                                 |
|-------------|---------|---------------------------------------------|
| month       | STRING  | YYYY-MM                                     |
| country     | STRING  | ISO country code (US, RU, JP, …)            |
| total       | INTEGER | Total number of responses                   |
| promoters   | INTEGER | Responses with score 9-10                   |
| neutral     | INTEGER | Responses with score 7-8                    |
| detractors  | INTEGER | Responses with score 0-6                    |

---

## Intercom API specifics

1. Authentication: `Authorization: Bearer <API_KEY>`
2. Rate limit: 1 active export job per workspace — pipeline polls until completion.
3. Export job is used instead of REST pagination, which significantly reduces API calls and avoids timeouts. 