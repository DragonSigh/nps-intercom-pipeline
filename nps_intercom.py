import apache_beam as beam
import typing
import argparse
import logging
import requests
import time
import io
import gzip
import zipfile
import re
import os
import json
from datetime import datetime, date, timedelta

import csv
import math
import google.auth
from google.oauth2 import service_account
from googleapiclient.discovery import build

from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery


# Размер батча для Google Sheets
BATCH_SIZE = 50
MAX_RETRIES = 3

# NPS survey configuration (остается той же)
NPS_SURVEYS = {
    'ENG': {
        'series_name': 'Measure NPS® and follow-up_ENG',
        'survey_name_pattern': r'Q\d+ NPS Survey$',
        'nps_question': 'Thank you for choosing HEALBE! How likely is it that you would recommend HEALBE GoBe to a friend or colleague?',
        'country_code': 'US'
    },
    'RU': {
        'series_name': 'Measure NPS® and follow-up_RU',
        'survey_name_pattern': r'Q\d+ NPS Survey RU$',
        'nps_question': 'Спасибо за ваш выбор HEALBE! Пожалуйста, оцените вероятность того, что вы порекомендуете наш продукт друзьям и знакомым',
        'country_code': 'RU'
    },
    'JP': {
        'series_name': 'Measure NPS® and follow-up_JP',
        'survey_name_pattern': r'Q\d+ NPS Survey JP$',
        'nps_question': 'GoBeデバイスを利用いただき、ありがとうございます。友人やご家族のメンバーにGoBeをお勧めしますか？',
        'country_code': 'JP'
    }
}

# BigQuery schema definition
NPS_SCHEMA = {
    'fields': [
        {'name': 'month', 'type': 'STRING'},
        {'name': 'country', 'type': 'STRING'},
        {'name': 'total', 'type': 'INTEGER'},
        {'name': 'promoters', 'type': 'INTEGER'},
        {'name': 'neutral', 'type': 'INTEGER'},
        {'name': 'detractors', 'type': 'INTEGER'}
    ]
}


def date_to_unix(date_str):
    """Converts a YYYY-MM-DD date string to Unix timestamp."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp())


def calculate_period_dates(period_type='monthly', period_offset=1):
    """Calculates the start and end dates for the specified period."""
    today = date.today()
    
    if period_type == 'monthly':
        current_month = today.replace(day=1)
        for _ in range(period_offset):
            current_month = (current_month - timedelta(days=1)).replace(day=1)
        
        start_date = current_month
        if current_month.month == 12:
            end_date = current_month.replace(year=current_month.year + 1, month=1)
        else:
            end_date = current_month.replace(month=current_month.month + 1)
            
    elif period_type == 'quarterly':
        current_quarter = ((today.month - 1) // 3) + 1
        current_year = today.year
        
        for _ in range(period_offset):
            current_quarter -= 1
            if current_quarter < 1:
                current_quarter = 4
                current_year -= 1
        
        start_month = (current_quarter - 1) * 3 + 1
        start_date = date(current_year, start_month, 1)
        
        end_month = start_month + 2
        if end_month > 12:
            end_date = date(current_year + 1, end_month - 12 + 1, 1)
        else:
            end_date = date(current_year, end_month + 1, 1)
    
    else:
        raise ValueError(f"Unsupported period_type: {period_type}")
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    logging.info(f"Requesting data for {period_type} period: from {start_date_str} to {end_date_str}")
    return start_date_str, end_date_str


def parse_datetime(value):
    """Parse a variety of timestamp representations without pandas.

    Supported inputs:
        * Unix epoch seconds or milliseconds (int / float)
        * ISO-8601 strings with or without fractional seconds / trailing "Z"

    Returns a ``datetime`` instance or ``None`` if parsing fails.
    """
    # --- Missing values -----------------------------------------------------
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None

    # --- Numeric epoch ------------------------------------------------------
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        ts = float(value)
        # Milliseconds → seconds heuristic
        if ts > 1e12:
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts)
        except Exception:
            pass

    # --- ISO-8601 strings ---------------------------------------------------
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None

        # Fast path using stdlib (Python ≥3.11)
        try:
            return datetime.fromisoformat(value.rstrip("Z"))
        except Exception:
            pass

        # Fallback: manual regex-based parse for common shapes
        iso_regex = r'^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(\.\d+)?'
        m = re.match(iso_regex, value.rstrip("Z"))
        if m:
            fmt = "%Y-%m-%d %H:%M:%S" + (".%f" if m.group(3) else "")
            try:
                return datetime.strptime(value[:len(m.group(0))], fmt)
            except Exception:
                pass

    # Could not parse
    return None


class IntercomService:
    """Service wrapper for Intercom API interactions with improved error handling."""
    
    def __init__(self, api_key):
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Intercom-Version": "2.7"
        }
        self.base_url = "https://api.intercom.io"

    def _make_request_with_retry(self, method, url, **kwargs):
        """Makes HTTP request with retry logic."""
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.request(method, url, timeout=120, **kwargs)
                if response.status_code == 429:  # Rate limit
                    wait_time = int(response.headers.get('Retry-After', 60))
                    logging.warning(f"Rate limited, waiting {wait_time} seconds")
                    time.sleep(wait_time)
                    continue
                return response
            except requests.exceptions.RequestException as e:
                logging.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(2 ** attempt)

    def create_export_job(self, start_date, end_date):
        """Creates an export job with retry logic."""
        endpoint = f"{self.base_url}/export/content/data"
        payload = {
            "created_at_after": date_to_unix(start_date),
            "created_at_before": date_to_unix(end_date)
        }
        
        response = self._make_request_with_retry('POST', endpoint, headers=self.headers, json=payload)
        
        if response.status_code == 200:
            result = response.json()
            if 'job_identifier' in result:
                return result['job_identifier']
            logging.error("No 'job_identifier' found in the response.")
        else:
            logging.error(f"HTTP error {response.status_code}: {response.text}")
        return None

    def check_export_status(self, job_id):
        """Checks the status of an export job with retry."""
        endpoint = f"{self.base_url}/export/content/data/{job_id}"
        response = self._make_request_with_retry('GET', endpoint, headers=self.headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Error while checking job status: {response.status_code}")
            return None

    def download_export_results(self, download_url):
        """Download Intercom export and return a list[dict] of rows parsed from CSV.

        Supports GZIP-compressed CSV, ZIP archives with one or more CSVs and
        plain CSV payloads. Uses only the Python standard library, avoiding
        pandas to stay compatible with Apache Beam environments.
        """
        download_headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/octet-stream"
        }

        response = self._make_request_with_retry('GET', download_url, headers=download_headers)
        if response.status_code != 200:
            logging.error(f"Error while downloading results: {response.status_code}")
            return None

        content = response.content

        def _rows_from_csv(raw: bytes):
            text_stream = io.StringIO(raw.decode('utf-8', errors='replace'))
            return list(csv.DictReader(text_stream))

        try:
            # GZIP (.gz)
            if content[:2] == b'\x1f\x8b':
                with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
                    return _rows_from_csv(gz.read())

            # ZIP (.zip)
            if content[:2] == b'PK':
                rows = []
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    for fname in zf.namelist():
                        if fname.endswith('.csv'):
                            with zf.open(fname) as f:
                                rows.extend(_rows_from_csv(f.read()))
                if not rows:
                    logging.error("No CSV files found inside the ZIP archive.")
                    return None
                return rows

            # Plain CSV
            return _rows_from_csv(content)
        except Exception as e:
            logging.error(f"Error processing file content: {e}")
            return None

    def get_nps_data(self, start_date, end_date):
        """Fetches NPS data from Intercom - SINGLE REQUEST FOR ALL DATA."""
        logging.info(f"Creating export job for data from {start_date} to {end_date}...")

        job_id = self.create_export_job(start_date, end_date)
        if not job_id:
            logging.error("Failed to create export job.")
            return None

        logging.info(f"Export job created. ID: {job_id}")

        # Wait for job completion
        max_attempts = 60
        for attempt in range(max_attempts):
            status_info = self.check_export_status(job_id)
            if not status_info:
                logging.error("Export job not found or was deleted")
                return None

            state = status_info.get('status', 'unknown')
            logging.info(f"Status: {state.upper()}")

            if state == 'completed':
                download_url = status_info.get('download_url')
                if not download_url:
                    logging.error("Error: 'download_url' is missing in the response")
                    return None

                logging.info("Downloading and processing data...")
                rows = self.download_export_results(download_url)
                
                if rows is not None:
                    logging.info(f"Loaded {len(rows)} rows")
                    return rows
                else:
                    logging.error("Failed to load data")
                    return None
            
            elif state in ('failed', 'no_data'):
                logging.error(f"Export failed: {status_info.get('message', 'unspecified')}")
                return None
            
            time.sleep(20)
        
        logging.error(f"Exceeded maximum attempts ({max_attempts}) to check export status")
        return None


class FetchIntercomData(beam.DoFn):
    """DoFn that fetches raw Intercom data - ORIGINAL LOGIC WITH IMPROVEMENTS."""
    
    def __init__(self, api_key, start_date, end_date):
        self.api_key = api_key
        self.start_date = start_date
        self.end_date = end_date
        
    def process(self, _):
        intercom_service = IntercomService(self.api_key)
        data = intercom_service.get_nps_data(self.start_date, self.end_date)
        
        if not data:
            logging.warning("Failed to retrieve data from Intercom or dataset is empty")
            return

        logging.info(f"Loaded columns: {list(data[0].keys()) if isinstance(data, list) else 'unknown'}")

        # Process data for each language configuration
        for lang_code, config in NPS_SURVEYS.items():
            logging.info(f"Processing data for language: {lang_code}")

            # Filter by series name if present
            if any('series_name' in row for row in data):
                series_data = [r for r in data if r.get('series_name', '') == config['series_name']]
            else:
                series_data = data

            if not series_data:
                logging.warning(f"No data found for series: {config['series_name']}")
                continue

            # Filter by survey name regex if present
            pattern = re.compile(config['survey_name_pattern'])
            if any('survey_name' in row for row in series_data):
                nps_surveys = [r for r in series_data if pattern.match(str(r.get('survey_name', '')))]
            else:
                nps_surveys = series_data

            if not nps_surveys:
                logging.warning(f"No NPS surveys found for pattern: {config['survey_name_pattern']}")
                continue

            logging.info(f"Found {len(nps_surveys)} NPS survey responses for {lang_code}")

            results = []
            for row in nps_surveys:
                # Extract score ------------------------------------------------
                score = None
                # Possible columns containing the numeric NPS score
                score_columns = ['answer', 'rating', 'score', 'response']
                # Add the exact survey question column defined in config (if any)
                if config.get('nps_question'):
                    score_columns.append(config['nps_question'])

                for col in score_columns:
                    val = row.get(col)
                    if val in (None, '', 'NaN'):
                        continue
                    if isinstance(val, str):
                        val_str = val.strip()
                        if val_str.isdigit():
                            score = int(val_str)
                        else:
                            m = re.search(r'\d+', val_str)
                            if m:
                                score = int(m.group())
                    elif isinstance(val, (int, float)):
                        score = int(val)
                    if score is not None:
                        break

                # Extract feedback ------------------------------------------
                feedback = None
                for col in ['body', 'message', 'feedback', 'comment', 'follow_up']:
                    val = row.get(col)
                    if isinstance(val, str) and val.strip():
                        feedback = val.strip()
                        break

                # User info ---------------------------------------------------
                user_id = row.get('user_id') or row.get('user_external_id') or row.get('contact_id')
                user_name = row.get('name') or row.get('user_name', '')
                user_email = row.get('email') or row.get('user_email', '')

                # Datetime ----------------------------------------------------
                # Try multiple timestamp columns – Intercom export uses "received_at" for message receipts
                raw_created_at = (
                    row.get('created_at')
                    or row.get('received_at')
                    or row.get('updated_at')
                    or row.get('submitted_at')
                )
                created_dt = parse_datetime(raw_created_at)
                if created_dt is None and raw_created_at is not None:
                    logging.warning(f"Could not parse date '{raw_created_at}' – falling back to current time")
                created_at = created_dt or datetime.now()

                # Compose result ---------------------------------------------
                if score is not None or (feedback and feedback.strip()):
                    results.append({
                        'user_uuid': str(user_id or ''),
                        'user_name': str(user_name or ''),
                        'user_email': str(user_email or ''),
                        'country': config['country_code'],
                        'date': created_at,
                        'score': score,
                        'review': feedback
                    })

            logging.info(f"Processed {len(results)} records for {lang_code}")

            if results:
                yield {
                    'region': config['country_code'],
                    'results': results
                }


class CalculateNPS(beam.DoFn):
    """DoFn that calculates the NPS metric and formats reviews - ORIGINAL LOGIC."""
    
    def __init__(self, period_type='monthly', run_period=None):
        self.period_type = period_type
        self.run_period = run_period
        
    def process(self, element):
        region = element['region']
        results = element['results']
        
        if not results:
            logging.warning(f"No data to process for region {region}")
            return
        
        logging.info(f"Received {len(results)} results to process for region {region}")
        
        # Group data by period for NPS calculation
        period_data = {}
        reviews = []
        
        for result in results:
            date_obj = result.get('date')
            score = result.get('score')
            
            # Determine period string for NPS grouping
            if self.period_type == 'monthly':
                period_str = date_obj.strftime('%Y-%m')
            elif self.period_type == 'quarterly':
                quarter = (date_obj.month - 1) // 3 + 1
                period_str = f"{date_obj.year}-Q{quarter}"
            else:
                period_str = date_obj.strftime('%Y-%m')
            
            # Filter by specific period if provided
            if self.run_period and period_str != self.run_period:
                logging.debug(f"Skipping data for period {period_str}, expected: {self.run_period}")
                continue
            
            # Add data for NPS calculation (only if there is a score)
            if score is not None:
                if period_str not in period_data:
                    period_data[period_str] = {
                        'scores': [],
                        'country': result.get('country')
                    }
                period_data[period_str]['scores'].append(score)
            
            # Add review (even if text is empty, but there is a score)
            if result.get('review') or score is not None:
                year = date_obj.year
                quarter = (date_obj.month - 1) // 3 + 1
                quarter_str = f"{year} Q{quarter}"
                
                review_data = beam.Row(
                    user_uuid=str(result.get('user_uuid', '')),
                    user_name=str(result.get('user_name', '')),
                    user_email=str(result.get('user_email', '')),
                    country=result.get('country', ''),
                    date=date_obj.strftime('%Y-%m-%d'),
                    quarter=quarter_str,
                    score=int(score) if score is not None else 0,
                    review=str(result.get('review', ''))
                )
                reviews.append(review_data)
        
        # Calculate NPS for each period
        nps_results = []
        for period, data in period_data.items():
            scores = data['scores']
            country = data['country']
            
            if not scores:
                continue
                
            promoters = sum(1 for s in scores if s >= 9)
            detractors = sum(1 for s in scores if s <= 6)
            total = len(scores)
            neutral = total - promoters - detractors
            
            nps_results.append(beam.Row(
                month=period,
                country=country,
                total=total,
                promoters=promoters,
                neutral=neutral,
                detractors=detractors
            ))
        
        logging.info(f"Calculated NPS for {len(nps_results)} periods in region {region}")
        logging.info(f"Found {len(reviews)} reviews in region {region}")
        
        # Return NPS results and reviews
        for nps_result in nps_results:
            yield ('nps', nps_result)
        
        for review in reviews:
            yield ('review', review)


def write_to_sheets(reviews, spreadsheet_id, spreadsheet_name='NPS Data', credentials_path=None):
    """Writes reviews data to Google Sheets with improved error handling."""
    
    if not reviews:
        logging.warning("Empty reviews list – nothing to write to Google Sheets")
        return None
        
    logging.info(f"Starting to write {len(reviews)} reviews to Google Sheets (ID: {spreadsheet_id})")
    
    # Authenticate
    try:
        if credentials_path:
            logging.info(f"Using service-account credentials from file: {credentials_path}")
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path, 
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
        else:
            logging.info("Using Application Default Credentials")
            credentials, _ = google.auth.default(
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
        
        service = build('sheets', 'v4', credentials=credentials)
    except Exception as e:
        logging.error(f"Failed to authenticate with Google Sheets: {e}")
        return None
    
    try:
        # Get list of existing sheets
        sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = sheet_metadata.get('sheets', '')
        
        # Check if the "NPS Reviews" sheet already exists
        reviews_sheet_id = None
        for sheet in sheets:
            if sheet['properties']['title'] == 'NPS Reviews':
                reviews_sheet_id = sheet['properties']['sheetId']
                break
        
        # Create the sheet if it does not exist
        if not reviews_sheet_id:
            logging.info("'NPS Reviews' sheet not found, creating a new one")
            body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': 'NPS Reviews',
                            'gridProperties': {
                                'rowCount': 2000,
                                'columnCount': 8
                            }
                        }
                    }
                }]
            }
            response = service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()
            
            # Add headers
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range='NPS Reviews!A1:H1',
                valueInputOption='RAW',
                body={
                    'values': [['user_uuid', 'user_name', 'user_email', 'country', 'date', 'quarter', 'score', 'review']]
                }
            ).execute()
            logging.info("Created a new sheet and added headers")
        else:
            logging.info("Found existing sheet 'NPS Reviews'")
        
        # Get the number of existing rows
        response = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range='NPS Reviews!A:A'
        ).execute()
        values = response.get('values', [])
        next_row = len(values) + 1
        logging.info(f"Detected {next_row-1} existing rows, starting to write from row {next_row}")
        
        # Format data for upload
        review_values = []
        for review in reviews:
            if hasattr(review, '_asdict'):
                review_dict = review._asdict()
            else:
                review_dict = review
                
            review_values.append([
                review_dict.get('user_uuid', ''),
                review_dict.get('user_name', ''),
                review_dict.get('user_email', ''),
                review_dict.get('country', ''),
                review_dict.get('date', ''),
                review_dict.get('quarter', ''),
                str(review_dict.get('score', 0)),
                review_dict.get('review', '')
            ])
        
        # Write data in batches
        total_written = 0
        results = []
        
        for i in range(0, len(review_values), BATCH_SIZE):
            batch = review_values[i:i + BATCH_SIZE]
            current_row = next_row + i
            
            logging.info(f"Writing batch {i//BATCH_SIZE + 1} of {len(batch)} reviews starting from row {current_row}")
            
            body = {'values': batch}
            try:
                result = service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f'NPS Reviews!A{current_row}',
                    valueInputOption='RAW',
                    body=body
                ).execute()
                
                updated_rows = result.get('updatedRows', 0)
                total_written += updated_rows
                results.append(result)
                
                logging.info(f"Successfully wrote {updated_rows} rows for batch {i//BATCH_SIZE + 1}")
            except Exception as e:
                logging.error(f"Error while writing batch {i//BATCH_SIZE + 1}: {str(e)}")
        
        logging.info(f"Total of {total_written} reviews written to Google Sheets out of {len(review_values)} passed")
        return results
        
    except Exception as e:
        logging.error(f"Error in Google Sheets operations: {e}")
        return None


class WriteReviewsToSheets(beam.DoFn):
    """DoFn that writes reviews into Google Sheets with batching."""
    
    def __init__(self, spreadsheet_id, credentials_path=None):
        self.spreadsheet_id = spreadsheet_id
        self.credentials_path = credentials_path

    def process(self, _, reviews_as_list):
        if not reviews_as_list:
            logging.warning("Empty reviews list – nothing to write")
            return
            
        try:
            logging.info(f"{len(reviews_as_list)} reviews were passed to the Google Sheets write function")
            
            results = write_to_sheets(
                reviews_as_list, 
                self.spreadsheet_id, 
                credentials_path=self.credentials_path
            )
            
            if results:
                total_written = sum(result.get('updatedRows', 0) for result in results)
                logging.info(f"Total of {total_written} reviews written to Google Sheets out of {len(reviews_as_list)} passed")
                yield {'status': 'success', 'written_reviews': total_written}
            else:
                logging.warning("Write to Google Sheets skipped, empty result")
                yield {'status': 'no_data', 'written_reviews': 0}
                
        except Exception as e:
            error_message = str(e)
            logging.error(f"Error writing to Google Sheets: {error_message}")
            yield {'status': 'error', 'error': error_message}


def run(argv=None):
    """Entry point for the NPS Dataflow pipeline."""
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--api_key', required=True, help='Intercom API key')
    parser.add_argument('--target_table', required=True, help='BigQuery table for NPS data (project:dataset.table)')
    parser.add_argument('--spreadsheet_id', required=True, help='Google Spreadsheet ID for reviews')
    
    parser.add_argument('--period_type', default='monthly', choices=['monthly', 'quarterly'], 
                       help='Collection period type: monthly or quarterly')
    parser.add_argument('--period_offset', type=int, default=1, 
                       help='How many periods back to collect (1 = previous period)')
    
    parser.add_argument('--credentials_path', help='Path to service account credentials file for Google Sheets')
    parser.add_argument('--debug', type=str, default='false', choices=['true', 'false'], 
                       help='Enable debug mode (no period filtering)')
    
    parser.add_argument('--start_date', help='Override start date in format YYYY-MM-DD')
    parser.add_argument('--end_date', help='Override end date in format YYYY-MM-DD')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    debug_mode = known_args.debug.lower() == 'true'
    
    if known_args.start_date and known_args.end_date:
        start_date = known_args.start_date
        end_date = known_args.end_date
        logging.info(f"Using provided dates: from {start_date} to {end_date}")
    else:
        start_date, end_date = calculate_period_dates(
            known_args.period_type, 
            known_args.period_offset
        )
    
    run_period = None
    if not debug_mode:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        if known_args.period_type == 'monthly':
            run_period = start_dt.strftime('%Y-%m')
        elif known_args.period_type == 'quarterly':
            quarter = (start_dt.month - 1) // 3 + 1
            run_period = f"{start_dt.year}-Q{quarter}"
    
    logging.info(f"Starting NPS pipeline for period: {run_period or 'debug mode'}")

    with beam.Pipeline(options=pipeline_options) as p:
        # Create a dummy input element to kick off the pipeline
        dummy_input = p | "Create Dummy Input" >> beam.Create([None])
        
        # Fetch data from Intercom - SINGLE REQUEST
        intercom_data = dummy_input | "Fetch Intercom Data" >> beam.ParDo(
            FetchIntercomData(
                known_args.api_key,
                start_date,
                end_date
            )
        )
        
        # Calculate NPS and format reviews
        nps_and_reviews = intercom_data | "Calculate NPS" >> beam.ParDo(
            CalculateNPS(
                period_type=known_args.period_type,
                run_period=run_period
            )
        )
        
        # Split NPS aggregates and review rows
        nps_data, reviews_data = nps_and_reviews | "Split NPS and Reviews" >> beam.Partition(
            lambda elem, _: 0 if elem[0] == 'nps' else 1, 2
        )
        
        # Format NPS data for BigQuery
        nps_for_bq = nps_data | "Format NPS for BigQuery" >> beam.Map(
            lambda elem: elem[1]._asdict() if hasattr(elem[1], '_asdict') else elem[1]
        )
        
        # Write NPS data to BigQuery
        nps_for_bq | "Write NPS to BigQuery" >> WriteToBigQuery(
            known_args.target_table,
            schema=NPS_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
        
        # Format reviews for Google Sheets
        reviews_for_sheets = reviews_data | "Format Reviews for Sheets" >> beam.Map(lambda x: x[1])
        
        # Collect all reviews into a single list
        reviews_as_list = reviews_for_sheets | "Collect Reviews" >> beam.combiners.ToList()
        
        # Write reviews to Google Sheets
        sheets_result = dummy_input | "Write Reviews to Sheets" >> beam.ParDo(
            WriteReviewsToSheets(
                known_args.spreadsheet_id, 
                known_args.credentials_path
            ),
            reviews_as_list=beam.pvalue.AsSingleton(reviews_as_list)
        )
        
        # Log the outcome
        sheets_result | "Log Sheets Result" >> beam.Map(
            lambda result: logging.info(f"Sheets write result: {result}")
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()