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
import ast

import pandas as pd
import google.auth
from google.oauth2 import service_account
from googleapiclient.discovery import build

from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, WorkerOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery


# Updated NPS survey configuration for series-based approach
NPS_SURVEYS = {
    'ENG': {
        'series_name': 'Measure NPS® and follow-up_ENG',
        'survey_name_pattern': r'Q\d+ NPS Survey$',  # Matches Q1 NPS Survey, Q2 NPS Survey, etc.
        'nps_question': 'Thank you for choosing HEALBE! How likely is it that you would recommend HEALBE GoBe to a friend or colleague?',
        'country_code': 'US'
    },
    'RU': {
        'series_name': 'Measure NPS® and follow-up_RU',
        'survey_name_pattern': r'Q\d+ NPS Survey RU$',  # Matches Q1 NPS Survey RU, Q2 NPS Survey RU, etc.
        'nps_question': 'Спасибо за ваш выбор HEALBE! Пожалуйста, оцените вероятность того, что вы порекомендуете наш продукт друзьям и знакомым',
        'country_code': 'RU'
    },
    'JP': {
        'series_name': 'Measure NPS® and follow-up_JP',
        'survey_name_pattern': r'Q\d+ NPS Survey JP$',  # Matches Q1 NPS Survey JP, Q2 NPS Survey JP, etc.
        'nps_question': 'GoBeデバイスを利用いただき、ありがとうございます。友人やご家族のメンバーにGoBeをお勧めしますか？',
        'country_code': 'JP'
    }
}

# BigQuery schema definition (unchanged)
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

# Typed tuples for output data (unchanged)
class NPSData(typing.NamedTuple):
    month: str
    country: str
    total: int
    promoters: int
    neutral: int
    detractors: int

class ReviewData(typing.NamedTuple):
    user_uuid: str
    user_name: str
    user_email: str
    country: str
    date: str
    quarter: str
    score: int
    review: str


def iso_to_unix(iso_str):
    """Converts an ISO8601 string into a Unix timestamp."""
    dt = datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%SZ")
    return int(dt.timestamp())


def calculate_period_dates(period_type='monthly', period_offset=1):
    """
    Calculates the start and end dates for the specified period.
    
    Args:
        period_type: 'monthly' or 'quarterly'
        period_offset: How many periods back to go (1 = previous period, 2 = two periods back, etc.)
    
    Returns:
        tuple: (start_date, end_date) in ISO format
    """
    today = date.today()
    
    if period_type == 'monthly':
        # Calculate previous month(s)
        current_month = today.replace(day=1)
        for _ in range(period_offset):
            current_month = (current_month - timedelta(days=1)).replace(day=1)
        
        start_date = current_month
        # Last day of the target month
        if current_month.month == 12:
            end_date = current_month.replace(year=current_month.year + 1, month=1)
        else:
            end_date = current_month.replace(month=current_month.month + 1)
            
    elif period_type == 'quarterly':
        # Calculate previous quarter(s)
        current_quarter = ((today.month - 1) // 3) + 1
        current_year = today.year
        
        for _ in range(period_offset):
            current_quarter -= 1
            if current_quarter < 1:
                current_quarter = 4
                current_year -= 1
        
        # Start of quarter
        start_month = (current_quarter - 1) * 3 + 1
        start_date = date(current_year, start_month, 1)
        
        # End of quarter
        end_month = start_month + 2
        if end_month > 12:
            end_date = date(current_year + 1, end_month - 12 + 1, 1)
        else:
            end_date = date(current_year, end_month + 1, 1)
    
    else:
        raise ValueError(f"Unsupported period_type: {period_type}")
    
    # Format dates in ISO8601 for the Intercom API
    start_date_iso = f"{start_date.strftime('%Y-%m-%d')}T00:00:00Z"
    end_date_iso = f"{end_date.strftime('%Y-%m-%d')}T00:00:00Z"
    
    logging.info(f"Requesting data for {period_type} period: from {start_date_iso} to {end_date_iso}")
    return start_date_iso, end_date_iso


class IntercomService:
    """Service wrapper for Intercom API interactions."""
    
    def __init__(self, api_key):
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Intercom-Version": "2.7"
        }
        self.base_url = "https://api.intercom.io"

    def create_export_job(self, start_date, end_date):
        """Creates an export job with basic error handling."""
        endpoint = f"{self.base_url}/export/content/data"
        payload = {
            "created_at_after": iso_to_unix(start_date),
            "created_at_before": iso_to_unix(end_date)
        }
        response = requests.post(endpoint, headers=self.headers, json=payload)
        if response.status_code == 200:
            result = response.json()
            if 'job_identifier' in result:
                return result['job_identifier']
            logging.error("No 'job_identifier' found in the response.")
        elif response.status_code == 429:
            logging.error("Error: Active job limit reached (1 per workspace)")
        else:
            logging.error(f"HTTP error {response.status_code}: {response.text}")
        return None

    def check_export_status(self, job_id):
        """Checks the status of an export job."""
        endpoint = f"{self.base_url}/export/content/data/{job_id}"
        response = requests.get(endpoint, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Error while checking job status: {response.status_code}")
            return None

    def download_export_results(self, download_url):
        """Downloads and extracts a gzipped CSV or ZIP archive produced by the export job."""
        download_headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/octet-stream"
        }
        response = requests.get(download_url, headers=download_headers)
        if response.status_code == 200:
            content = response.content

            # Detect file signature
            if content[:2] == b'\x1f\x8b':  # GZ
                with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
                    df = pd.read_csv(gz)
                return df
            elif content[:2] == b'PK':  # ZIP
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    dfs = []
                    for filename in zf.namelist():
                        if filename.endswith('.csv'):
                            with zf.open(filename) as csvfile:
                                dfs.append(pd.read_csv(csvfile))
                    if dfs:
                        df = pd.concat(dfs, ignore_index=True)
                        return df
                    logging.error("No CSV files found inside the ZIP archive.")
                    return None
            else:
                # It might simply be a plain CSV file
                try:
                    df = pd.read_csv(io.BytesIO(content))
                    return df
                except:
                    logging.error("Unknown export file format.")
                    return None
        else:
            logging.error(f"Error while downloading results: {response.status_code}")
            return None

    def get_nps_data(self, start_date, end_date):
        """Fetches NPS data from Intercom."""
        logging.info(f"Creating export job for data from {start_date} to {end_date}...")

        # Create the export job
        job_id = self.create_export_job(start_date, end_date)
        if not job_id:
            logging.error("Failed to create export job.")
            return None

        logging.info(f"Export job created. ID: {job_id}")

        # Wait until the export job is finished
        max_attempts = 30  # Maximum number of status-check attempts
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
                df = self.download_export_results(download_url)
                
                if df is not None:
                    logging.info(f"Loaded {len(df)} rows")
                    return df
                else:
                    logging.error("Failed to load data")
                    return None
            
            elif state in ('failed', 'no_data'):
                logging.error(f"Export failed: {status_info.get('message', 'unspecified')}")
                return None
            
            # Sleep before the next status check
            time.sleep(20)
        
        logging.error(f"Exceeded maximum attempts ({max_attempts}) to check export status")
        return None


class FetchIntercomData(beam.DoFn):
    """DoFn that fetches raw Intercom data and processes it for all regions using the new series structure."""
    
    def __init__(self, api_key, start_date, end_date, debug_mode=False):
        self.api_key = api_key
        self.start_date = start_date
        self.end_date = end_date
        self.debug_mode = debug_mode
        
    def process(self, _):
        intercom_service = IntercomService(self.api_key)
        data = intercom_service.get_nps_data(self.start_date, self.end_date)
        
        if data is None:
            logging.warning("Failed to retrieve data from Intercom")
            return
        
        # Log the column names for diagnostics
        logging.info(f"Loaded columns: {list(data.columns)}")
        
        # Enhanced debugging if enabled
        if self.debug_mode:
            self._log_data_structure(data)
        
        # Process data for each region/language
        for lang_code, config in NPS_SURVEYS.items():
            logging.info(f"Processing data for language: {lang_code}")
            
            # Find series data with enhanced logic
            series_data = self._find_series_data(data, config, lang_code)
            
            if series_data.empty:
                logging.warning(f"No data found for series: {config['series_name']}")
                continue
            
            # Find NPS surveys within the series
            nps_surveys = self._find_nps_surveys(series_data, config, lang_code)
            
            if nps_surveys.empty:
                logging.warning(f"No NPS surveys found for pattern: {config['survey_name_pattern']}")
                continue
            
            logging.info(f"Found {len(nps_surveys)} NPS survey responses for {lang_code}")
            
            results = []
            
            for _, row in nps_surveys.iterrows():
                # Extract NPS score and feedback
                score, feedback = self._extract_nps_data(row, config, lang_code)
                
                # Extract user information
                user_id = row.get('user_id') or row.get('user_external_id') or row.get('contact_id')
                user_name = row.get('name', '') or row.get('user_name', '')
                user_email = row.get('email', '') or row.get('user_email', '')
                
                # Extract creation date
                created_at = self._extract_creation_date(row)
                
                # Add result if we have a score or feedback
                if score is not None or (feedback and feedback.strip()):
                    results.append({
                        'user_uuid': str(user_id) if user_id else "",
                        'user_name': str(user_name) if user_name else "",
                        'user_email': str(user_email) if user_email else "",
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

    def _log_data_structure(self, data):
        """Logs data structure for diagnostics"""
        try:
            logging.info("=== DATA STRUCTURE ANALYSIS ===")
            
            # Log series-related columns
            series_columns = [col for col in data.columns if 'series' in col.lower()]
            if series_columns:
                logging.info(f"Series columns: {series_columns}")
                for col in series_columns[:2]:
                    try:
                        unique_values = data[col].dropna().unique()[:5]
                        logging.info(f"'{col}' samples: {list(unique_values)}")
                    except Exception as e:
                        logging.warning(f"Error processing column {col}: {e}")
            
            # Log survey-related columns
            survey_columns = [col for col in data.columns if any(word in col.lower() for word in ['survey', 'content', 'title', 'question'])]
            if survey_columns:
                logging.info(f"Survey columns: {survey_columns}")
                for col in survey_columns[:2]:
                    try:
                        unique_values = data[col].dropna().unique()[:5]
                        logging.info(f"'{col}' samples: {list(unique_values)}")
                    except Exception as e:
                        logging.warning(f"Error processing column {col}: {e}")
            
            # Log response columns
            response_columns = [col for col in data.columns if any(word in col.lower() for word in ['response', 'answer', 'rating', 'score'])]
            if response_columns:
                logging.info(f"Response columns: {response_columns}")
                for col in response_columns[:2]:
                    try:
                        unique_values = data[col].dropna().unique()[:5]
                        logging.info(f"'{col}' samples: {list(unique_values)}")
                    except Exception as e:
                        logging.warning(f"Error processing column {col}: {e}")
                        
        except Exception as e:
            logging.error(f"Error in data structure logging: {e}")

    def _find_series_data(self, data, config, lang_code):
        """Finds data for specific series with enhanced logic"""
        series_name = config['series_name']
        
        # Try different columns for series search
        possible_series_columns = ['series_title', 'series_name', 'content_title', 'series']
        
        for col in possible_series_columns:
            if col in data.columns:
                try:
                    # Exact match
                    exact_match = data[data[col] == series_name]
                    if not exact_match.empty:
                        logging.info(f"Found series data by exact match in '{col}' for {lang_code}")
                        return exact_match
                    
                    # Partial match
                    partial_match = data[data[col].str.contains(series_name, na=False, case=False)]
                    if not partial_match.empty:
                        logging.info(f"Found series data by partial match in '{col}' for {lang_code}")
                        return partial_match
                except Exception as e:
                    logging.warning(f"Error searching in column {col}: {e}")
        
        # Fallback: search by NPS question content
        nps_question = config['nps_question']
        question_columns = ['question', 'content_title', 'body', 'text']
        
        for col in question_columns:
            if col in data.columns:
                try:
                    question_match = data[data[col].str.contains(nps_question[:50], na=False, case=False)]
                    if not question_match.empty:
                        logging.info(f"Found data by NPS question match in '{col}' for {lang_code}")
                        return question_match
                except Exception as e:
                    logging.warning(f"Error searching by question in column {col}: {e}")
        
        logging.warning(f"No series data found for {lang_code}")
        return pd.DataFrame()

    def _find_nps_surveys(self, series_data, config, lang_code):
        """Finds NPS surveys within series data"""
        survey_pattern = config['survey_name_pattern']
        
        # Try different columns for survey name search
        possible_survey_columns = ['content_title', 'survey_name', 'title', 'name']
        
        for col in possible_survey_columns:
            if col in series_data.columns:
                try:
                    pattern_match = series_data[series_data[col].str.match(survey_pattern, na=False)]
                    if not pattern_match.empty:
                        logging.info(f"Found NPS surveys by pattern in '{col}' for {lang_code}")
                        return pattern_match
                except Exception as e:
                    logging.warning(f"Error matching pattern in column {col}: {e}")
        
        # Fallback: search for "NPS" keyword
        for col in possible_survey_columns:
            if col in series_data.columns:
                try:
                    nps_match = series_data[series_data[col].str.contains('NPS', na=False, case=False)]
                    if not nps_match.empty:
                        logging.info(f"Found surveys by 'NPS' keyword in '{col}' for {lang_code}")
                        return nps_match
                except Exception as e:
                    logging.warning(f"Error searching NPS keyword in column {col}: {e}")
        
        # If nothing found, return all series data
        logging.warning(f"Using all series data as NPS surveys for {lang_code}")
        return series_data

    def _extract_nps_data(self, row, config, lang_code):
        """Extracts NPS score and review from data row"""
        score = None
        feedback = None
        
        # Search for score in various columns
        score_columns = ['response', 'answer', 'rating', 'score', 'value']
        nps_question = config['nps_question']
        
        # First, try to find column with exact question name
        for col in row.index:
            if nps_question in str(col):
                try:
                    if pd.notnull(row[col]):
                        score = self._parse_score(row[col])
                        if score is not None:
                            break
                except Exception as e:
                    logging.warning(f"Error extracting score from question column: {e}")
        
        # If not found, search in standard columns
        if score is None:
            for col in score_columns:
                if col in row.index:
                    try:
                        if pd.notnull(row[col]):
                            score = self._parse_score(row[col])
                            if score is not None:
                                break
                    except Exception as e:
                        logging.warning(f"Error extracting score from {col}: {e}")
        
        # Search for feedback
        feedback_columns = ['body', 'message', 'feedback', 'comment', 'follow_up', 'text', 'content']
        for col in feedback_columns:
            if col in row.index:
                try:
                    if pd.notnull(row[col]):
                        feedback_value = str(row[col]).strip()
                        if feedback_value and len(feedback_value) > 3:
                            feedback = feedback_value
                            break
                except Exception as e:
                    logging.warning(f"Error extracting feedback from {col}: {e}")
        
        return score, feedback

    def _parse_score(self, score_value):
        """Parses score from various formats"""
        try:
            if pd.isna(score_value):
                return None
                
            if isinstance(score_value, (int, float)):
                score = int(score_value)
                return score if 0 <= score <= 10 else None
            
            if isinstance(score_value, str):
                # Remove non-digits
                cleaned = re.sub(r'[^\d]', '', score_value)
                if cleaned and cleaned.isdigit():
                    score = int(cleaned)
                    return score if 0 <= score <= 10 else None
        except Exception as e:
            logging.warning(f"Error parsing score '{score_value}': {e}")
        
        return None

    def _extract_creation_date(self, row):
        """Extracts creation date from various columns"""
        date_columns = ['created_at', 'updated_at', 'submitted_at', 'answered_at', 'received_at']
        
        for col in date_columns:
            if col in row.index:
                try:
                    if pd.notnull(row[col]):
                        date_value = row[col]
                        if isinstance(date_value, str):
                            # Try different date formats
                            for date_format in ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
                                try:
                                    return datetime.strptime(date_value, date_format)
                                except ValueError:
                                    continue
                        elif isinstance(date_value, (int, float)):
                            return datetime.fromtimestamp(date_value)
                        elif isinstance(date_value, datetime):
                            return date_value
                except Exception as e:
                    logging.warning(f"Error extracting date from {col}: {e}")
        
        # If no date found, use current time
        logging.warning("Could not extract creation date, using current time")
        return datetime.now()


class CalculateNPS(beam.DoFn):
    """DoFn that calculates the NPS metric and formats reviews."""
    
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
        all_periods = set()
        nps_periods = set()
        
        for result in results:
            try:
                user_uuid = result.get('user_uuid', '')
                user_name = result.get('user_name', '')
                user_email = result.get('user_email', '')
                country = result.get('country', '')
                date_obj = result.get('date')
                score = result.get('score')
                review = result.get('review', '')
                
                if not date_obj:
                    continue
                
                # Determine period string for NPS grouping
                if self.period_type == 'monthly':
                    period_str = date_obj.strftime('%Y-%m')
                elif self.period_type == 'quarterly':
                    quarter = (date_obj.month - 1) // 3 + 1
                    period_str = f"{date_obj.year}-Q{quarter}"
                else:
                    period_str = date_obj.strftime('%Y-%m')
                
                all_periods.add(period_str)
                
                # Filter by specific period if provided
                if self.run_period and period_str != self.run_period:
                    continue
                
                # Add data for NPS calculation (only if there is a score)
                if score is not None:
                    if period_str not in period_data:
                        period_data[period_str] = {
                            'scores': [],
                            'country': country
                        }
                    period_data[period_str]['scores'].append(score)
                    nps_periods.add(period_str)
                
                # Add review (even if text is empty, but there is a score)
                if review or score is not None:
                    # Determine quarter for review data
                    year = date_obj.year
                    quarter = (date_obj.month - 1) // 3 + 1
                    quarter_str = f"{year} Q{quarter}"
                    
                    # Safely convert score to integer
                    try:
                        score_int = int(score) if score is not None else 0
                    except (ValueError, TypeError):
                        score_int = 0
                    
                    review_data = ReviewData(
                        user_uuid=str(user_uuid),
                        user_name=str(user_name),
                        user_email=str(user_email),
                        country=str(country),
                        date=date_obj.strftime('%Y-%m-%d'),
                        quarter=quarter_str,
                        score=score_int,
                        review=str(review)
                    )
                    reviews.append(review_data)
                    
            except Exception as e:
                logging.error(f"Error processing result: {e}")
                continue
        
        logging.info(f"Prepared {len(reviews)} reviews for region {region}")
        logging.info(f"Periods found: {sorted(list(all_periods))}")
        logging.info(f"Periods with NPS scores: {sorted(list(nps_periods))}")
        
        # Calculate NPS for each period
        nps_results = []
        for period, data in period_data.items():
            try:
                scores = data['scores']
                country = data['country']
                
                if not scores:
                    continue
                    
                promoters = sum(1 for s in scores if s >= 9)
                detractors = sum(1 for s in scores if s <= 6)
                total = len(scores)
                neutral = total - promoters - detractors
                
                # Create NPS record
                nps_results.append(NPSData(
                    month=period,
                    country=country,
                    total=total,
                    promoters=promoters,
                    neutral=neutral,
                    detractors=detractors
                ))
            except Exception as e:
                logging.error(f"Error calculating NPS for period {period}: {e}")
                continue
        
        logging.info(f"Calculated NPS for {len(nps_results)} periods in region {region}")
        
        # Return NPS results and reviews
        for nps_result in nps_results:
            yield ('nps', nps_result)
        
        for review in reviews:
            yield ('review', review)


def write_to_sheets(reviews, spreadsheet_id, spreadsheet_name='NPS Data', credentials_path=None):
    """Writes reviews data to Google Sheets."""
    
    if not reviews:
        logging.warning("Empty reviews list – nothing to write to Google Sheets")
        return None
        
    logging.info(f"Starting to write {len(reviews)} reviews to Google Sheets (ID: {spreadsheet_id})")
    
    # Authenticate
    if credentials_path:
        logging.info(f"Using service-account credentials from file: {credentials_path}")
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path, 
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
    else:
        logging.info("Using Application Default Credentials in Dataflow")
        credentials, _ = google.auth.default(
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
    
    service = build('sheets', 'v4', credentials=credentials)
    
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
        reviews_sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']
        
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
        review_values.append([
            review.user_uuid,
            review.user_name,
            review.user_email,
            review.country,
            review.date,
            review.quarter,
            str(review.score),
            review.review
        ])
    
    # Write data in batches to stay within API limitations
    BATCH_SIZE = 500
    total_written = 0
    results = []
    
    for i in range(0, len(review_values), BATCH_SIZE):
        batch = review_values[i:i + BATCH_SIZE]
        current_row = next_row + i
        
        logging.info(f"Writing batch {i//BATCH_SIZE + 1} of {len(batch)} reviews starting from row {current_row}")
        
        body = {
            'values': batch
        }
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


class WriteReviewsToSheets(beam.DoFn):
    """DoFn that writes reviews into Google Sheets."""
    
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
    # Required arguments
    parser.add_argument('--api_key', required=True, help='Intercom API key')
    parser.add_argument('--target_table', required=True, help='BigQuery table for NPS data (project:dataset.table)')
    parser.add_argument('--spreadsheet_id', required=True, help='Google Spreadsheet ID for reviews')
    
    # Period configuration
    parser.add_argument('--period_type', default='monthly', choices=['monthly', 'quarterly'], 
                       help='Collection period type: monthly or quarterly')
    parser.add_argument('--period_offset', type=int, default=1, 
                       help='How many periods back to collect (1 = previous period)')
    
    # Optional arguments
    parser.add_argument('--credentials_path', help='Path to service account credentials file for Google Sheets')
    parser.add_argument('--debug', type=str, default='false', help='Enable debug mode (no period filtering)')
    parser.add_argument('--debug_data_only', type=str, default='false', help='Only run data structure debug (no pipeline)')
    
    # Date override (for manual runs)
    parser.add_argument('--start_date', help='Override start date in ISO format (YYYY-MM-DDThh:mm:ssZ)')
    parser.add_argument('--end_date', help='Override end date in ISO format (YYYY-MM-DDThh:mm:ssZ)')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Calculate the date range to fetch data
    if known_args.start_date and known_args.end_date:
        start_date = known_args.start_date
        end_date = known_args.end_date
        logging.info(f"Using provided dates: from {start_date} to {end_date}")
    else:
        start_date, end_date = calculate_period_dates(
            known_args.period_type, 
            known_args.period_offset
        )
    
    # Debug data only mode
    if known_args.debug_data_only.lower() == 'true':
        logging.info("Running in debug_data_only mode - will exit after data analysis")
        intercom_service = IntercomService(known_args.api_key)
        data = intercom_service.get_nps_data(start_date, end_date)
        
        if data is not None:
            logging.info(f"Data loaded successfully: {len(data)} rows, {len(data.columns)} columns")
            logging.info(f"Columns: {list(data.columns)}")
            
            # Basic analysis
            for i, (idx, row) in enumerate(data.head(3).iterrows()):
                key_cols = [col for col in row.index if any(word in col.lower() for word in 
                           ['content', 'title', 'series', 'survey', 'response', 'answer', 'question'])]
                if key_cols:
                    key_data = {col: row[col] for col in key_cols if pd.notnull(row[col])}
                    logging.info(f"Row {i} key data: {key_data}")
        else:
            logging.error("Failed to load data from Intercom")
        
        return
    
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    # Extract period string from start date (for logging/filtering)
    run_period = None
    if known_args.debug.lower() != 'true':
        start_dt = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
        if known_args.period_type == 'monthly':
            run_period = start_dt.strftime('%Y-%m')
        elif known_args.period_type == 'quarterly':
            quarter = (start_dt.month - 1) // 3 + 1
            run_period = f"{start_dt.year}-Q{quarter}"
    
    nps_table = known_args.target_table
    
    if run_period:
        logging.info(f"Starting NPS pipeline for {known_args.period_type} period: {run_period}")
    else:
        logging.info("Starting NPS pipeline in debug mode (no period filtering)")

    with beam.Pipeline(options=pipeline_options) as p:
        
        # Create a dummy input element to kick off the pipeline
        dummy_input = p | "Create Dummy Input" >> beam.Create([None])
        
        # Fetch data from Intercom
        intercom_data = dummy_input | "Fetch Intercom Data" >> beam.ParDo(
            FetchIntercomData(
                known_args.api_key,
                start_date,
                end_date,
                debug_mode=(known_args.debug.lower() == 'true')
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
        nps_for_bq = nps_data | "Format NPS for BigQuery" >> beam.Map(lambda x: x[1]._asdict())
        
        # Write NPS data to BigQuery
        nps_for_bq | "Write NPS to BigQuery" >> WriteToBigQuery(
            nps_table,
            schema=NPS_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
        
        # Format reviews for Google Sheets
        reviews_for_sheets = reviews_data | "Format Reviews for Sheets" >> beam.Map(lambda x: x[1])
        
        # Log review statistics
        reviews_count = reviews_for_sheets | "Count Reviews" >> beam.combiners.Count.Globally()
        reviews_count | "Log Reviews Count" >> beam.Map(lambda count: logging.info(f"Total number of reviews to write: {count}"))
        
        # Collect all reviews into a single list
        reviews_as_list = reviews_for_sheets | "Collect Reviews" >> beam.combiners.ToList()
        
        # Write reviews to Google Sheets and log the result
        sheets_result = dummy_input | "Write Reviews to Sheets" >> beam.ParDo(
            WriteReviewsToSheets(
                known_args.spreadsheet_id, 
                known_args.credentials_path
            ),
            reviews_as_list=beam.pvalue.AsSingleton(reviews_as_list)
        )
        
        # Log the outcome of the write operation
        sheets_result | "Log Sheets Result" >> beam.Map(
            lambda result: logging.info(f"Sheets write result: {result}")
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()