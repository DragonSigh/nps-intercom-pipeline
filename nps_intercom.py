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

import google.auth
from google.oauth2 import service_account
from googleapiclient.discovery import build

from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery


# Размер батча для обработки
BATCH_SIZE = 100
MAX_RETRIES = 3

# Конфигурация NPS опросов (остается той же)
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


class IntercomService:
    """Service wrapper for Intercom API interactions with retry logic."""
    
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
                response = requests.request(method, url, timeout=60, **kwargs)
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
                time.sleep(2 ** attempt)  # Exponential backoff
        
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

    def download_export_results_chunked(self, download_url):
        """Downloads export results and yields rows instead of loading everything into memory."""
        download_headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/octet-stream"
        }
        
        response = self._make_request_with_retry('GET', download_url, headers=download_headers, stream=True)
        
        if response.status_code != 200:
            logging.error(f"Error while downloading results: {response.status_code}")
            return
        
        # Получаем содержимое для определения типа файла
        content = b''
        for chunk in response.iter_content(chunk_size=8192):
            content += chunk
            if len(content) >= 10:  # Достаточно для определения типа
                break
        
        # Дочитываем остальное содержимое
        for chunk in response.iter_content(chunk_size=8192):
            content += chunk
        
        try:
            # Обрабатываем разные форматы файлов
            if content[:2] == b'\x1f\x8b':  # GZ
                with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
                    # Читаем CSV построчно
                    text_content = gz.read().decode('utf-8')
                    lines = text_content.strip().split('\n')
                    if lines:
                        headers = lines[0].split(',')
                        for line in lines[1:]:
                            if line.strip():
                                values = line.split(',')
                                if len(values) == len(headers):
                                    yield dict(zip(headers, values))
                                    
            elif content[:2] == b'PK':  # ZIP
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    for filename in zf.namelist():
                        if filename.endswith('.csv'):
                            with zf.open(filename) as csvfile:
                                text_content = csvfile.read().decode('utf-8')
                                lines = text_content.strip().split('\n')
                                if lines:
                                    headers = lines[0].split(',')
                                    for line in lines[1:]:
                                        if line.strip():
                                            values = line.split(',')
                                            if len(values) == len(headers):
                                                yield dict(zip(headers, values))
            else:
                # Plain CSV
                text_content = content.decode('utf-8')
                lines = text_content.strip().split('\n')
                if lines:
                    headers = lines[0].split(',')
                    for line in lines[1:]:
                        if line.strip():
                            values = line.split(',')
                            if len(values) == len(headers):
                                yield dict(zip(headers, values))
                                
        except Exception as e:
            logging.error(f"Error processing file content: {e}")

    def get_nps_data_streaming(self, start_date, end_date):
        """Fetches NPS data from Intercom and yields rows instead of loading everything."""
        logging.info(f"Creating export job for data from {start_date} to {end_date}...")

        job_id = self.create_export_job(start_date, end_date)
        if not job_id:
            logging.error("Failed to create export job.")
            return

        logging.info(f"Export job created. ID: {job_id}")

        # Wait for job completion
        max_attempts = 30
        for attempt in range(max_attempts):
            status_info = self.check_export_status(job_id)
            if not status_info:
                logging.error("Export job not found or was deleted")
                return

            state = status_info.get('status', 'unknown')
            logging.info(f"Status: {state.upper()}")

            if state == 'completed':
                download_url = status_info.get('download_url')
                if not download_url:
                    logging.error("Error: 'download_url' is missing in the response")
                    return

                logging.info("Downloading and processing data...")
                yield from self.download_export_results_chunked(download_url)
                return
            
            elif state in ('failed', 'no_data'):
                logging.error(f"Export failed: {status_info.get('message', 'unspecified')}")
                return
            
            time.sleep(20)
        
        logging.error(f"Exceeded maximum attempts ({max_attempts}) to check export status")


class CreateDataFetchTasks(beam.DoFn):
    """DoFn that creates tasks for data fetching instead of fetching everything at once."""
    
    def __init__(self, api_key, start_date, end_date):
        self.api_key = api_key
        self.start_date = start_date
        self.end_date = end_date
        
    def process(self, _):
        # Создаем задачу для каждого региона
        for lang_code, config in NPS_SURVEYS.items():
            yield {
                'region': lang_code,
                'config': config,
                'api_key': self.api_key,
                'start_date': self.start_date,
                'end_date': self.end_date
            }


class ProcessIntercomData(beam.DoFn):
    """DoFn that processes individual rows from Intercom data."""
    
    def setup(self):
        self.intercom_service = None
    
    def process(self, task):
        if self.intercom_service is None:
            self.intercom_service = IntercomService(task['api_key'])
        
        region = task['region']
        config = task['config']
        
        logging.info(f"Processing data for region: {region}")
        
        row_count = 0
        results = []
        
        try:
            # Обрабатываем данные построчно
            for row in self.intercom_service.get_nps_data_streaming(task['start_date'], task['end_date']):
                row_count += 1
                
                # Фильтруем по серии
                if row.get('series_name') != config['series_name']:
                    continue
                
                # Фильтруем по типу опроса
                survey_name = row.get('survey_name', '')
                if not re.match(config['survey_name_pattern'], survey_name):
                    continue
                
                # Извлекаем данные
                result = self._extract_nps_data(row, config)
                if result:
                    results.append(result)
                
                # Обрабатываем батчами для контроля памяти
                if len(results) >= BATCH_SIZE:
                    yield {
                        'region': region,
                        'results': results.copy()
                    }
                    results.clear()
            
            # Отправляем оставшиеся результаты
            if results:
                yield {
                    'region': region,
                    'results': results
                }
            
            logging.info(f"Processed {row_count} rows for region {region}")
            
        except Exception as e:
            logging.error(f"Error processing data for region {region}: {e}")
            # Продолжаем работу для других регионов
            
    def _extract_nps_data(self, row, config):
        """Extracts NPS data from a single row."""
        score = None
        feedback = None
        
        # Извлекаем оценку NPS
        score_columns = ['answer', 'rating', 'score', 'response']
        for col in score_columns:
            if col in row and row[col]:
                score_value = row[col]
                
                if isinstance(score_value, str):
                    if score_value.isdigit():
                        score = int(score_value)
                    else:
                        match = re.search(r'\d+', score_value)
                        if match:
                            score = int(match.group())
                elif isinstance(score_value, (int, float)):
                    score = int(score_value)
                
                if score is not None:
                    break
        
        # Извлекаем отзыв
        feedback_columns = ['body', 'message', 'feedback', 'comment', 'follow_up']
        for col in feedback_columns:
            if col in row and row[col]:
                feedback_value = row[col]
                if isinstance(feedback_value, str) and feedback_value.strip():
                    feedback = feedback_value.strip()
                    break
        
        # Извлекаем информацию о пользователе
        user_id = row.get('user_id') or row.get('user_external_id') or row.get('contact_id')
        user_name = row.get('name', '') or row.get('user_name', '')
        user_email = row.get('email', '') or row.get('user_email', '')
        
        # Извлекаем дату создания
        created_at = row.get('created_at') or row.get('updated_at') or row.get('submitted_at')
        if created_at:
            try:
                if isinstance(created_at, str):
                    if 'T' in created_at:
                        created_at = datetime.strptime(created_at.replace('Z', ''), "%Y-%m-%dT%H:%M:%S")
                    else:
                        created_at = datetime.strptime(created_at, "%Y-%m-%d")
                elif isinstance(created_at, (int, float)):
                    created_at = datetime.fromtimestamp(created_at)
            except (ValueError, TypeError) as e:
                logging.warning(f"Could not parse date '{created_at}': {e}")
                created_at = datetime.now()
        else:
            created_at = datetime.now()
        
        # Возвращаем результат, если есть оценка или отзыв
        if score is not None or (feedback and feedback.strip()):
            return {
                'user_uuid': str(user_id) if user_id else "",
                'user_name': str(user_name) if user_name else "",
                'user_email': str(user_email) if user_email else "",
                'country': config['country_code'],
                'date': created_at,
                'score': score,
                'review': feedback
            }
        
        return None


class CalculateNPS(beam.DoFn):
    """DoFn that calculates the NPS metric and formats reviews."""
    
    def __init__(self, period_type='monthly', run_period=None):
        self.period_type = period_type
        self.run_period = run_period
        
    def process(self, element):
        region = element['region']
        results = element['results']
        
        if not results:
            return
        
        logging.info(f"Calculating NPS for {len(results)} results in region {region}")
        
        # Группируем данные по периодам
        period_data = {}
        reviews = []
        
        for result in results:
            date_obj = result.get('date')
            score = result.get('score')
            
            # Определяем период
            if self.period_type == 'monthly':
                period_str = date_obj.strftime('%Y-%m')
            elif self.period_type == 'quarterly':
                quarter = (date_obj.month - 1) // 3 + 1
                period_str = f"{date_obj.year}-Q{quarter}"
            else:
                period_str = date_obj.strftime('%Y-%m')
            
            # Фильтруем по нужному периоду, если указан
            if self.run_period and period_str != self.run_period:
                continue
            
            # Добавляем данные для расчета NPS
            if score is not None:
                if period_str not in period_data:
                    period_data[period_str] = {
                        'scores': [],
                        'country': result.get('country')
                    }
                period_data[period_str]['scores'].append(score)
            
            # Добавляем отзыв
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
        
        # Рассчитываем NPS для каждого периода
        for period, data in period_data.items():
            scores = data['scores']
            country = data['country']
            
            if not scores:
                continue
                
            promoters = sum(1 for s in scores if s >= 9)
            detractors = sum(1 for s in scores if s <= 6)
            total = len(scores)
            neutral = total - promoters - detractors
            
            nps_result = beam.Row(
                month=period,
                country=country,
                total=total,
                promoters=promoters,
                neutral=neutral,
                detractors=detractors
            )
            yield ('nps', nps_result)
        
        # Отправляем отзывы
        for review in reviews:
            yield ('review', review)


class WriteReviewsToSheets(beam.DoFn):
    """DoFn that writes reviews to Google Sheets with better error handling."""
    
    def __init__(self, spreadsheet_id, credentials_path=None):
        self.spreadsheet_id = spreadsheet_id
        self.credentials_path = credentials_path
        self.service = None

    def setup(self):
        """Initialize Google Sheets service."""
        try:
            if self.credentials_path:
                credentials = service_account.Credentials.from_service_account_file(
                    self.credentials_path, 
                    scopes=['https://www.googleapis.com/auth/spreadsheets']
                )
            else:
                credentials, _ = google.auth.default(
                    scopes=['https://www.googleapis.com/auth/spreadsheets']
                )
            
            self.service = build('sheets', 'v4', credentials=credentials)
            logging.info("Google Sheets service initialized successfully")
        except Exception as e:
            logging.error(f"Failed to initialize Google Sheets service: {e}")
            raise

    def process(self, reviews_batch):
        if not reviews_batch or not self.service:
            return
            
        try:
            # Проверяем/создаем лист
            self._ensure_sheet_exists()
            
            # Получаем следующую строку для записи
            next_row = self._get_next_row()
            
            # Форматируем данные для записи
            review_values = []
            for review in reviews_batch:
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
            
            # Записываем данные
            if review_values:
                body = {'values': review_values}
                result = self.service.spreadsheets().values().update(
                    spreadsheetId=self.spreadsheet_id,
                    range=f'NPS Reviews!A{next_row}',
                    valueInputOption='RAW',
                    body=body
                ).execute()
                
                updated_rows = result.get('updatedRows', 0)
                logging.info(f"Successfully wrote {updated_rows} reviews to Google Sheets")
                yield {'status': 'success', 'written_reviews': updated_rows}
                
        except Exception as e:
            error_message = str(e)
            logging.error(f"Error writing to Google Sheets: {error_message}")
            yield {'status': 'error', 'error': error_message}

    def _ensure_sheet_exists(self):
        """Ensures the NPS Reviews sheet exists."""
        sheet_metadata = self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
        sheets = sheet_metadata.get('sheets', [])
        
        # Проверяем существование листа
        sheet_exists = any(sheet['properties']['title'] == 'NPS Reviews' for sheet in sheets)
        
        if not sheet_exists:
            # Создаем лист
            body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': 'NPS Reviews',
                            'gridProperties': {
                                'rowCount': 5000,
                                'columnCount': 8
                            }
                        }
                    }
                }]
            }
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=body
            ).execute()
            
            # Добавляем заголовки
            self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range='NPS Reviews!A1:H1',
                valueInputOption='RAW',
                body={
                    'values': [['user_uuid', 'user_name', 'user_email', 'country', 'date', 'quarter', 'score', 'review']]
                }
            ).execute()

    def _get_next_row(self):
        """Gets the next available row number."""
        response = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range='NPS Reviews!A:A'
        ).execute()
        values = response.get('values', [])
        return len(values) + 1


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
        # Создаем задачи для получения данных
        data_tasks = (p 
            | "Create Dummy Input" >> beam.Create([None])
            | "Create Data Fetch Tasks" >> beam.ParDo(
                CreateDataFetchTasks(known_args.api_key, start_date, end_date)
            )
        )
        
        # Обрабатываем данные построчно по регионам
        intercom_data = (data_tasks
            | "Process Intercom Data" >> beam.ParDo(ProcessIntercomData())
        )
        
        # Рассчитываем NPS и форматируем отзывы
        nps_and_reviews = (intercom_data 
            | "Calculate NPS" >> beam.ParDo(
                CalculateNPS(
                    period_type=known_args.period_type,
                    run_period=run_period
                )
            )
        )
        
        # Разделяем NPS и отзывы
        nps_data, reviews_data = (nps_and_reviews 
            | "Split NPS and Reviews" >> beam.Partition(
                lambda elem, _: 0 if elem[0] == 'nps' else 1, 2
            )
        )
        
        # Форматируем NPS данные для BigQuery
        nps_for_bq = (nps_data 
            | "Format NPS for BigQuery" >> beam.Map(lambda elem: elem[1]._asdict())
        )
        
        # Записываем NPS в BigQuery
        (nps_for_bq 
            | "Write NPS to BigQuery" >> WriteToBigQuery(
                known_args.target_table,
                schema=NPS_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )
        
        # Форматируем отзывы для Google Sheets
        reviews_for_sheets = (reviews_data 
            | "Format Reviews for Sheets" >> beam.Map(lambda x: x[1])
        )
        
        # Группируем отзывы в батчи
        reviews_batched = (reviews_for_sheets
            | "Batch Reviews" >> beam.BatchElements(min_batch_size=10, max_batch_size=50)
        )
        
        # Записываем отзывы в Google Sheets
        sheets_results = (reviews_batched
            | "Write Reviews to Sheets" >> beam.ParDo(
                WriteReviewsToSheets(
                    known_args.spreadsheet_id, 
                    known_args.credentials_path
                )
            )
        )
        
        # Логируем результаты
        (sheets_results 
            | "Log Sheets Results" >> beam.Map(
                lambda result: logging.info(f"Sheets write result: {result}")
            )
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()