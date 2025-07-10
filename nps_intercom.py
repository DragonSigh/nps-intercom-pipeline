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


# Названия колонок NPS для разных языков
NPS_COLUMNS = {
    'RU': {
        'score': "Пожалуйста, оцените вероятность того, что вы порекомендуете наш продукт друзьям и знакомым:",
        'feedback': "Если Вам есть чем поделиться о своем опыте использования GoBe, мы будем рады обратной связи!"
    },
    'US': {
        'score': "How likely is it that you would recommend HEALBE GoBe to a friend or colleague",
        'feedback': "If you have something to share about your GoBe experience, please feel free"
    },
    'JP': {
        'score': "友人やご家族のメンバーにGoBeをお勧めしますか？",
        'feedback': "GoBeのご利用の体験について共有したいご意見などがございましたら、是非ともお聞かせください。"
    }
}

# Схемы для BigQuery
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

# Типы данных для вывода
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
    """Преобразует ISO8601 в Unix timestamp"""
    dt = datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%SZ")
    return int(dt.timestamp())


def calculate_previous_month_dates():
    """Вычисляет дату начала и конца предыдущего месяца"""
    today = date.today()
    # Первое число текущего месяца
    first_of_this_month = today.replace(day=1)
    # Последний день предыдущего месяца (день перед первым числом этого месяца)
    last_of_previous_month = first_of_this_month - timedelta(days=1)
    # Первое число предыдущего месяца
    first_of_previous_month = last_of_previous_month.replace(day=1)
    
    # Форматируем даты в ISO8601 для Intercom API
    start_date = f"{first_of_previous_month.strftime('%Y-%m-%d')}T00:00:00Z"
    end_date = f"{first_of_this_month.strftime('%Y-%m-%d')}T00:00:00Z"
    
    logging.info(f"Запрашиваем данные за предыдущий месяц: с {start_date} по {end_date}")
    return start_date, end_date


class IntercomService:
    """Сервис для работы с Intercom API"""
    
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
        """Создает задание экспорта с обработкой ошибок"""
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
            logging.error("В ответе нет job_identifier.")
        elif response.status_code == 429:
            logging.error("Ошибка: Достигнут лимит активных заданий (1 на workspace)")
        else:
            logging.error(f"HTTP ошибка {response.status_code}: {response.text}")
        return None

    def check_export_status(self, job_id):
        """Проверяет статус задания"""
        endpoint = f"{self.base_url}/export/content/data/{job_id}"
        response = requests.get(endpoint, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Ошибка при проверке статуса задания: {response.status_code}")
            return None

    def download_export_results(self, download_url):
        """Скачивает и распаковывает gzipped CSV или ZIP-архив экспорта"""
        download_headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/octet-stream"
        }
        response = requests.get(download_url, headers=download_headers)
        if response.status_code == 200:
            content = response.content

            # Проверяем сигнатуру файла
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
                    logging.error("В ZIP-архиве нет CSV-файлов.")
                    return None
            else:
                # Возможно это просто CSV файл
                try:
                    df = pd.read_csv(io.BytesIO(content))
                    return df
                except:
                    logging.error("Неизвестный формат файла экспорта.")
                    return None
        else:
            logging.error(f"Ошибка при загрузке результатов: {response.status_code}")
            return None

    def get_nps_data(self, start_date, end_date):
        """Загружает данные NPS из Intercom"""
        logging.info(f"Создание задания на экспорт данных с {start_date} по {end_date}...")

        # Создаем задание на экспорт
        job_id = self.create_export_job(start_date, end_date)
        if not job_id:
            logging.error(f"Ошибка создания задания.")
            return None

        logging.info(f"Задание создано. ID: {job_id}")

        # Ожидаем завершения задания
        max_attempts = 30  # Максимальное количество попыток проверки статуса
        for attempt in range(max_attempts):
            status_info = self.check_export_status(job_id)
            if not status_info:
                logging.error("Задание не найдено или удалено")
                return None

            state = status_info.get('status', 'unknown')
            logging.info(f"Статус: {state.upper()}")

            if state == 'completed':
                download_url = status_info.get('download_url')
                if not download_url:
                    logging.error("Ошибка: отсутствует download_url")
                    return None

                logging.info("Загрузка и обработка данных...")
                df = self.download_export_results(download_url)
                
                if df is not None:
                    logging.info(f"Загружено {len(df)} строк")
                    return df
                else:
                    logging.error(f"Ошибка загрузки данных")
                    return None
            
            elif state in ('failed', 'no_data'):
                logging.error(f"Экспорт завершился с ошибкой: {status_info.get('message', 'не указана')}")
                return None
            
            # Ждем перед следующей проверкой
            time.sleep(20)
        
        logging.error(f"Превышено максимальное количество попыток ({max_attempts}) для проверки статуса экспорта")
        return None


class FetchIntercomData(beam.DoFn):
    """DoFn для получения данных из Intercom API и обработки их для всех регионов"""
    
    def __init__(self, api_key, start_date, end_date):
        self.api_key = api_key
        self.start_date = start_date
        self.end_date = end_date
        
    def process(self, _):
        intercom_service = IntercomService(self.api_key)
        data = intercom_service.get_nps_data(self.start_date, self.end_date)
        
        if data is None:
            logging.warning("Не удалось получить данные из Intercom")
            return
        
        # Логируем название колонок для диагностики
        logging.info(f"Загруженные колонки: {list(data.columns)}")
        
        # Безопасно извлекаем диапазон дат
        if 'created_at' in data.columns:
            try:
                # Преобразуем datetime значения к строкам для безопасного вывода
                created_at_sample = data['created_at'].astype(str)
                logging.info(f"Примеры значений created_at: {created_at_sample.head(5).tolist()}")
                
                # Попытка преобразовать все даты к единому формату
                if data['created_at'].dtype == 'object':  # если строковый или смешанный тип
                    logging.info("Колонка created_at содержит строковые или смешанные типы данных")
                else:
                    min_date = data['created_at'].min()
                    max_date = data['created_at'].max()
                    logging.info(f"Диапазон дат в данных: {min_date} - {max_date}")
            except Exception as e:
                logging.warning(f"Не удалось определить диапазон дат: {str(e)}")
        
        # Проверяем наличие данных NPS для каждого региона
        for region in ['RU', 'JP', 'US']:
            score_column = NPS_COLUMNS.get(region, NPS_COLUMNS['US'])['score']
            feedback_column = NPS_COLUMNS.get(region, NPS_COLUMNS['US'])['feedback']
            
            logging.info(f"Обработка данных для региона {region}. Колонка с оценками: '{score_column}'")
            
            # Обрабатываем данные
            results = []
            
            # Проверяем наличие колонок
            if score_column not in data.columns and feedback_column not in data.columns:
                logging.warning(f"Для региона {region} не найдены требуемые колонки")
                continue
                
            for _, row in data.iterrows():
                score = None
                feedback = None
                
                # Извлекаем оценку NPS с обработкой списков
                if score_column in row and pd.notnull(row[score_column]):
                    score_value = row[score_column]
                    
                    # Преобразуем строковые представления списков в объекты Python
                    if isinstance(score_value, str) and ('[' in score_value or '{' in score_value):
                        try:
                            parsed_value = ast.literal_eval(score_value)
                            if isinstance(parsed_value, list) and len(parsed_value) > 0:
                                score_value = parsed_value[0]
                            else:
                                score_value = parsed_value
                        except:
                            pass
                    
                    # Обработка значения оценки
                    if isinstance(score_value, str):
                        # Для русской версии: "0 - Мало вероятно" или "10 - Точно порекомендую"
                        if " - " in score_value:
                            score = int(score_value.split(" - ")[0])
                        else:
                            # Пытаемся извлечь числовое значение
                            match = re.search(r'\d+', score_value)
                            if match:
                                score = int(match.group())
                    else:
                        # Если уже числовое значение
                        try:
                            score = int(float(score_value))
                        except (ValueError, TypeError):
                            score = None
                
                # Извлекаем текст отзыва с обработкой списков
                if feedback_column in row and pd.notnull(row[feedback_column]):
                    feedback_value = row[feedback_column]
                    
                    # Преобразуем строковые представления списков в объекты Python
                    if isinstance(feedback_value, str) and ('[' in feedback_value or '{' in feedback_value):
                        try:
                            parsed_value = ast.literal_eval(feedback_value)
                            if isinstance(parsed_value, list) and len(parsed_value) > 0:
                                feedback_value = parsed_value[0]
                            else:
                                feedback_value = parsed_value
                        except:
                            pass
                    
                    feedback = str(feedback_value)
                
                # Извлекаем идентификатор пользователя и имя
                user_id = row.get('user_id', None)
                if user_id is None:
                    user_id = row.get('user_external_id', None)
                
                user_name = row.get('name', '')
                user_email = row.get('email', '')
                
                # Извлекаем дату создания
                created_at = row.get('created_at', None)
                if created_at:
                    try:
                        # Преобразуем в datetime
                        if isinstance(created_at, str):
                            created_at = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
                        elif isinstance(created_at, (int, float)):
                            created_at = datetime.fromtimestamp(created_at)
                    except (ValueError, TypeError):
                        created_at = datetime.now()
                else:
                    created_at = datetime.now()
                
                # Добавляем запись только если есть оценка или отзыв
                if score is not None or (feedback is not None and feedback.strip()):
                    results.append({
                        'user_uuid': user_id,
                        'user_name': user_name,
                        'user_email': user_email,
                        'country': region,
                        'date': created_at,
                        'score': score,
                        'review': feedback
                    })
            
            logging.info(f"Обработано {len(results)} записей для региона {region}")
            yield {
                'region': region,
                'results': results
            }


class CalculateNPS(beam.DoFn):
    """DoFn для расчета NPS и форматирования отзывов"""
    
    def __init__(self, run_month=None):
        """
        Инициализирует обработчик с опциональным указанием конкретного месяца
        
        Args:
            run_month: Строка в формате 'YYYY-MM', указывающая месяц, за который обрабатываются данные
        """
        self.run_month = run_month
        
    def process(self, element):
        region = element['region']
        results = element['results']
        
        if not results:
            logging.warning(f"Нет данных для обработки в регионе {region}")
            return
        
        logging.info(f"Получено {len(results)} результатов для обработки в регионе {region}")
        
        # Группируем данные по месяцу для расчета NPS
        month_data = {}
        
        # Подготавливаем отзывы
        reviews = []
        all_months = set()
        nps_months = set()
        
        for result in results:
            user_uuid = result.get('user_uuid')
            user_name = result.get('user_name', '')
            user_email = result.get('user_email', '')
            country = result.get('country')
            date_obj = result.get('date')
            score = result.get('score')
            review = result.get('review')
            
            # Определяем месяц для группировки NPS
            month_str = date_obj.strftime('%Y-%m')
            all_months.add(month_str)
            
            # СТАЛО: логирование месяца и пропуск фильтрации для отладки
            if self.run_month and month_str != self.run_month:
                logging.info(f"Найдены данные за месяц {month_str}, ожидаемый месяц: {self.run_month}")
            
            # Добавляем данные для расчета NPS (только если есть оценка)
            if score is not None:
                if month_str not in month_data:
                    month_data[month_str] = {
                        'scores': [],
                        'country': country
                    }
                month_data[month_str]['scores'].append(score)
                nps_months.add(month_str)
            
            # Добавляем отзыв (даже если текст пустой, но есть оценка)
            if review or score is not None:
                # Определяем квартал
                year = date_obj.year
                quarter = (date_obj.month - 1) // 3 + 1
                quarter_str = f"{year} Q{quarter}"
                
                # Безопасно преобразуем данные к строке
                user_uuid_str = str(user_uuid) if user_uuid else ""
                user_name_str = str(user_name) if user_name else ""
                user_email_str = str(user_email) if user_email else ""
                review_str = str(review) if review else ""
                
                # Безопасно преобразуем score к целому числу
                try:
                    score_int = int(score) if score is not None else 0
                except (ValueError, TypeError):
                    logging.warning(f"Не удалось преобразовать оценку '{score}' к целому числу, используем 0")
                    score_int = 0
                
                review_data = ReviewData(
                    user_uuid=user_uuid_str,
                    user_name=user_name_str,
                    user_email=user_email_str,
                    country=country,
                    date=date_obj.strftime('%Y-%m-%d'),
                    quarter=quarter_str,
                    score=score_int,
                    review=review_str
                )
                reviews.append(review_data)
                
                # Для отладки: выводим первые 3 отзыва и показываем пример непустого отзыва
                if len(reviews) <= 3 or (len(reviews) <= 10 and review_str):
                    logging.info(f"Отзыв #{len(reviews)}: name={user_name_str}, score={score_int}, review='{review_str[:50]}...' (user: {user_uuid_str[:8]})")
        
        logging.info(f"Подготовлено {len(reviews)} отзывов для региона {region}")
        logging.info(f"Найдены данные за месяцы: {sorted(list(all_months))}")
        logging.info(f"Месяцы с NPS оценками: {sorted(list(nps_months))}")
        
        # Рассчитываем NPS для каждого месяца
        nps_results = []
        for month, data in month_data.items():
            scores = data['scores']
            country = data['country']
            
            if not scores:
                continue
                
            promoters = sum(1 for s in scores if s >= 9)
            detractors = sum(1 for s in scores if s <= 6)
            total = len(scores)
            neutral = total - promoters - detractors
            
            # Расчет NPS
            nps_score = (promoters - detractors) / total * 100 if total > 0 else 0
            
            # Создаем запись NPS без поля nps_score
            nps_results.append(NPSData(
                month=month,
                country=country,
                total=total,
                promoters=promoters,
                neutral=neutral,
                detractors=detractors
            ))
        
        logging.info(f"Рассчитано NPS для {len(nps_results)} месяцев в регионе {region}")
        logging.info(f"Найдено {len(reviews)} отзывов в регионе {region}")
        
        # Возвращаем результаты NPS и отзывы
        for nps_result in nps_results:
            yield ('nps', nps_result)
        
        # Всегда возвращаем все отзывы, независимо от месяца
        for review in reviews:
            yield ('review', review)


def write_to_sheets(reviews, spreadsheet_id, spreadsheet_name='NPS Data', credentials_path=None):
    """Записывает отзывы в Google Sheets"""
    
    if not reviews:
        logging.warning("Пустой список отзывов, нечего записывать в Google Sheets")
        return None
        
    logging.info(f"Начинаем запись {len(reviews)} отзывов в Google Sheets (ID: {spreadsheet_id})")
    
    # Аутентификация
    if credentials_path:
        logging.info(f"Используем учетные данные из файла: {credentials_path}")
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path, 
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
    else:
        logging.info("Используем учетные данные по умолчанию для Dataflow")
        credentials, _ = google.auth.default(
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
    
    service = build('sheets', 'v4', credentials=credentials)
    
    # Получаем список текущих листов
    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get('sheets', '')
    
    # Проверяем наличие листа "NPS Reviews"
    reviews_sheet_id = None
    for sheet in sheets:
        if sheet['properties']['title'] == 'NPS Reviews':
            reviews_sheet_id = sheet['properties']['sheetId']
            break
    
    # Если лист не существует, создаем его
    if not reviews_sheet_id:
        logging.info("Лист 'NPS Reviews' не найден, создаем новый")
        body = {
            'requests': [{
                'addSheet': {
                    'properties': {
                        'title': 'NPS Reviews',
                        'gridProperties': {
                            'rowCount': 2000,  # Увеличиваем начальный размер
                            'columnCount': 8   # Увеличиваем количество колонок для новых полей
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
        
        # Добавляем заголовки
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='NPS Reviews!A1:H1',
            valueInputOption='RAW',
            body={
                'values': [['user_uuid', 'user_name', 'user_email', 'country', 'date', 'quarter', 'score', 'review']]
            }
        ).execute()
        logging.info("Создан новый лист и добавлены заголовки")
    else:
        logging.info("Найден существующий лист 'NPS Reviews'")
    
    # Получаем количество существующих строк
    response = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='NPS Reviews!A:A'
    ).execute()
    values = response.get('values', [])
    next_row = len(values) + 1
    logging.info(f"Обнаружено {next_row-1} строк в таблице, начинаем запись с {next_row} строки")
    
    # Форматируем данные для загрузки
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
    
    # Записываем данные с разбивкой на пакеты для обхода ограничений API
    BATCH_SIZE = 500  # Google Sheets API имеет ограничения на размер запроса
    total_written = 0
    results = []
    
    for i in range(0, len(review_values), BATCH_SIZE):
        batch = review_values[i:i + BATCH_SIZE]
        current_row = next_row + i
        
        logging.info(f"Записываем пакет {i//BATCH_SIZE + 1} из {len(batch)} отзывов начиная со строки {current_row}")
        
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
            
            logging.info(f"Успешно записано {updated_rows} строк в пакете {i//BATCH_SIZE + 1}")
        except Exception as e:
            logging.error(f"Ошибка при записи пакета {i//BATCH_SIZE + 1}: {str(e)}")
    
    logging.info(f"Всего записано {total_written} отзывов в Google Sheets")
    return results


class WriteReviewsToSheets(beam.DoFn):
    """DoFn для записи отзывов в Google Sheets"""
    
    def __init__(self, spreadsheet_id, credentials_path=None):
        self.spreadsheet_id = spreadsheet_id
        self.credentials_path = credentials_path

    def process(self, _, reviews_as_list):
        if not reviews_as_list:
            logging.warning("Пустой список отзывов, нечего записывать")
            return
            
        try:
            logging.info(f"Передано {len(reviews_as_list)} отзывов в функцию записи в Google Sheets")
            
            results = write_to_sheets(
                reviews_as_list, 
                self.spreadsheet_id, 
                credentials_path=self.credentials_path
            )
            
            if results:
                total_written = sum(result.get('updatedRows', 0) for result in results)
                logging.info(f"Итого записано {total_written} отзывов в Google Sheets из {len(reviews_as_list)} переданных")
                yield {'status': 'success', 'written_reviews': total_written}
            else:
                logging.warning("Запись в Google Sheets не выполнена, результат пустой")
                yield {'status': 'no_data', 'written_reviews': 0}
                
        except Exception as e:
            error_message = str(e)
            logging.error(f"Ошибка при записи в Google Sheets: {error_message}")
            yield {'status': 'error', 'error': error_message}


def run(argv=None):
    """Основная функция пайплайна"""
    
    parser = argparse.ArgumentParser()
    # Основные параметры
    parser.add_argument('--api_key', required=True, help='Intercom API key')
    # Используем target_table вместо nps_table для совместимости с метаданными
    parser.add_argument('--target_table', required=True, help='BigQuery table for NPS data (project:dataset.table)')
    parser.add_argument('--spreadsheet_id', required=True, help='Google Spreadsheet ID for reviews')
    # Опциональные параметры
    parser.add_argument('--credentials_path', help='Path to service account credentials file for Google Sheets')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode (skip date filtering)')
    
    # Добавляем параметры, которые могут быть в логах и template
    parser.add_argument('--postgres_host', help='Not used, for compatibility only')
    parser.add_argument('--postgres_port', type=int, help='Not used, for compatibility only')
    parser.add_argument('--postgres_database', help='Not used, for compatibility only')
    parser.add_argument('--start_date', help='Override start date in ISO format (YYYY-MM-DDThh:mm:ssZ)')
    parser.add_argument('--end_date', help='Override end date in ISO format (YYYY-MM-DDThh:mm:ssZ)')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    # Вычисляем даты для выборки данных
    if known_args.start_date and known_args.end_date:
        start_date = known_args.start_date
        end_date = known_args.end_date
        logging.info(f"Используются переданные даты: с {start_date} по {end_date}")
    else:
        start_date, end_date = calculate_previous_month_dates()
    
    # Используем параметр target_table для совместимости
    nps_table = known_args.target_table
    
    # Извлекаем месяц из даты начала для логирования и фильтрации
    run_month = None if known_args.debug else datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ").strftime('%Y-%m')
    
    if run_month:
        logging.info(f"Запуск пайплайна NPS для месяца: {run_month}")
    else:
        logging.info("Запуск пайплайна NPS в режиме отладки (без фильтрации по месяцу)")

    with beam.Pipeline(options=pipeline_options) as p:
        # Регистрируем кодеры для пользовательских типов
        beam.coders.registry.register_coder(NPSData, beam.coders.RowCoder)
        beam.coders.registry.register_coder(ReviewData, beam.coders.RowCoder)
        
        # Создаем исходные данные - один элемент для запуска загрузки данных
        dummy_input = p | "Create Dummy Input" >> beam.Create([None])
        
        # Получаем данные из Intercom
        intercom_data = dummy_input | "Fetch Intercom Data" >> beam.ParDo(
            FetchIntercomData(
                known_args.api_key,
                start_date,
                end_date
            )
        )
        
        # Рассчитываем NPS и форматируем отзывы с фильтрацией по месяцу
        nps_and_reviews = intercom_data | "Calculate NPS" >> beam.ParDo(CalculateNPS(run_month=run_month))
        
        # Разделяем NPS данные и отзывы
        nps_data, reviews_data = nps_and_reviews | "Split NPS and Reviews" >> beam.Partition(
            lambda elem, _: 0 if elem[0] == 'nps' else 1, 2
        )
        
        # Форматируем данные NPS для записи в BigQuery
        nps_for_bq = nps_data | "Format NPS for BigQuery" >> beam.Map(lambda x: x[1]._asdict())
        
        # Записываем NPS данные в BigQuery
        nps_for_bq | "Write NPS to BigQuery" >> WriteToBigQuery(
            nps_table,
            schema=NPS_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
        
        # Форматируем отзывы для записи в Google Sheets
        reviews_for_sheets = reviews_data | "Format Reviews for Sheets" >> beam.Map(lambda x: x[1])
        
        # Логируем статистику отзывов
        reviews_count = reviews_for_sheets | "Count Reviews" >> beam.combiners.Count.Globally()
        reviews_count | "Log Reviews Count" >> beam.Map(lambda count: logging.info(f"Общее количество отзывов для записи: {count}"))
        
        # Собираем все отзывы в один список
        reviews_as_list = reviews_for_sheets | "Collect Reviews" >> beam.combiners.ToList()
        
        # Записываем отзывы в Google Sheets и логируем результат
        sheets_result = dummy_input | "Write Reviews to Sheets" >> beam.ParDo(
            WriteReviewsToSheets(
                known_args.spreadsheet_id, 
                known_args.credentials_path
            ),
            reviews_as_list=beam.pvalue.AsSingleton(reviews_as_list)
        )
        
        # Логируем результат записи
        sheets_result | "Log Sheets Result" >> beam.Map(
            lambda result: logging.info(f"Результат записи в Sheets: {result}")
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()