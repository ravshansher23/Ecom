import requests
from clickhouse_driver import Client
from google.protobuf.timestamp_pb2 import Timestamp
from decor.decor import time_decorator


class ClickhouseConnection:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self.client = Client(host='localhost', port=9000)

    def execute_query(self, query):
        return self.client.execute(query)


# Создаем экземпляр класса ClickhouseConnection
clickhouse_connection = ClickhouseConnection()

"""Получение данных от API Ozon"""


def get_ozon_data(client_id, api_key, start_date, end_date):
    url = "https://api-seller.ozon.ru/v1/posting/global/etgb"
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key
    }

    post_data = {
        "date": {
            "from": time_to_ozon(start_date),
            "to": time_to_ozon(end_date)
        }
    }

    ozon_data = requests.post(url=url, headers=headers, json=post_data)

    return ozon_data.json()


"""Для запросов к API необходимо преобразовать время к формату timestamp_pb2 """


def time_to_ozon(time):
    timestamp = Timestamp()
    timestamp.FromDatetime(time)
    return timestamp.ToJsonString()


# @celery.task
"""Функция сохранения полученного результата в БД"""


@time_decorator
def save_to_clickhouse(data):
    client = Client(host='localhost', port=9000)

    for item in data["result"]:
        posting_number = item["posting_number"]
        etgb = item["etgb"]
        etgb_number = etgb["number"]
        etgb_date = etgb["date"]
        etgb_url = etgb["url"]

        query = f"SELECT COUNT(*) FROM ozon_table WHERE posting_number = '{posting_number}' AND etgb_number = '{etgb_number}' AND etgb_date = '{etgb_date}' AND etgb_url = '{etgb_url}'"
        result = clickhouse_connection.execute_query(query)
        count = result[0][0]

        if count == 0:
            # Если дублей нет, выполняем INSERT
            query = f"INSERT INTO ozon_table (posting_number, etgb_number, etgb_date, etgb_url) VALUES ('{posting_number}', '{etgb_number}', '{etgb_date}', '{etgb_url}')"
            clickhouse_connection.execute_query(query)
        else:
            print(f"Дубликат данных: {posting_number}, {etgb_number}, {etgb_date}, {etgb_url}")


"""Создание таблицы"""


def create_ozon_table():
    client = Client(host='localhost', port=9000)

    clickhouse_connection.execute_query(
        'CREATE TABLE IF NOT EXISTS ozon_table (posting_number String, etgb_number String, etgb_date String, etgb_url String) ENGINE = MergeTree() ORDER BY (etgb_date)')
