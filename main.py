from datetime import datetime, timedelta
from fastapi import FastAPI, Form, BackgroundTasks
# from celery import Celery
from starlette.responses import HTMLResponse
import time
from starlette.staticfiles import StaticFiles
from utils import *

"""Для запуска потребуется запустить докер-контейнер с Clickhouse
   Выполнив команду "docker run -d --name clickhouse-server -p 8123:8123 9000:9000 --ulimit nofile=262144:262144 yandex/clickhouse-server"
   redis так же можно запустить в контейнере
"""

app = FastAPI()
# celery = Celery('task', broker='redis://localhost:6379')
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def read_index():
    return HTMLResponse(content=open("static/index.html", "r").read(), media_type="text/html")


@app.post('/ozon')
async def ozon_data(background_tasks: BackgroundTasks, client_id: str = Form(...), api_key: str = Form(...)):
    today = datetime.today()
    start_date = today - timedelta(days=4)
    data = get_ozon_data(client_id, api_key, start_date, today)
    create_ozon_table()
    # test_data = {
    #       "result": [
    #         {
    #           "posting_number": "string",
    #           "etgb": {
    #             "number": "string",
    #             "date": "string",
    #             "url": "string"
    #           }
    #         }
    #       ]
    #     }
    # Сохранение данных в базу ClickHouse
    try:
        start_time = time.time()
        # Очередь можно реализовать через celery с брокером redis
        # save_to_clickhouse.delay(test_data)
        # И более простой вариант с помощью background_tasks, он создает отдельный поток для выполнения задач из очереди.
        background_tasks.add_task(save_to_clickhouse, data)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Время выполнения функции: {execution_time} секунд")
        return {"message": "Form submitted successfully"}
    except Exception as err:
        print(err)
        return {"mes": err}


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host='0.0.0.0', port=8000)
