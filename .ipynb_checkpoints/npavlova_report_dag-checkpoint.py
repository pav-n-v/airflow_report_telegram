import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
from datetime import datetime, timedelta
import pandahouse as ph
import os
from dotenv import load_dotenv

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


def report_telegram(df, chat=None): 
    # Сетка для графиков
    sns.set_style("whitegrid")
    # Функция для создания и сохранения графиков в буфер
    def create_graph(df, metric, yesterday, week): 
        plt.figure(figsize=(8,6))
        plt.title(metric + ' for ' + week +' - ' + yesterday )
        sns.lineplot(x = df.date, y = df[metric])
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = metric + '.png'
        plt.close()
        return plot_object
    chat_id = chat or 447391757
    bot = telegram.Bot(token=my_token)
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%d.%m.%Y')
    week = (datetime.today() - timedelta(days=7)).strftime('%d.%m.%Y')
    #ключевые метрики за вчера
    dau = df.DAU[df.shape[0]- 1]
    views = df.views[df.shape[0]- 1]
    likes = df.likes[df.shape[0]- 1]
    ctr = df.CTR[df.shape[0]- 1]

    # Отправка текстового сообщения
    msg = f'''Ключевые метрики за *{yesterday}*:
    *DAU* - {dau}
    *Просмотры* - {views}
    *Лайки* - {likes}
    *CTR* - {ctr}'''
    bot.sendMessage(chat_id=chat_id, text=msg, parse_mode='Markdown') 

    media_group = []

    # Создание массива с графиками для send_media_group()
    for metric in df.columns[1:]:
        media_group.append(telegram.InputMediaPhoto(media=create_graph(df, metric, yesterday, week),
                                                  caption=metric + ' for ' + week +' - ' + yesterday ))
    # Отправка группы медиа    
    bot.sendMediaGroup(chat_id=chat_id, media=media_group)


load_dotenv()
connection = os.getenv('CONNECTION_DB_SIM')
my_token = os.getenv('TOKEN_TG')



# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'n.pavlova',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 4, 19),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def npavlova_report_dag():

    @task()
    def extract():
        q = '''
        SELECT  toDate(time) as date,
                uniq(user_id) as DAU,
                countIf(action, action = 'view') as views,
                countIf(action, action = 'like') as likes,
                round(likes/views, 2) as CTR
        FROM simulator_20230320.feed_actions 
        WHERE date BETWEEN today() - 7 AND  yesterday() 
        GROUP BY date
        ORDER BY date
        '''
        df = ph.read_clickhouse(query=q, connection=connection)
        return df
        
    
    @task()
    def load(df):
        print('extr ok')
        report_telegram(df, chat = -802518328)
        
        
    df = extract()
    load(df)
    
npavlova_report_dag = npavlova_report_dag()
