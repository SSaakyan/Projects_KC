#импорт библиотек
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta

from airflow.decorators import dag, task

#для подключения к базе simulator_20221120:
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': password,
    'user': user,
    'database': 'simulator_20221120'
}

#параметры для дага
default_args = {
    'owner': 's.saakyan',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 15),
    'end_date': datetime(2023, 1, 31)
}

#график запуска дага
schedule_interval = '0 11 * * * *'

#chat_id:
chat_id = chat_num

#определяем бот:
bot = telegram.Bot(token=my_token)

#запрос данных для текстового сообщения:
text_query = """
SELECT
    toDate(time) as day,
    count(DISTINCT user_id) as DAU,
    countIf(user_id, action = 'view') as views,
    countIf(user_id, action = 'like') as likes,
    countIf(user_id, action = 'like') / countIf(user_id, action = 'view') as CTR
FROM simulator_20221120.feed_actions 
WHERE day = yesterday() 
GROUP BY day
"""

#запрос данных для графиков:
graph_query = """
SELECT toDate(time) as day,
    count(DISTINCT user_id) as DAU,
    countIf(user_id, action = 'view') as views,
    countIf(user_id, action = 'like') as likes,
    countIf(user_id, action = 'like') / countIf(user_id, action = 'view') as CTR
FROM simulator_20221120.feed_actions 
WHERE toDate(time) BETWEEN today() - 7 AND yesterday() 
GROUP BY day
""" 

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_report_saak():
    
    @task
    def reportbot():
        #настройка отправки текстового сообщения:
        df = ph.read_clickhouse(text_query, connection=connection)
        msg = '''Доброе утро, коллеги! Отчет за {day}:\nDAU: {DAU}\nViews: {views}\nLikes: {likes}\nCTR: {CTR:.2%}'''\
                .format(day=df.day[0].strftime('%d.%m.%Y'),
                        DAU=df.DAU[0],
                        views=df.views[0],
                        likes=df.likes[0],
                        CTR=df.CTR[0])  
        bot.sendMessage(chat_id=chat_id, text=msg)
        
        #настройка отправки графиков:
        df_gr = ph.read_clickhouse(graph_query, connection=connection)
        fig, axes = plt.subplots(2, 2, figsize=(23,17))
        axes_list = [item for sublist in axes for item in sublist]

        fig.suptitle('Данные за прошедшую неделю', fontsize=25, fontweight="bold")
        for name in ['DAU', 'CTR', 'views', 'likes']:
            ax = axes_list.pop(0)
            sns.set_theme(style="whitegrid")
            sns.lineplot(df_gr['day'], df_gr[name], ax=ax)
            ax.set_title(name, fontsize=21, fontweight="bold")
            ax.set_xlabel('Date', fontsize=12)
            ax.set_ylabel(name, fontsize=12)
    
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'report_gr.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
       
    reportbot()    
    
dag_report_saak = dag_report_saak()
    





