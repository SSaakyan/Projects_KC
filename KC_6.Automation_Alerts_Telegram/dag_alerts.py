#импорт библиотек
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import date, datetime, timedelta

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
schedule_interval = '*/15 * * * *'

#запрос данных из базы:
query = '''
SELECT 
    ts, date, hm, users_feed, views, likes, CTR, users_messenger, number_messages 
FROM
    (SELECT 
        toStartOfFifteenMinutes(time) as ts,
        toDate(time) as date,
        formatDateTime(ts,'%R') as hm,
        uniqExact(user_id) as users_feed,
        countIf(action='view') as views,
        countIf(action='like') as likes,
        round(countIf(action='like') / countIf(action='view'),3) as CTR
    FROM simulator_20221120.feed_actions
    WHERE time >= yesterday() and time < toStartOfFifteenMinutes(now())
    GROUP BY ts, date, hm) t1
    LEFT JOIN
    (SELECT 
        toStartOfFifteenMinutes(time) as t_m,
        uniqExact(user_id) as users_messenger,
        count(reciever_id) as number_messages
    FROM simulator_20221120.message_actions
    GROUP BY t_m) t2
    ON t1.ts = t2.t_m
ORDER BY ts
'''
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_alerts_saak():
    
    @task
    def get_alerts():
        
        def check_anomaly(df,metric, a=3, n=4):
            #проверяем наличие аномалий по правилу 3х сигм
            df['mean'] = df[metric].shift(1).rolling(n).mean()
            df['std'] = df[metric].shift(1).rolling(n).std()
            df['up'] = df['mean'] + a * df['std']
            df['low'] = df['mean'] - a * df['std']

            # сглаживаем границы
            df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
            df['low'] = df['low'].rolling(n, center=False, min_periods=1).mean()

            if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
                is_alert = 1
            else:
                is_alert = 0

            return is_alert, df

    
        def run_alerts():  
            #настраиваем оповещения
            data = ph.read_clickhouse(query, connection=connection)
            data.rename(columns={'users_feed':'Аудитория ленты', 'views':'Просмотры', 'likes':'Лайки', \
                                 'users_messenger':'Аудитория мессенджера', 'number_messages':'Число отправленных сообщений'}, inplace=True)
            metrics_list = ['Аудитория ленты', 'Просмотры', 'Лайки', 'CTR', 'Аудитория мессенджера', 'Число отправленных сообщений']
            
            #chat_id:
            chat_id = chat_num

            #определяем бот:
            bot = telegram.Bot(token=my_token)
            
            for metric in metrics_list:
                df = data[['ts','date','hm',metric]].copy()
                is_alert, df = check_anomaly(df,metric)

                if is_alert == 1:
                    msg = f"Внимание!Метрика: {metric}:\n- текущее значение {df[metric].iloc[-1]:.2f};\n- отклонение от предыдущего значения {abs(1-df[metric].iloc[-1]/df[metric].iloc[-2]):.2%}\nДашборд : http://superset.lab.karpov.courses/r/2687"
                    #график
                    sns.set(rc={'figure.figsize':(16,14)})
                    plt.tight_layout()
                    sns.set_theme(style="whitegrid")
                    ax = sns.lineplot(x = df['ts'],y = df[metric],label='metric')
                    ax = sns.lineplot(x = df['ts'],y = df['up'],label='upper bound')
                    ax = sns.lineplot(x = df['ts'],y = df['low'],label='lower bound')

                    ax.set_xticks(df['ts'])
                    for ind, label in enumerate(ax.set_xticklabels(labels=df['ts'].dt.strftime('%d/%m %H:%M'), rotation=45)):
                        if ind % 7 == 0:
                            label.set_visible(True)
                        else:
                            label.set_visible(False)
    
                    ax.set_title(metric, fontsize=19, fontweight="bold")
                    ax.set(xlabel='дата и время')
                    ax.set(ylabel=metric)
                                  
                    plot_object = io.BytesIO()
                    plt.savefig(plot_object)
                    plot_object.seek(0)
                    plot_object.name = 'plot_alerts.png'
                    plt.close()

                    #отправляем отчет боту
                    bot.sendMessage(chat_id=chat_id, text=msg)
                    bot.sendPhoto(chat_id=chat_id,photo=plot_object) 

        run_alerts()

    get_alerts()
    
dag_alerts_saak = dag_alerts_saak()
