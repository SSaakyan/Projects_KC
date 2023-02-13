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
schedule_interval = '0 11 * * *'


#запрос данных по действиям:
q_action = """
SELECT toDate(day) AS day_,
        sum(like) AS likes,
        sum(view) AS views,
        sum(message) AS messages_sent
FROM 
    (SELECT * 
    FROM 
        (SELECT toDate(time) as day,
                user_id,
                countIf(user_id, action='like') as like,
                countIf(user_id, action='view') as view
        FROM {db}.feed_actions
        GROUP BY day,user_id) t1
                 
    FULL JOIN 
                 
        (SELECT toDate(time) as day,
                user_id,
                count(reciever_id) AS message
        FROM {db}.message_actions
        GROUP BY day, user_id) t2
     USING user_id) t3
WHERE toDate(day) > '1970-01-01'    
GROUP BY toDate(day)
"""

#Новые пользователи приложения
q_new_users = """
SELECT start_date as day_, COUNT(user_id) as new_users
FROM
(SELECT user_id, MIN(date) as start_date
from
(select DISTINCT user_id, toDate(time) as date
from {db}.feed_actions
UNION ALL
select DISTINCT user_id, toDate(time) as date
from {db}.message_actions)
GROUP by user_id
)
GROUP BY start_date
"""


#Новые посты:
q_new_posts = """
SELECT COUNT(post_id) as new_posts, post_day as day_
FROM
(SELECT post_id, min(toDate(time)) AS post_day
FROM {db}.feed_actions
GROUP BY post_id)
GROUP BY post_day
"""

#DAU:
q_dau = """
        SELECT
            t1.date_ as day_,
            DAU_message,
            DAU_feed,
            DAU_all_services
        FROM 
            (
                SELECT COUNT(user_id) as DAU_message, date_
                FROM
                    (
                    SELECT distinct user_id, toDate(time) as date_
                    FROM simulator_20221120.message_actions
                    EXCEPT
                    SELECT distinct user_id, toDate(time) as date_
                    FROM simulator_20221120.feed_actions
                    )
                    GROUP BY date_ 
                    ) as t1
            JOIN
            (
                SELECT COUNT(user_id) as DAU_feed, date_
                FROM
                (
                    SELECT distinct user_id, toDate(time) as date_
                    FROM simulator_20221120.feed_actions
                    EXCEPT
                    SELECT distinct user_id, toDate(time) as date_
                    FROM simulator_20221120.message_actions
                    )
                GROUP BY date_ 
                ) as t2
            ON  t1.date_=t2.date_
            JOIN
            (
                SELECT
                    toDate(time) AS date_,
                    COUNT(DISTINCT user_id) AS DAU_all_services
                FROM simulator_20221120.feed_actions t1
                FULL JOIN simulator_20221120.message_actions t2 
                ON t1.user_id = t2.user_id
                WHERE date_ > '1970-01-01'
                GROUP BY toDate(time)) as t3
            ON t2.date_=t3.date_ 
"""
#аудитория по неделям:
stat = """
        WITH all_table as (
        SELECT time,
            user_id
          FROM simulator_20221120.feed_actions t1
              FULL JOIN simulator_20221120.message_actions t2 
              ON t1.user_id = t2.user_id
         WHERE toDate(time) > '1970-01-01'
         GROUP BY time, user_id
        )
        select this_week,
            previous_week,
            -uniq(user_id) as num_users,
            status
        from

        (select user_id,
        groupUniqArray(toMonday(toDate(time))) as weeks_visited,
        addWeeks(arrayJoin(weeks_visited), +1) this_week,
        if(has(weeks_visited, this_week) = 1, 'retained', 'gone') as status,
        addWeeks(this_week, -1) as previous_week
        from all_table
        group by user_id)

        where status = 'gone'

        group by this_week, previous_week, status

        having this_week != addWeeks(toMonday(today()), +1)

        union all

        select this_week,
            previous_week,
            toInt64(uniq(user_id)) as num_users,
            status
        from
        (select user_id,
        groupUniqArray(toMonday(toDate(time))) as weeks_visited,
        arrayJoin(weeks_visited) this_week,
        if(has(weeks_visited, addWeeks(this_week, -1)) = 1, 'retained', 'new') as status,
        addWeeks(this_week, -1) as previous_week
        from all_table
        group by user_id)

        group by this_week, previous_week, status
"""
#топ-10 постов:
query_file = """
        SELECT post_id AS post_id,
               countIf(action='view') AS views,
               countIf(action='like') AS likes,
               countIf(action='like') / countIf(action='view') AS "CTR",
               count(DISTINCT user_id) AS uniq_users
        FROM {db}.feed_actions
        GROUP BY post_id
        ORDER BY views DESC
        LIMIT 10
        """ 
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_report2_saak():
    
     
    @task
    def send_bot():
        
        #чат для отправки отчёта:
        chat_id = chat_num
        
        #определяем бота:
        bot = telegram.Bot(token=mytoken)
        
        action = ph.read_clickhouse(q_action, connection=connection)
        new_users = ph.read_clickhouse(q_new_users, connection=connection)
        new_posts = ph.read_clickhouse(q_new_posts, connection=connection)
        df_dau = ph.read_clickhouse(q_dau, connection=connection)
        
        #объединяем таблицы:
        df = action.merge(new_users, how='outer')\
            .merge(new_posts, how='outer')\
            .merge(df_dau, how='outer')
             
        
        #берём данные за вчерашний день:
        x = date.today()-timedelta(days=1)
        df_msg = df.query('day_ == @x')
                
        #отправляем сообщение:
        msg = '''Доброго времени суток, коллеги! Отчет по приложению за {day}:\nНовые пользователи: {new_users}\nDAU: {dau}\nНовые посты: {new_posts}\nViews: {views}\nLikes: {likes}\nДашборд : https://redash.lab.karpov.courses/dashboards/2963-report-dashboard'''\
                .format(day=df_msg.day_.dt.strftime('%d.%m.%Y').iloc[0],
                        dau=df_msg.DAU_all_services.iloc[0],
                        views=df_msg.views.iloc[0],
                        likes=df_msg.likes.iloc[0],
                        new_users=df_msg.new_users.iloc[0],
                        new_posts=df_msg.new_posts.iloc[0]) 
        bot.sendMessage(chat_id=chat_id, text=msg)
        
        
        #для графика Audience by weeks
        df_stat = ph.read_clickhouse(stat,connection=connection)
        dm = df_stat.groupby(['this_week','status']).agg({'num_users': 'sum'}).reset_index()
        
        #рисуем графики
        fig, axes = plt.subplots(3, 1, figsize=(21,23))
        fig.suptitle('Application report', fontsize=25, fontweight="bold") 
        
        #график DAU
        axes[0].set_title('DAU', fontsize=20, fontweight="bold")
        for name in ['DAU_message', 'DAU_feed', 'DAU_all_services']:
            ax=axes[0]
            sns.lineplot(x=df['day_'], y=df[name], label=name, ax=ax)
            ax.set_xlabel('date')
            ax.set_ylabel('DAU')
        axes[0].set_xticks(df['day_'])
        for ind, label in enumerate(axes[0].set_xticklabels(labels=df['day_'].dt.strftime('%d/%m/%Y'), rotation=15)):
            if ind % 7 == 0:
                label.set_visible(True)
            else:
                label.set_visible(False)

        #график Actions
        axes[1].set_title('Actions', fontsize=20, fontweight="bold")
        for name in ['likes', 'views', 'messages_sent']:
            ax=axes[1]
            sns.lineplot(x=df['day_'], y=df[name], label=name, ax=ax)
            ax.set_xlabel('date')
            ax.set_ylabel('Quantity')
        axes[1].set_xticks(df['day_'])
        for ind, label in enumerate(axes[1].set_xticklabels(labels=df['day_'].dt.strftime('%d/%m/%Y'), rotation=15)):
            if ind % 7 == 0:
                label.set_visible(True)
            else:
                label.set_visible(False)
        
        #график Audience by weeks
        axes[2].set_title('Audience by weeks', fontsize=20, fontweight="bold")
        sns.barplot(data=dm, x="this_week", y="num_users", hue="status", ax=axes[2])
        axes[2].set_xlabel('week')
        axes[2].set_ylabel('Number of users')
        axes[2].set_xticklabels(labels=dm.groupby('this_week').agg({'num_users':'sum'}).reset_index()['this_week'].dt.strftime('%d/%m/%Y'), rotation=15)

        #отправляем графики
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'report_gr.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        


        #данные по топ 10 постам по количеству просмотров:
        top_posts = ph.read_clickhouse(query_file, connection=connection)
        
        #отправляем файл
        file_object = io.StringIO()
        top_posts.to_csv(file_object)
        file_object.seek(0)
        file_object.name = 'top_posts.csv'
        bot.send_document(chat_id=chat_id, document=file_object)
    
        
    send_bot()
       
    
dag_report2_saak = dag_report2_saak()