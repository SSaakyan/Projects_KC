from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import pandahouse as ph
from airflow.decorators import dag, task


#для подключения к базе simulator_20221120:
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': password,
    'user': user,
    'database': 'simulator_20221120'
}

#для подключения к базе test:
test_conn = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': password1,
    'user': user1,
    'database': 'test'
}

default_args = {
    'owner': 's.saakyan',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 12, 10),
    'end_date': datetime(2023, 1, 31)
}

schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_etl_saak():

    #из таблицы feed_actions считаем число просмотров и лайков для каждого юзера 
    @task()
    def extract_feed():
        q = """
        SELECT
            toDate(time) as event_date, 
            user_id, 
            os,
            age,
            gender,
            sum(action = 'view') as views,
            sum(action = 'like') as likes
        FROM 
            simulator_20221120.feed_actions 
        WHERE 
            toDate(time) = yesterday() 
        GROUP BY
            event_date,
            user_id,
            os,
            age,
            gender
        """
        df_feed = ph.read_clickhouse(q, connection=connection)
        return df_feed

    #из таблицы message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему
    @task()
    def extract_message():
        q = """
        SELECT
            user_id,
            event_date,
            messages_received,
            messages_sent,
            users_received,
            users_sent,
            os,
            age,
            gender
        FROM
            (SELECT
                toDate(time) as event_date,
                user_id,
                os,
                age,
                gender,
                count() as messages_sent,
                count(distinct reciever_id) as users_sent
            FROM
                simulator_20221120.message_actions
            WHERE
                toDate(time) = yesterday()
            GROUP BY
                event_date,
                user_id,
                os,
                age,
                gender) t1
            
            JOIN
            
            (SELECT
                reciever_id,
                count() as messages_received,
                count(distinct user_id) as users_received
            FROM
                simulator_20221120.message_actions
            WHERE
                toDate(time) = yesterday()
            GROUP BY
                toDate(time),
                reciever_id) t2
            ON t1.user_id = t2.reciever_id
        """
        df_message = ph.read_clickhouse(q, connection=connection)
        return df_message
    
    #объединяем две полученные таблицы
    @task
    def get_total_df(df_feed, df_message):
        df_total = df_feed.merge(df_message, how='outer')
        return df_total
    
    #расчёт метрик по os
    @task
    def transform_by_os(df_total):
        df_total_os = df_total.drop(['age','gender'], axis=1)\
            .groupby(['event_date', 'os'], as_index=False)\
            .sum()\
            .rename(columns = {'os': 'dimension_value'})
        df_total_os['dimension'] = 'os'
        return df_total_os
    
    #расчёт метрик по age
    @task
    def transfrom_by_age(df_total):
        df_total_age = df_total.drop(['os','gender'], axis=1)\
            .groupby(['event_date', 'age'], as_index=False)\
            .sum()\
            .rename(columns = {'age': 'dimension_value'})
        df_total_age['dimension'] = 'age'
        return df_total_age

    #расчёт метрик по gender
    @task
    def transfrom_by_gender(df_total):
        df_total_gender = df_total.drop(['os','age'], axis=1)\
            .groupby(['event_date', 'gender'], as_index=False)\
            .sum()\
            .rename(columns = {'gender': 'dimension_value'})
        df_total_gender['dimension'] = 'gender'
        return df_total_gender
    
    #объединяем срезы
    @task
    def result_table(df_total_os, df_total_age, df_total_gender):
        df_result = pd.concat([df_total_os, df_total_age, df_total_gender], ignore_index=True)
        df_result = df_result[['event_date','dimension','dimension_value','views','likes','messages_received','messages_sent','users_received','users_sent']]
        df_result[[i for i in df_result][3:]] = df_result[[i for i in df_result][3:]].astype('int64')
        return df_result
    
    #записываем полученные данные в отдельную таблицу 
    @task
    def load(df_result):
        query_result ="""
        CREATE TABLE IF NOT EXISTS test.saakyan_sv
        (event_date Date,
         dimension String,
         dimension_value String,
         views Int64,
         likes Int64,
         messages_received Int64,
         messages_sent Int64,
         users_received Int64,
         users_sent Int64
         ) ENGINE = MergeTree()
         ORDER BY event_date
         """
        ph.execute(query_result, connection=test_conn)
        ph.to_clickhouse(df_result,'saakyan_sv', connection=test_conn,index=False)



    df_feed = extract_feed()
    df_message = extract_message()
    df_total = get_total_df(df_feed, df_message)
    df_total_os = transform_by_os(df_total)
    df_total_age = transfrom_by_age(df_total)
    df_total_gender = transfrom_by_gender(df_total)
    df_result = result_table(df_total_os, df_total_age, df_total_gender)
    load(df_result)
    
dag_etl_saak = dag_etl_saak()
