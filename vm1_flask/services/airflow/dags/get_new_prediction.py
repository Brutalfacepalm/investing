import airflow

from pendulum import datetime, from_format, duration
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.providers.mongo.hooks.mongo import MongoHook
from pymongo.write_concern import WriteConcern
import json
import pandas as pd
import numpy as np


def fn_mongo_select_star(**context):
    """ TO DO IN MongoHook. Connect to collection by ticker and return all data. """
    try:
        hook = MongoHook(conn_id='mongodb_conn')
        client = hook.get_conn()
        db = client.investing
        if context['p_table'] in db.list_collection_names():
            collection = db[context['f_table']]
            last_time_features = db[context['p_table']].find(sort=[("time", -1)]).limit(1)[0]['time']
            # print(last_time_features)
            delta = collection.count_documents({'time': {'$gt': last_time_features}})
            # print(delta)

            request_mongo = list(collection.find(sort=[("time", -1)],
                                            projection={'_id': False}).limit(200 + delta))

            context['task_instance'].xcom_push(key='delta_data', value=delta)
            return json.dumps(request_mongo)

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")


def fn_get_new_prediction(**context):
    """TO DO PREDICTION PROCESS"""
    delta = context['task_instance'].xcom_pull(task_ids='mongo_select_star', key='delta_data')
    features = json.loads(context['features'])
    features_df = pd.DataFrame.from_dict(features)

    times = features_df.iloc[:delta, 0].values
    closes = features_df.iloc[:delta, 4].values * 0.01
    t_delta = np.ones_like(times) * 11
    lh = np.ones_like(times) * 0.87
    """TO DO PREDICTION PROCESS"""

    to_insert = [dict(zip(['time', 'predict', 't_delta', 'lh'], t_d)) for t_d in
                 list(zip(times, closes, t_delta, lh))]

    return json.dumps(to_insert)


def fn_mongo_load_new_prediction(**context):
    try:
        hook = MongoHook(conn_id='mongodb_conn')
        client = hook.get_conn()
        db = client.investing

        to_insert = json.loads(context['to_insert'])

        if context['table'] in db.list_collection_names():
            collection = db[context['table']]
            collection.with_options(write_concern=WriteConcern(w=0)).insert_many(to_insert, ordered=False)

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")


default_args = {'start_date': datetime(2022, 12, 2, 15, tz="Europe/Moscow"),
                'retries': 3,
                'retry_delay': duration(minutes=1),}


for ticker in ['sber', 'gazp', 'lkoh']:
    with DAG(
            dag_id=f'007_{ticker}_get_predict',
            default_args=default_args,
            schedule_interval='0 10-18 * * 1-5',
            catchup=False,
            params={'table': ticker}
    ) as dag:
        previous_dag = ExternalTaskSensor(
            task_id='external_task',
            external_dag_id=f'006_{ticker}_gen_features',
            external_task_ids=['mongo_load_new_features'],
            timeout=300)

        mongo_select_star = PythonOperator(
            task_id="mongo_select_star",
            python_callable=fn_mongo_select_star,
            op_kwargs={"f_table": f'{ticker}_features',
                       "p_table": f'{ticker}_predictions'}, )

        get_new_prediction = PythonOperator(
            task_id="get_new_prediction",
            python_callable=fn_get_new_prediction,
            op_kwargs={'table': f'{ticker}_predictions',
                       'features': mongo_select_star.output}, )

        mongo_load_new_prediction = PythonOperator(
            task_id="mongo_load_new_prediction",
            python_callable=fn_mongo_load_new_prediction,
            op_kwargs={'table': f'{ticker}_predictions',
                       'to_insert': get_new_prediction.output})

        previous_dag >> mongo_select_star >> get_new_prediction >> mongo_load_new_prediction
