import airflow

import pandas as pd
from pendulum import datetime, from_format, duration
from airflow import DAG
from contextlib import closing
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from pymongo.write_concern import WriteConcern
from service_files.feature_creator_old_2 import FeatureCreator
import json


with open('./dags/service_files/currencies.json', 'r+') as f:
    currencies = json.load(f)

with open('./dags/service_files/features.json', 'r+') as f:
    features = json.load(f)


class SkipConflictPostgresHook(PostgresHook):
    def insert_rows(self, table, rows, target_fields=None, commit_every=1000,
                    replace=False, resolve_conflict=None, **kwargs):
        """
        A generic way to insert a set of tuples into a table,
        a new transaction is created every commit_every rows
        :param table: Name of the target table
        :param rows: The rows to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :param replace: Whether to replace instead of insert
        """
        i = 0
        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, False)

            conn.commit()

            with closing(conn.cursor()) as cur:
                for i, row in enumerate(rows, 1):
                    lst = []
                    for cell in row:
                        lst.append(self._serialize_cell(cell, conn))
                    values = tuple(lst)
                    sql = self._generate_insert_sql(table, values, target_fields, replace, **kwargs)
                    if resolve_conflict:
                        sql += f' ON CONFLICT ({resolve_conflict}) DO NOTHING'
                    self.log.debug("Generated sql: %s", sql)
                    # print(sql)
                    cur.execute(sql, values)
                    if commit_every and i % commit_every == 0:
                        conn.commit()
                        self.log.info("Loaded %s rows into %s so far", i, table)

            conn.commit()
        self.log.info("Done loading. Loaded a total of %s rows into %s", i, table)


def fn_mongo_select_star(**context):
    """ TO DO IN MongoHook. Connect to collection by ticker and return all data. """
    try:
        hook = MongoHook(conn_id='mongodb_conn')
        client = hook.get_conn()
        db = client.investing
        if context['table'] in db.list_collection_names():
            collection = db[context['table']]
            last_time_features = db[context['f_table']].find(sort=[("time", -1)]).limit(1)[0]['time']
            delta = collection.count_documents({'time': {'$gt': last_time_features}})

            request_mongo = collection.find(sort=[("time", -1)]).limit(1224+delta)
            data = [[d['time'], d['open'], d['high'], d['low'], d['close'], d['volume']] for d in request_mongo]
            return json.dumps(data)

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")


def fn_get_subdata():
    try:
        hook = MongoHook(conn_id='mongodb_conn')
        client = hook.get_conn()
        db = client.investing
        subdata = {}
        for t in currencies + features:
            if t in db.list_collection_names():
                collection = db[t]
                request_mongo = list(collection.find({}, projection={'_id': False}))
                print(request_mongo)
                data = [[d['time'], d['close']] for d in request_mongo]
                subdata[t] = data
        return json.dumps(subdata)

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")


def fn_merge_and_fillna(**context):
    data = json.loads(context['data'])
    data = pd.DataFrame.from_dict(dict(zip(['time', 'open', 'high', 'low', 'close', 'volume'], list(zip(*data)))))
    subdata = json.loads(context['subdata'])
    subdata = {k: list(zip(*v)) for k, v in subdata.items()}
    subdata = {k: pd.DataFrame.from_dict(dict(zip(['time', 'close'], v))) for k, v in subdata.items()}

    for k, sub_v in subdata.items():
        data = data.merge(sub_v, how='left', on='time', suffixes=('', f'_{k}'))
    print(data.info())
    # data = data.interpolate(method='nearest', limit_direction='both')
    data = data.interpolate(mothod='linear', limit_direction='both')
    print(data.info())
    data = data.to_numpy().tolist()
    return json.dumps(data)


def fn_generate_features(**context):
    data = json.loads(context['data'])
    data = dict(zip(['time', 'open', 'high', 'low', 'close', 'volume',
                     'close_s1', 'close_s2', 'close_s3', 'close_s4'], list(zip(*data))))
    df = pd.DataFrame.from_dict(data)
    df['time'] = pd.to_datetime(df['time'])
    df = df.iloc[::-1]
    # print(df)
    features_creator = FeatureCreator(df)
    # features = features_creator.generate_feature().to_numpy().tolist()
    features = features_creator.generate_feature()
    features['time'] = features['time'].dt.strftime('%Y-%m-%d %H:00:00')
    features = features.to_numpy().tolist()
    # print(len(features))
    shape = len(features[0])
    # print(shape)
    context['task_instance'].xcom_push(key='shape_features', value=shape)
    return json.dumps(features)


def fn_postgres_load_new_features(**context):
    hook = SkipConflictPostgresHook(postgres_conn_id=context['postgres_conn_id'])
    data = json.loads(context['features'])
    hook.insert_rows(table=context['table'],
                     rows=data,
                     resolve_conflict='time')


def fn_mongodb_load_new_feature(**context):
    try:
        hook = MongoHook(conn_id='mongodb_conn')
        client = hook.get_conn()
        db = client.investing
        data = json.loads(context['features'])
        # print(f'DATA: {data}')
        shape = context['task_instance'].xcom_pull(
            task_ids='generate_new_features', key='shape_features'
        )
        to_insert = list(
            map(lambda x: dict(zip(['time', *[f'f{i}' for i in range(1, shape)]], x)), data))
        # print(len(to_insert))
        collection = db[context['table']]
        collection.with_options(write_concern=WriteConcern(w=0)).insert_many(to_insert, ordered=False)

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")


default_args = {'start_date': datetime(2022, 12, 2, 15, tz="Europe/Moscow"),
                'retries': 3,
                'retry_delay': duration(minutes=1),}

for ticker in ['sber', 'gazp', 'lkoh']:
    with DAG(
            dag_id=f'006_{ticker}_gen_features',
            default_args=default_args,
            schedule_interval='0 10-18 * * 1-5',
            catchup=False,
            params={'table': ticker}
    ) as dag:
        previous_dag = ExternalTaskSensor(
            task_id='external_task',
            external_dag_id=f'005_{ticker}_parse_data',
            external_task_ids=['mongodb_load_data', 'postgres_load_data'],
            timeout=300)

        mongo_select_star = PythonOperator(
            task_id="mongo_select_star",
            python_callable=fn_mongo_select_star,
            op_kwargs={'table': ticker,
                       'f_table': f'{ticker}_features'}, )
        get_subdata = PythonOperator(
            task_id="get_subdata",
            python_callable=fn_get_subdata, )
        merge_and_fillna = PythonOperator(
            task_id='merge_and_fillna',
            python_callable=fn_merge_and_fillna,
            op_kwargs={'data': mongo_select_star.output,
                       'subdata': get_subdata.output}
        )

        generate_new_features = PythonOperator(
            task_id="generate_new_features",
            python_callable=fn_generate_features,
            op_kwargs={"data": merge_and_fillna.output}, )

        postgres_load_new_features = PythonOperator(
            task_id="postgres_load_new_features",
            python_callable=fn_postgres_load_new_features,
            op_kwargs={'postgres_conn_id': 'postgres_conn',
                       'table': f'{ticker}_features',
                       'features': generate_new_features.output})

        mongo_load_new_features = PythonOperator(
            task_id="mongo_load_new_features",
            python_callable=fn_mongodb_load_new_feature,
            op_kwargs={'table': f'{ticker}_features',
                       'features': generate_new_features.output}, )

        previous_dag >> mongo_select_star >> get_subdata >> merge_and_fillna >> generate_new_features
        generate_new_features >> [postgres_load_new_features, mongo_load_new_features]
