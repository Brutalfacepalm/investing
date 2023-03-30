from pendulum import datetime, from_format, duration
from pendulum.time import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd
import numpy as np
import pymongo
from pymongo.write_concern import WriteConcern
from contextlib import closing

from service_files.feature_creator import FeatureCreator
from service_files.parser_data import Parser
import json

# with open('./dags/service_files/shares.json', 'r+') as f:
#     shares = json.load(f)

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


def fn_mongodb_create_collection(**context):
    try:
        hook = MongoHook(conn_id='mongodb_conn')
        client = hook.get_conn()
        db = client.investing
        if context['params']['table'] in db.list_collection_names():
            db.drop_collection(context['params']['table'])

        db.create_collection(name=context['params']['table'], capped=True, size=2128 * 1680, max=1680)
        collection = db[context['params']['table']]
        collection.create_index([("time", pymongo.DESCENDING)],
                                background=True,
                                unique=True)

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")


def fn_get_metadata():
    try:
        hook = MongoHook(conn_id='mongodb_conn')
        client = hook.get_conn()
        db = client.investing
        if 'meta' in db.list_collection_names():
            collection = db['meta']
            meta_data = list(collection.find({}, projection={'_id': False}))
            return json.dumps(meta_data)

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")


def fn_parse_data(execution_date, **context):
    """ Define start_date how min(postgres_date, mongodb_date) and end_date how Today() for Parser. """
    meta = json.loads(context['meta'])
    meta = pd.DataFrame.from_dict(dict(zip(['id', 'name', 'code', 'market', 'url'],
                                           list(zip(*[list(v.values()) for v in meta])))))
    t = context['params']['table']
    date_start = '01.01.2012 00:00:00'
    date_end = execution_date.in_timezone("Europe/Moscow").add(days=-1).strftime("%d.%m.%Y 23:00:00")

    parser_ticker = Parser(t.upper(),
                           date_start,
                           date_end,
                           split_period='year',
                           is_feature=False,
                           meta_df=meta)
    data_as_df = parser_ticker.parse()
    return json.dumps(data_as_df)


def fn_get_subdata(**context):
    subdata = {}
    hook = PostgresHook(postgres_conn_id=context['postgres_conn_id'])

    for t in currencies + features:
        sql = context['sql'].format(t)
        select_all = hook.get_records(sql=sql)
        select_all = list(map(lambda x: [x[0].strftime("%Y-%m-%d %H:00:00"), x[1]], select_all))
        subdata[t] = select_all

    return json.dumps(subdata)


def fn_postgres_load_data(**context):
    """ TO DO IN PostgresOperator and INSERT all new data """
    hook = SkipConflictPostgresHook(postgres_conn_id=context['postgres_conn_id'])
    data = json.loads(context['parse_data'])
    # if len(data) > 5000:
    #     for start_i in range(0, len(data), 5000):
    #         end_i = min(start_i + 5000, len(data))
    #         batch_data = data[start_i: end_i]
    #         hook.insert_rows(table=context['table'],
    #                          rows=batch_data,
    #                          resolve_conflict='time')
    # else:
    hook.insert_rows(table=context['table'],
                     rows=data,
                     resolve_conflict='time')


def fn_mongodb_load_data(**context):
    """ TO DO IN MongoHook and INSERT all new data """
    try:
        hook = MongoHook(conn_id='mongodb_conn')
        client = hook.get_conn()
        db = client.investing
        data = json.loads(context['parse_data'])
        to_insert = list(
            map(lambda x: dict(zip(['time', 'open', 'high', 'low', 'close', 'volume',
                                    'close_s1', 'close_s2', 'close_s3', 'close_s4'], x)), data))

        collection = db[context['params']['table']]
        collection.with_options(write_concern=WriteConcern(w=0)).insert_many(to_insert, ordered=False)

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")


def fn_merge_and_fillna(**context):
    data = json.loads(context['data'])
    data = pd.DataFrame.from_dict(dict(zip(['time', 'open', 'high', 'low', 'close', 'volume'],
                                           list(zip(*data)))))
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
    data = json.loads(context['parse_data'])
    to_df = dict(zip(['time', 'open', 'high', 'low', 'close', 'volume',
                      'close_s1', 'close_s2', 'close_s3', 'close_s4'], list(zip(*data))))
    df = pd.DataFrame.from_dict(to_df)
    df['time'] = pd.to_datetime(df['time'])
    features_creator = FeatureCreator(df)
    generated_features = features_creator.generate_feature()
    generated_features['time'] = generated_features['time'].dt.strftime('%Y-%m-%d %H:00:00')
    generated_features = generated_features.to_numpy().tolist()

    shape = len(generated_features[0])
    context['task_instance'].xcom_push(key='shape_features', value=shape)
    len_df = len(generated_features)
    for b_d_i, start_i in enumerate(range(0, len_df, ((len_df // 8 + 1) * 8) // 8)):
        end_i = min(start_i + ((len_df // 8 + 1) * 8) // 8, len_df)
        print(start_i, end_i)
        batch_data = generated_features[start_i:end_i]
        context['task_instance'].xcom_push(key=f'batch_data_{b_d_i}', value=batch_data)

    context['task_instance'].xcom_push(key='shape_features', value=shape)
    # return json.dumps(generated_features)


def fn_postgres_create_table_features(**context):
    hook = PostgresHook(postgres_conn_id=context['postgres_conn_id'])
    shape = context['task_instance'].xcom_pull(
        task_ids='generate_features', key='shape_features'
    )

    table = context['table']
    sql = context['sql'].format(table, table, 'time timestamp UNIQUE NOT NULL, ' + ', '.join([f'f{i} float' for i in range(1, shape)]))
    print(sql)
    hook.run(sql=sql)


def fn_mongodb_create_collection_features(**context):
    try:
        hook = MongoHook(conn_id='mongodb_conn')
        client = hook.get_conn()
        db = client.investing
        if context['table'] in db.list_collection_names():
            db.drop_collection(context['table'])

        db.create_collection(name=context['table'], capped=True, size=213408 * 400, max=400)
        collection = db[context['table']]
        collection.create_index([("time", pymongo.DESCENDING)],
                                background=True,
                                unique=True)

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")


def fn_postgres_load_feature(**context):
    hook = SkipConflictPostgresHook(postgres_conn_id=context['postgres_conn_id'])
    # data = json.loads(context['features'])
    data = []
    for b_d_i in range(8):
        batch_data = context['task_instance'].xcom_pull(task_ids='generate_features',
                                                        key=f'batch_data_{b_d_i}')
        data += batch_data
    # hook.insert_rows(table=context['table'],
    #                  rows=data)

    # if len(data) > 5000:
    #     for start_i in range(0, len(data), 5000):
    #         end_i = min(start_i + 5000, len(data))
    #         batch_data = data[start_i: end_i]
    #         hook.insert_rows(table=context['table'],
    #                          rows=batch_data,
    #                          resolve_conflict='time')
    # else:
    hook.insert_rows(table=context['table'],
                     rows=data,
                     resolve_conflict='time')


def fn_mongodb_load_features(**context):
    try:
        hook = MongoHook(conn_id='mongodb_conn')
        client = hook.get_conn()
        db = client.investing
        # data = json.loads(context['features'])
        data = []
        for b_d_i in range(8):
            batch_data = context['task_instance'].xcom_pull(task_ids='generate_features',
                                                            key=f'batch_data_{b_d_i}')
            data += batch_data

        shape = context['task_instance'].xcom_pull(
            task_ids='generate_features', key='shape_features'
        )
        to_insert = list(
            map(lambda x: dict(zip(['time', *[f'f{i}' for i in range(1, shape)]], x)), data))

        collection = db[context['table']]
        collection.with_options(write_concern=WriteConcern(w=0)).insert_many(to_insert, ordered=False)

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")


def fn_mongodb_create_collection_predictions(**context):
    try:
        hook = MongoHook(conn_id='mongodb_conn')
        client = hook.get_conn()
        db = client.investing
        if context['table'] in db.list_collection_names():
            db.drop_collection(context['table'])

        db.create_collection(name=context['table'], capped=True, size=832 * 200, max=200)
        collection = db[context['table']]
        collection.create_index([("time", pymongo.DESCENDING)],
                                background=True,
                                unique=True)

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")


def fn_get_predictions(**context):
    try:
        hook = MongoHook(conn_id='mongodb_conn')
        client = hook.get_conn()
        db_f = client.investing[context['f_table']]
        request_mongo = list(db_f.find(sort=[('time', -1)],
                                       projection={'_id': False}).limit(218))[::-1]
        client.close()

        """TO DO PREDICTION PROCESS"""
        data = pd.DataFrame.from_records(request_mongo)
        times, opens, highs, lows, closes = data.iloc[-200:, 0], data.iloc[-200:, 1], data.iloc[-200:, 2], data.iloc[-200:, 3], data.iloc[-200:, 4]
        times_predict = []

        # times = data.iloc[-200:, 0].values
        for t in times[-200:]:
            t = from_format(t, 'YYYY-MM-DD HH:00:00', tz='Europe/Moscow')

            if t.isoweekday() == 5:
                t += timedelta(days=3)
            else:
                t += timedelta(days=1)
            times_predict.append(t.strftime('%Y-%m-%d %H:00:00'))

        opens_predict = np.exp(np.array(opens)) + np.random.normal(3, 1, 200)
        highs_predict = np.exp(np.array(highs)) + np.random.normal(3, 1, 200)
        lows_predict = np.exp(np.array(lows)) + np.random.normal(3, 1, 200)
        closes_predict = np.exp(np.array(closes)) + np.random.normal(3, 1, 200)
        # t_delta = np.ones_like(times) * 11
        # lh = np.ones_like(times) * 0.87
        """TO DO PREDICTION PROCESS"""
        to_insert = [dict(zip(['time', 'open', 'high', 'low', 'close'], t_d)) for t_d in
                     list(zip(times, opens_predict, highs_predict, lows_predict, closes_predict))]
        return to_insert
    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")


def fn_mongodb_load_predictions(**context):
    try:
        hook = MongoHook(conn_id='mongodb_conn')
        client = hook.get_conn()

        to_insert = context['to_insert']

        collection = client.investing[context['p_table']]
        collection.with_options(write_concern=WriteConcern(w=0)).insert_many(to_insert, ordered=False)

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")


default_args = {'start_date': datetime(2022, 12, 2, 15, tz="Europe/Moscow"),
                'retries': 5,
                'retry_delay': duration(seconds=15), }

for ticker in ['sber', 'gazp', 'lkoh', 'aapl']:
    with DAG(
            dag_id=f'003_{ticker}_init',
            default_args=default_args,
            schedule_interval=None,
            params={'table': ticker}
    ) as dag:
        postgres_create = PostgresOperator(
            task_id='postgres_create',
            postgres_conn_id="postgres_conn",
            sql='DROP TABLE IF EXISTS {}; \n' \
                'CREATE TABLE IF NOT EXISTS {} ' \
                '(time timestamp UNIQUE NOT NULL, open float, high float, low float, close float, volume int);'.format(ticker, ticker)
        )
        mongo_create = PythonOperator(
            task_id="mongo_create",
            python_callable=fn_mongodb_create_collection, )

        get_metadata = PythonOperator(
            task_id="get_metadata",
            python_callable=fn_get_metadata, )

        parse_data = PythonOperator(
            task_id="parse_data",
            python_callable=fn_parse_data,
            op_kwargs={'meta': get_metadata.output})

        get_subdata = PythonOperator(
            task_id="get_subdata",
            python_callable=fn_get_subdata,
            op_kwargs={'postgres_conn_id': 'postgres_conn',
                       'sql': 'SELECT * FROM {};'})

        merge_and_fillna = PythonOperator(
            task_id="merge_and_fillna",
            python_callable=fn_merge_and_fillna,
            op_kwargs={'data': parse_data.output,
                       'subdata': get_subdata.output})

        postgres_load_data = PythonOperator(
            task_id="postgres_load_data",
            python_callable=fn_postgres_load_data,
            op_kwargs={'postgres_conn_id': 'postgres_conn',
                       'table': ticker,
                       'parse_data': parse_data.output})
        mongodb_load_data = PythonOperator(
            task_id="mongodb_load_data",
            python_callable=fn_mongodb_load_data,
            op_kwargs={"parse_data": parse_data.output}, )

        generate_features = PythonOperator(
            task_id="generate_features",
            python_callable=fn_generate_features,
            op_kwargs={"parse_data": merge_and_fillna.output}, )

        postgres_create_table_features = PythonOperator(
            task_id="postgres_create_table_features",
            python_callable=fn_postgres_create_table_features,
            op_kwargs={'postgres_conn_id': 'postgres_conn',
                       'sql': 'DROP TABLE IF EXISTS {}; \n CREATE TABLE IF NOT EXISTS {} ( {} )',
                       'table': f'{ticker}_features'}, )
        mongodb_create_collection_features = PythonOperator(
            task_id="mongodb_create_collection_features",
            python_callable=fn_mongodb_create_collection_features,
            op_kwargs={'table': f'{ticker}_features'}, )

        postgres_load_feature = PythonOperator(
            task_id="postgres_load_feature",
            python_callable=fn_postgres_load_feature,
            op_kwargs={'postgres_conn_id': 'postgres_conn',
                       'table': f'{ticker}_features',
                       'features': generate_features.output})
        mongodb_load_features = PythonOperator(
            task_id="mongodb_load_features",
            python_callable=fn_mongodb_load_features,
            op_kwargs={'table': f'{ticker}_features',
                       'features': generate_features.output}, )
        mongodb_create_collection_predictions = PythonOperator(
            task_id="mongodb_create_collection_predictions",
            python_callable=fn_mongodb_create_collection_predictions,
            op_kwargs={'table': f'{ticker}_predictions'},
        )
        get_predictions = PythonOperator(
            task_id="get_predictions",
            python_callable=fn_get_predictions,
            op_kwargs={'f_table': f'{ticker}_features',
                       'features': generate_features.output}
        )
        mongodb_load_predictions = PythonOperator(
            task_id="mongodb_load_predictions",
            python_callable=fn_mongodb_load_predictions,
            op_kwargs={'to_insert': get_predictions.output,
                       'p_table': f'{ticker}_predictions'},
        )

        [postgres_create, mongo_create] >> get_metadata >> [parse_data, get_subdata]
        parse_data >> [postgres_load_data, mongodb_load_data]
        [parse_data, get_subdata] >> merge_and_fillna
        [postgres_load_data, mongodb_load_data, merge_and_fillna] >> generate_features
        generate_features >> [postgres_create_table_features,
                              mongodb_create_collection_features]
        postgres_create_table_features >> postgres_load_feature
        [postgres_load_feature, mongodb_create_collection_features] >> mongodb_load_features
        mongodb_load_features >> mongodb_create_collection_predictions
        mongodb_create_collection_predictions >> get_predictions >> mongodb_load_predictions
