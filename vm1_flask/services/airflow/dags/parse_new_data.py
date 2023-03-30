from pendulum import datetime, from_format, duration
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from pymongo.write_concern import WriteConcern
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator

import pandas as pd
from typing import Any
from contextlib import closing
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.context import Context
from airflow.utils.operator_helpers import determine_kwargs

import json
from urllib.parse import urlencode

# with open('./dags/service_files/tickers.json', 'r+') as f:
#     TICKERS = json.load(f)


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


class HttpOperatorCheckDates(SimpleHttpOperator):
    def execute(self, context: Context) -> Any:
        print(f"EXECUTION_DATE: {context['execution_date'].in_timezone(tz='Europe/Moscow').strftime('%Y%m%d,%H0000')}")
        http = HttpHook(
            self.method,
            http_conn_id=self.http_conn_id,
            auth_type=self.auth_type,
            tcp_keep_alive=self.tcp_keep_alive,
            tcp_keep_alive_idle=self.tcp_keep_alive_idle,
            tcp_keep_alive_count=self.tcp_keep_alive_count,
            tcp_keep_alive_interval=self.tcp_keep_alive_interval,
        )

        self.log.info("Calling HTTP method")

        response = http.run(self.endpoint, self.data, self.headers, self.extra_options)
        if self.log_response:
            self.log.info(response.text)
        if context['execution_date'].in_timezone(tz='Europe/Moscow').strftime('%Y%m%d,%H0000') not in response.text:
            raise AirflowException("Response check returned False.")
        if self.response_filter:
            kwargs = determine_kwargs(self.response_filter, [response], context)
            return self.response_filter(response, **kwargs)
        return response.text


def fn_postgres_select_last_date(**context):
    sql = 'SELECT MAX(time) FROM {} ;'.format(context['ticker'])
    hook = PostgresHook(postgres_conn_id=context['postgres_conn_id'])
    records = hook.get_records(sql=sql)[0][0]
    return records.strftime('%Y-%m-%d %H:00:00')


def fn_mongodb_select_last_date(**context):
    try:
        hook = MongoHook(conn_id='mongodb_conn')
        client = hook.get_conn()
        db = client.investing
        if context['params']['ticker'] in db.list_collection_names():
            collection = db[context['params']['ticker']]
            last_time = collection.find(sort=[("time", -1)]).limit(1)[0]['time']
            return last_time

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


def fn_get_endpoint(execution_date, **context):
    """ Define start_date how min(postgres_date, mongodb_date) and end_date how Today() for Parser. """
    date_start = min(from_format(context['postgres_last_date'], 'YYYY-MM-DD HH:00:00'),
                     from_format(context['mongo_last_date'], 'YYYY-MM-DD HH:00:00'))

    date_start = date_start
    date_end = execution_date
    context['task_instance'].xcom_push(key='response_check_date',
                                       value=execution_date.add(hours=1).strftime('%Y%m%d,%H0000'))
    code = context['ticker'].upper()

    meta = json.loads(context['meta'])
    meta = pd.DataFrame.from_dict(dict(zip(['id', 'name', 'code', 'market', 'url'],
                                           list(zip(*[list(v.values()) for v in meta])))))

    curr_df = meta[(meta['code'] == code) &
                   (meta['market'].isin([1]))]
    em = curr_df['id'].values[0]
    market = curr_df['market'].values[0]

    head = f'{code}_{date_start.strftime("%d%m%y")}_{date_end.strftime("%d%m%y")}'
    endpoint = head + '.txt?' + urlencode([('market', market),
                                 ('em', em),
                                 ('code', code),
                                 ('apply', 0),
                                 ('df', date_start.day),
                                 ('mf', date_start.month-1),
                                 ('yf', date_start.year),
                                 ('from', date_start.strftime('%d.%m.%Y')),
                                 ('dt', date_end.day),
                                 ('mt', date_end.month-1),
                                 ('yt', date_end.year),
                                 ('to', date_end.strftime('%d.%m.%Y')),
                                 ('p', 7),
                                 ('f', head),
                                 ('e', '.txt'),
                                 ('cn', code),
                                 ('dtf', 1),
                                 ('tmf', 1),
                                 ('MSOR', 0),
                                 ('mstime', 'on'),
                                 ('mstimever', 1),
                                 ('sep', 1),
                                 ('sep2', 1),
                                 ('datf', 1),
                                 ('at', 0)])
    return endpoint


def fn_get_correct_data(execution_date, **context):
    http_data = context['http_data']
    ticker_hourly_data = []
    last_date_time = execution_date
    for hourly in http_data.strip().split('\n'):
        hourly = hourly.strip().split(',')
        data = hourly[2]
        time = hourly[3]
        h_datetime = from_format(f'{data} {time}', 'YYYYMMDD HH0000', tz='Europe/Moscow')
        if last_date_time < h_datetime:
            break
        if h_datetime.hour >= 10 and h_datetime.hour < 23:
            o, h, l, c, v = float(hourly[4]), float(hourly[5]), float(hourly[6]), float(hourly[7]), int(hourly[8])
            ticker_hourly_data.append([h_datetime.strftime('%Y-%m-%d %H:00:00'), o, h, l, c, v])
    return json.dumps(ticker_hourly_data)


def fn_produce_to_topic_data(ticker, data):
    data = json.loads(data)
    yield (json.dumps(ticker), json.dumps(data))


def fn_postgres_load_new_data(**context):
    """ TO DO IN PostgresOperator and INSERT only new data """
    hook = SkipConflictPostgresHook(postgres_conn_id=context['postgres_conn_id'])
    data = json.loads(context['parse_data'])
    hook.insert_rows(table=context['ticker'],
                     rows=data,
                     resolve_conflict='time')


def fn_mongodb_load_new_data(**context):
    """ TO DO IN MongoHook and INSERT only new data """
    try:
        hook = MongoHook(conn_id='mongodb_conn')
        client = hook.get_conn()
        db = client.investing
        data = json.loads(context['parse_data'])
        to_insert = list(
            map(lambda x: dict(zip(['time', 'open', 'high', 'low', 'close', 'volume'], x)), data))

        collection = db[context['params']['ticker']]
        collection.with_options(write_concern=WriteConcern(w=0)).insert_many(to_insert, ordered=False)

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")


default_args = {'start_date': datetime(2022, 12, 2, 15, tz="Europe/Moscow"),
                'retries': 30,
                'retry_delay': duration(seconds=15),}


for ticker in ['sber', 'gazp', 'lkoh', 'aapl']:
    with DAG(
            dag_id=f'005_{ticker}_parse_data',
            default_args=default_args,
            schedule_interval='0 10-18 * * 1-5',
            catchup=False,
            params={'ticker': ticker}
    ) as dag:
        previous_dag = ExternalTaskSensor(
            task_id='external_task',
            external_dag_id=f'004_parse_subdata',
            external_task_ids=['mongodb_load_data', 'postgres_load_data'],
            timeout=300)

        postgres_last_date = PythonOperator(
            task_id='postgres_last_date',
            python_callable=fn_postgres_select_last_date,
            op_kwargs={'postgres_conn_id': 'postgres_conn',
                       'sql': 'SELECT MAX(time) FROM {} ;',
                       'ticker': ticker})
        mongo_last_date = PythonOperator(
            task_id="mongo_last_date",
            python_callable=fn_mongodb_select_last_date)

        get_metadata = PythonOperator(
            task_id='get_metadata',
            python_callable=fn_get_metadata,
        )

        get_endpoint = PythonOperator(
            task_id="get_endpoint",
            python_callable=fn_get_endpoint,
            op_kwargs={'ticker': ticker,
                       'postgres_last_date': postgres_last_date.output,
                       'mongo_last_date': mongo_last_date.output,
                       'meta': get_metadata.output},
            do_xcom_push=True)

        parse_data_http = HttpOperatorCheckDates(
            task_id='parse_data_http',
            http_conn_id='http_finam',
            method='GET',
            endpoint="{{ task_instance.xcom_pull(task_ids='get_endpoint') }}",
            headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) '
                                   'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'},
            log_response=True
        )

        get_correct_data = PythonOperator(
            task_id='get_correct_data',
            python_callable=fn_get_correct_data,
            op_kwargs={'http_data': parse_data_http.output,}
        )

        produce_to_topic_data = ProduceToTopicOperator(
            task_id=f"produce_to_topic_data",
            topic='data',
            producer_function=fn_produce_to_topic_data,
            producer_function_args=(ticker, get_correct_data.output),
            kafka_config={'bootstrap.servers': '192.168.1.13:39092'},
            # op_kwargs={'ticker': ticker,
            #            'parse_data': get_correct_data.output}
        )

        # postgres_load_data = PythonOperator(
        #     task_id="postgres_load_data",
        #     python_callable=fn_postgres_load_new_data,
        #     op_kwargs={'postgres_conn_id': 'postgres_conn',
        #                'ticker': ticker,
        #                'parse_data': get_correct_data.output})
        # mongodb_load_data = PythonOperator(
        #     task_id="mongodb_load_data",
        #     python_callable=fn_mongodb_load_new_data,
        #     op_kwargs={"parse_data": get_correct_data.output}, )

        previous_dag >> [postgres_last_date, mongo_last_date, get_metadata] >> get_endpoint >> parse_data_http
        parse_data_http >> get_correct_data >> produce_to_topic_data
        # get_correct_data >> [postgres_load_data, mongodb_load_data]
