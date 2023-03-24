from pendulum import datetime, from_format, duration
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from pymongo.write_concern import WriteConcern

import pandas as pd
from typing import Any
from contextlib import closing
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.context import Context
from airflow.utils.operator_helpers import determine_kwargs

import json
from urllib.parse import urlencode

# with open('./dags/service_files/tickers.json', 'r+') as f:
#     TICKERS = json.load(f)

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


class SubDataHttpOperator(SimpleHttpOperator):
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
        responses = {}
        for ticker, end in json.loads(self.endpoint).items():
            response = http.run(end, self.data, self.headers, self.extra_options)
            if self.log_response:
                self.log.info(response.text)
            # if context['execution_date'].in_timezone(tz='Europe/Moscow').strftime('%Y%m%d,%H0000') not in response.text:
            #     raise AirflowException("Response check returned False.")
            if self.response_filter:
                kwargs = determine_kwargs(self.response_filter, [response], context)
                return self.response_filter(response, **kwargs)
            responses[ticker] = (response.text)
        return json.dumps(responses)


def fn_postgres_select_last_date(**context):
    hook = PostgresHook(postgres_conn_id=context['postgres_conn_id'])
    dates = []
    for t in context['tickers']:
        sql = 'SELECT MAX(time) FROM {} ;'.format(t)
        records = hook.get_records(sql=sql)[0][0]
        dates.append(records)
    return min(dates).strftime('%Y-%m-%d %H:00:00')


def fn_mongodb_select_last_date(**context):
    try:
        hook = MongoHook(conn_id='mongodb_conn')
        client = hook.get_conn()
        db = client.investing
        dates = []
        for t in context['tickers']:
            if t in db.list_collection_names():
                collection = db[t]
                last_time = collection.find(sort=[("time", -1)]).limit(1)[0]['time']
                dates.append(from_format(last_time, 'YYYY-MM-DD HH:00:00'))
        return min(dates).strftime('%Y-%m-%d %H:00:00')

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

    features_codes = dict(zip(range(12),
                              ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']))
    short_code = {'BR': 'BR',
                  'NG': 'NG',
                  'PT': 'PLT',
                  'GD': 'GOLD',
                  'SV': 'SILV',
                  }
    date_start = min(from_format(context['postgres_last_date'], 'YYYY-MM-DD HH:00:00'),
                     from_format(context['mongo_last_date'], 'YYYY-MM-DD HH:00:00'))

    date_start = date_start
    date_end = execution_date

    meta = json.loads(context['meta'])
    meta = pd.DataFrame.from_dict(dict(zip(['id', 'name', 'code', 'market', 'url'],
                                           list(zip(*[list(v.values()) for v in meta])))))
    endpoints = {}
    for t in context['currencies']:
        code = t.upper()
        curr_df = meta[(meta['code'] == code) &
                       (meta['market'].isin([45]))]
        em = curr_df['id'].values[0]
        market = curr_df['market'].values[0]

        head = f'{code}_{date_start.strftime("%d%m%y")}_{date_end.strftime("%d%m%y")}'
        endpoint = head + '.txt?' + urlencode([('market', market),
                                     ('em', em), ('code', code),
                                     ('apply', 0), ('df', date_start.day),
                                     ('mf', date_start.month-1), ('yf', date_start.year),
                                     ('from', date_start.strftime('%d.%m.%Y')),
                                     ('dt', date_end.day), ('mt', date_end.month-1),
                                     ('yt', date_end.year), ('to', date_end.strftime('%d.%m.%Y')),
                                     ('p', 7), ('f', head),
                                     ('e', '.txt'), ('cn', code),
                                     ('dtf', 1), ('tmf', 1), ('MSOR', 0),
                                     ('mstime', 'on'), ('mstimever', 1), ('sep', 1), ('sep2', 1),
                                     ('datf', 4), ('at', 0)])
        endpoints[t] = endpoint
    for t in context['features']:
        ticker = t.upper()

        m = (date_start.month // 3 + 1) * 3 % 15 + ((date_start.month // 3 + 1) * 3 // 15) * 3

        code = f"{ticker}{features_codes[m - 1]}{date_start.add(months=1).year % 10}"
        name = f"{short_code[ticker]}-{m}.{date_start.add(months=1).strftime('%y')}"

        print(date_start, date_start.month, m, code, name)

        curr_df = meta[(meta['code'] == code) &
                       (meta['name'].str.contains(name))]
        if curr_df.empty:
            continue

        em = curr_df['id'].values[0]
        market = curr_df['market'].values[0]


        head = f'{code}_{date_start.strftime("%d%m%y")}_{date_end.strftime("%d%m%y")}'
        endpoint = head + '.txt?' + urlencode([('market', market),
                                     ('em', em), ('code', code),
                                     ('apply', 0), ('df', date_start.day),
                                     ('mf', date_start.month-1), ('yf', date_start.year),
                                     ('from', date_start.strftime('%d.%m.%Y')),
                                     ('dt', date_end.day), ('mt', date_end.month-1),
                                     ('yt', date_end.year), ('to', date_end.strftime('%d.%m.%Y')),
                                     ('p', 7), ('f', head),
                                     ('e', '.txt'), ('cn', code),
                                     ('dtf', 1), ('tmf', 1), ('MSOR', 0),
                                     ('mstime', 'on'), ('mstimever', 1), ('sep', 1), ('sep2', 1),
                                     ('datf', 4), ('at', 0)])
        endpoints[t] = endpoint

    return json.dumps(endpoints)


def fn_get_correct_data(execution_date, **context):
    http_data = json.loads(context['http_data'])
    ticker_hourly_data = {}
    for ticker, subdata in http_data.items():
        last_date_time = execution_date
        ticker_hourly_data[ticker] = []
        for hourly in subdata.strip().split('\n'):
            hourly = hourly.strip().split(',')
            data = hourly[2]
            time = hourly[3]
            h_datetime = from_format(f'{data} {time}', 'YYYYMMDD HH0000', tz='Europe/Moscow')
            if last_date_time < h_datetime:
                break
            if h_datetime.hour >= 10 and h_datetime.hour < 19:
                c = float(hourly[4])
                ticker_hourly_data[ticker].append([h_datetime.strftime('%Y-%m-%d %H:00:00'), c])
    return json.dumps(ticker_hourly_data)


def fn_postgres_load_new_data(**context):
    """ TO DO IN PostgresOperator and INSERT only new data """
    hook = SkipConflictPostgresHook(postgres_conn_id=context['postgres_conn_id'])
    data = json.loads(context['parse_data'])
    for ticker, subdata in data.items():
        hook.insert_rows(table=ticker,
                         rows=subdata,
                         resolve_conflict='time')


def fn_mongodb_load_new_data(**context):
    """ TO DO IN MongoHook and INSERT only new data """
    try:
        hook = MongoHook(conn_id='mongodb_conn')
        client = hook.get_conn()
        db = client.investing
        data = json.loads(context['parse_data'])
        for ticker, subdata in data.items():
            to_insert = list(
                map(lambda x: dict(zip(['time', 'close'], x)), subdata))

            collection = db[ticker]
            collection.with_options(write_concern=WriteConcern(w=0)).insert_many(to_insert, ordered=False)

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")


default_args = {'start_date': datetime(2022, 12, 2, 15, tz="Europe/Moscow"),
                'retries': 30,
                'retry_delay': duration(seconds=15),}


with DAG(
        dag_id='004_parse_subdata',
        default_args=default_args,
        schedule_interval='0 10-18 * * 1-5',
        catchup=False
) as dag:
    postgres_last_date = PythonOperator(
        task_id='postgres_last_date',
        python_callable=fn_postgres_select_last_date,
        op_kwargs={'postgres_conn_id': 'postgres_conn',
                   'sql': 'SELECT MAX(time) FROM {} ;',
                   'tickers': currencies + features})
    mongo_last_date = PythonOperator(
        task_id='mongo_last_date',
        python_callable=fn_mongodb_select_last_date,
        op_kwargs={'tickers': currencies + features})

    get_metadata = PythonOperator(
        task_id='get_metadata',
        python_callable=fn_get_metadata,
    )

    get_endpoints = PythonOperator(
        task_id='get_endpoints',
        python_callable=fn_get_endpoint,
        op_kwargs={'currencies': currencies,
                   'features': features,
                   'postgres_last_date': postgres_last_date.output,
                   'mongo_last_date': mongo_last_date.output,
                   'meta': get_metadata.output},
        do_xcom_push=True)

    parse_data_http = SubDataHttpOperator(
        task_id=f'parse_data_http',
        http_conn_id='http_finam',
        method='GET',
        endpoint="{{ task_instance.xcom_pull(task_ids='get_endpoints') }}",
        headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) '
                               'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'},
        log_response=True
    )

    get_correct_data = PythonOperator(
        task_id=f'get_correct_data',
        python_callable=fn_get_correct_data,
        op_kwargs={'http_data': parse_data_http.output,}
    )

    postgres_load_data = PythonOperator(
        task_id=f'postgres_load_data',
        python_callable=fn_postgres_load_new_data,
        op_kwargs={'postgres_conn_id': 'postgres_conn',
                   'parse_data': get_correct_data.output})
    mongodb_load_data = PythonOperator(
        task_id=f'mongodb_load_data',
        python_callable=fn_mongodb_load_new_data,
        op_kwargs={"parse_data": get_correct_data.output}, )

    [postgres_last_date, mongo_last_date, get_metadata] >> get_endpoints >> parse_data_http
    parse_data_http >> get_correct_data >> [postgres_load_data, mongodb_load_data]
