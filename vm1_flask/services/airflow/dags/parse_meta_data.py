import pandas as pd
from service_files.parser_meta import ExporterMeta
import json
import pymongo
from pendulum import datetime, from_format
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from pymongo.write_concern import WriteConcern


with open('./dags/service_files/shares.json', 'r+') as f:
    shares = json.load(f)

with open('./dags/service_files/currencies.json', 'r+') as f:
    currencies = json.load(f)

with open('./dags/service_files/features.json', 'r+') as f:
    features = json.load(f)


def fn_parse_meta():
    meta = ExporterMeta()
    df = meta.lookup(market=[1, 14, 17, 25, 45]).reset_index()
    df_cleaned = df[df['market'] == 1].groupby('code').agg(lambda x: set([i for i in x]))
    df_cleaned = df.merge(df_cleaned['url'].apply(lambda x: min(x, key=len)).reset_index(), on=['code', 'url'])
    df_cleaned = pd.concat([df_cleaned,
                            df[(df['market'] == 45) & df['code'].isin(currencies)],
                            df[df['market'].isin([14, 17, 25])]])
    df_cleaned.to_csv('./dags/service_files/meta_tickers.csv')
    return json.dumps(df_cleaned.to_numpy().tolist())


def fn_load_meta_to_mongodb(**context):
    try:
        hook = MongoHook(conn_id='mongodb_conn')
        client = hook.get_conn()
        db = client.investing
        if 'meta' in db.list_collection_names():
            db.drop_collection('meta')
        data = json.loads(context['meta_data'])
        to_insert = list(
            map(lambda x: dict(zip(['id', 'name', 'code', 'market', 'url'], x)), data))

        collection = db['meta']
        collection.create_index([("id", pymongo.DESCENDING)],
                                background=True,
                                unique=True)
        collection.with_options(write_concern=WriteConcern(w=0)).insert_many(to_insert, ordered=False)

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")


with DAG(
        dag_id='001_parse_meta_data',
        start_date=datetime(2022, 12, 2, 15, tz="Europe/Moscow"),
        schedule_interval='0 0 1 * *',
) as dag:

    parse_meta = PythonOperator(
        task_id="parse_meta",
        python_callable=fn_parse_meta, )
    load_meta_to_mongodb = PythonOperator(
        task_id="load_meta_to_mongodb",
        python_callable=fn_load_meta_to_mongodb,
        op_kwargs={'meta_data': parse_meta.output}
    )

    parse_meta >> load_meta_to_mongodb
