from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import pandas as pd
import pandas.io.sql as psql
import json
import click
import torch
import re

from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from feature_creator import FeatureCreator
from predictioner import PredictorPredict


MODEL_FILE_NAME = 'model.mdl'
SCALER_FILE_NAME = 'scaler.pkl'
CURRENCIES_PATH = 'currencies.json'
FEATURES_PATH = 'features.json'


def insert_on_duplicate(table, conn, keys, data_iter):
    """

    :param table:
    :param conn:
    :param keys:
    :param data_iter:
    :return:
    """
    insert_stmt = insert(table.table).values(list(data_iter))
    on_duplicate_key_stmt = insert_stmt.on_conflict_do_nothing()
    conn.execute(on_duplicate_key_stmt)


def check_topics(from_topic, to_topic, bootstrap_servers):
    """

    :param from_topic:
    :param to_topic:
    :param bootstrap_servers:
    :return:
    """
    bc = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topics_to_create = []
    if from_topic not in bc.list_topics():
        if from_topic != 'empty':
            topics_to_create.append(NewTopic(from_topic, 1, 1))
        else:
            print('To topic is empty. Are you seriously?')
    else:
        print(f'All right, {from_topic} is exist.')
    if to_topic not in bc.list_topics():
        if to_topic != 'empty':
            topics_to_create.append(NewTopic(to_topic, 1, 1))
        else:
            print('To topic is empty. Are you seriously?')
    else:
        print(f'All right, {to_topic} is exist.')
    if topics_to_create:
        bc.create_topics(topics_to_create)


def get_df_data(value):
    """

    :param value:
    :return:
    """
    try:
        data = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume'], data=json.loads(value))
        assert data.shape[-1] == 6, f'Wrong column set. Must length 6, but have {data.shape[-1]}'
        return data
    except Exception as e:
        print(e)
        print('Wrong data for create pandas.Dataframe')
        return None


def get_df_feature(value):
    """

    :param value:
    :return:
    """
    try:
        data = pd.DataFrame(columns=['time'] + [f'f{i+1}' for i in range(557)], data=json.loads(value))
        print(data.shape)
        assert data.shape[-1] == 558, f'Wrong column set. Must length 797, but have {data.shape[-1]}'
        return data
    except Exception as e:
        print(e)
        print('Wrong data for create pandas.Dataframe')
        return None


def get_df_predictis(value):
    """

    :param value:
    :return:
    """
    try:
        data = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close'], data=json.loads(value))
        assert data.shape[-1] == 5, f'Wrong column set. Must length 5, but have {data.shape[-1]}'
        return data
    except Exception as e:
        print(e)
        print('Wrong data for create pandas.Dataframe')
        return None


def generate_features_from_data(db2_client, db2_collection, ticker, currencies, commodities):
    """

    :param db2_client:
    :param db2_collection:
    :param ticker:
    :param currencies:
    :param commodities:
    :return:
    """
    data = select_from_mongo(db2_client, db2_collection, ticker, currencies, commodities, 300)
    data['time'] = pd.to_datetime(data['time'])
    f_creator = FeatureCreator(data.copy())
    features = f_creator.generate_feature()
    features['time'] = features['time'].dt.strftime('%Y-%m-%d %H:00:00')
    features = features.iloc[-20:, :]
    to_insert = list(
        map(lambda x: dict(zip(['time', *[f'f{i}' for i in range(1, features.shape[1])]], x)), features.values))
    to_topic_value = json.dumps(to_insert).encode()
    to_topic_key = json.dumps(ticker).encode()

    return to_topic_key, to_topic_value


def generate_predictions_from_features(db2_client, db2_collection, ticker):
    """

    :param db2_client:
    :param db2_collection:
    :param ticker:
    :return:
    """
    data = select_from_mongo(db2_client, db2_collection, f'{ticker}_features', None, None, 65)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    predictioner = PredictorPredict(device, seq_len=54, target_mode='abs', log=False)
    res = predictioner.predict(data,
                               f'.models/{ticker}/prod/{MODEL_FILE_NAME}',
                               f'.models/{ticker}/prod/{SCALER_FILE_NAME}')
    to_insert = [{'time': k.strftime('%Y-%m-%d %H:00:00'),
                  'open': v[0],
                  'high': v[1],
                  'low': v[2],
                  'close': v[3]} for k, v in res.items()]

    to_topic_value = json.dumps(to_insert).encode()
    to_topic_key = json.dumps(ticker).encode()

    return to_topic_key, to_topic_value


def load_to_postgres(data, ticker, db1_client, from_topic):
    """

    :param data:
    :param ticker:
    :param db1_client:
    :param from_topic:
    :return:
    """
    if from_topic == 'features':
        ticker = f'{ticker}_features'
    elif from_topic == 'predicts':
        ticker = f'{ticker}_predictions'
    try:
        psql.to_sql(frame=data, name=ticker, con=db1_client,
                    index=False, if_exists='append', method=insert_on_duplicate)
        print('Load to postgres success.')
    except Exception as e:
        print(e)
        print('Wrong attempt load data to postgres. Check data.')


def load_to_mongo(data, ticker, db2_collection, db2_client, from_topic):
    """

    :param data:
    :param ticker:
    :param db2_collection:
    :param db2_client:
    :param from_topic:
    :return:
    """
    if from_topic == 'features':
        ticker = f'{ticker}_features'
    elif from_topic == 'predicts':
        ticker = f'{ticker}_predictions'
    try:
        columns = data.columns.to_list()
        cursor = db2_client[db2_collection][ticker]
        to_insert = list(map(lambda x: dict(zip(columns, x)), data.values))

        cursor.with_options(write_concern=WriteConcern(w=0)).insert_many(to_insert, ordered=False)
        print('Load to mongodb success.')
    except Exception as e:
        print(e)
        print('Wrong attempt load data to mongodb. Check data.')


def select_from_mongo(db2_client, db2_collection, ticker, currencies, commodities, len_select):
    """

    :param db2_client:
    :param db2_collection:
    :param ticker:
    :param currencies:
    :param commodities:
    :param len_select:
    :return:
    """
    cursor = db2_client[db2_collection][ticker]
    data_for_features = list(cursor.find(sort=[('time', -1)], projection={'_id': False}).limit(len_select))[::-1]

    data_for_features = pd.DataFrame.from_records(data_for_features)

    if currencies and commodities:
        subdata = currencies + commodities
        for k, sbdt in enumerate(subdata):
            cursor = db2_client[db2_collection]["".join(re.findall(r"(\w*)", sbdt)).upper()]
            sbdt_db2 = list(cursor.find(sort=[('time', -1)], projection={'_id': False,
                                                                         'time': True,
                                                                         'close': True}).limit(len_select * 2))[::-1]
            sbdt_df = pd.DataFrame.from_records(sbdt_db2)
            data_for_features = data_for_features.merge(sbdt_df, how='left', on='time', suffixes=('', f'_s{k + 1}'))

        data_for_features = data_for_features.interpolate(mothod='linear', limit_direction='both')

    return data_for_features


@click.command()
@click.option('--bootstrap_servers', '-bs', default='broker1:9092')
@click.option('--from_topic', '-f', default='data')
@click.option('--to_topic', '-t', default='features')
@click.option('--db1_connect', '-db1', default='investing_db:investing_db:investing_db:localhost:5432')
@click.option('--db2_connect', '-db2', default='operate_database:operate_database:localhost:27017:investing')
def run_worker(bootstrap_servers, from_topic, to_topic, db1_connect, db2_connect):
    """

    :param bootstrap_servers:
    :param from_topic:
    :param to_topic:
    :param db1_connect:
    :param db2_connect:
    :return:
    """

    db1_dbname, db1_dbuser, db1_dbpass, db1_dbhost, db1_dbport = db1_connect.split(':')
    db2_dbuser, db2_dbpass, db2_dbhost, db2_dbport, db2_collection = db2_connect.split(':')

    db1_cl = create_engine(f'postgresql+psycopg2://{db1_dbuser}:{db1_dbpass}@{db1_dbhost}:{db1_dbport}/{db1_dbname}')
    db2_cl = MongoClient(f'mongodb://{db2_dbuser}:{db2_dbpass}@{db2_dbhost}:{db2_dbport}/')

    with open(CURRENCIES_PATH, 'r+') as f:
        currencies = json.load(f)

    with open(FEATURES_PATH, 'r+') as f:
        commodities = json.load(f)

    check_topics(from_topic, to_topic, bootstrap_servers)

    consumer = KafkaConsumer(from_topic, bootstrap_servers=bootstrap_servers)
    print(f'START! {from_topic} -> {to_topic}')
    for msg in consumer:
        ticker = json.loads(msg.key)
        if from_topic == 'data':
            data = get_df_data(msg.value)
        elif from_topic == 'features':
            data = get_df_feature(msg.value)
        elif from_topic == 'predicts':
            data = get_df_predictis(msg.value)
        else:
            print('Wrong from_topic')
            return

        if from_topic != 'predicts':
            load_to_postgres(data, ticker, db1_cl, from_topic)
        load_to_mongo(data, ticker, db2_collection, db2_cl, from_topic)

        if from_topic == 'data':
            key, value = generate_features_from_data(db2_cl, db2_collection, ticker, currencies, commodities)
            if to_topic != 'features':
                return f'Wrong to_topic. Must be features, but get {to_topic}'
            else:
                try:
                    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
                    future = producer.send(to_topic, key=key, value=value)
                    result = future.get(timeout=60)
                    print(result)
                except Exception as e:
                    print(e)
                    print('Wrong attempt send message to Producer. Check connection to Producer.')
        elif from_topic == 'features':
            key, value = generate_predictions_from_features(db2_cl, db2_collection, ticker)
            if to_topic != 'predicts':
                return f'Wrong to_topic. Must be predicts, but get {to_topic}'
            else:
                try:
                    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
                    future = producer.send(to_topic, key=key, value=value)
                    result = future.get(timeout=60)
                    print(result)
                except Exception as e:
                    print(e)
                    print('Wrong attempt send message to Producer. Check connection to Producer.')


if __name__ == "__main__":
    run_worker()
