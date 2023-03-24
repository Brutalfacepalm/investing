import pandas as pd
from utils import Parser, PostgresLoader, get_logger
from datetime import datetime
from pymongo import MongoClient
import json

TICKER = 'GAZP'

def parse(logger):

    FROM = '01.11.2022'
    TO = (datetime.now().date() + pd.Timedelta(value=10, unit='D')).strftime('%d.%m.%Y')

    print('START PARSE')
    logger.info('START PARSE')

    parser_ticker = Parser(TICKER, FROM, TO)
    data_as_df = parser_ticker.parse()
    print('DONE PARSE')
    logger.info('DONE PARSE')

    logger.info('DF!')
    print('DF')
    print(data_as_df.head(20))
    return data_as_df


def write_db(logger, db_connect, data_as_df):

    logger.info('START WRITE')
    print('START WRITE')
    db_connect.write_to_sql(data_as_df, TICKER)
    logger.info('DONE WRITE')
    print('DONE WRITE')

def read_db(logger, db_connect):
    logger.info('START READ')
    print('START READ')
    db_connect.read_from_sql(TICKER)
    logger.info('DONE READ')
    print('DONE READ')


if __name__ == '__main__':
    logger = get_logger()

    data_as_df = parse(logger)

    client = MongoClient('mongodb://operate_database:operate_database@0.0.0.0:27017/')
    print('CLIENT')
    cursor = client['investing']['gazp']
    print(cursor)
    print('cursor')
    print(client.list_database_names())
    print('list_database_names')
    print(data_as_df.shape)
    df_as_arr = data_as_df.to_numpy()
     # = []
    for i_frame in range(data_as_df.shape[0]-100, data_as_df.shape[0]):
        frame = dict(zip(['time', 'open', 'high', 'low', 'close', 'volume'], df_as_arr[i_frame]))

        cursor.insert_one(frame)

    request_mongo = list(cursor.find(projection={'_id': False,
                                       'time': True,
                                       'open': True,
                                       'high': True,
                                       'low': True,
                                       'close': True,
                                       'volume': True}))
    # print(list(zip(*[list(fr.values()) for fr in request_mongo])))
    client.close()
    print('DONE')
    logger.info('START CONNECTION')
    print('START CONNECTION')
    db_connect = PostgresLoader('investing_db',
                                'investing_db',
                                'investing_db',
                                '192.168.0.15',
                                '5432')
    logger.info('DONE CONNECTION')
    print('DONE CONNECTION')

    write_db(logger, db_connect, data_as_df)
    read_db(logger, db_connect)