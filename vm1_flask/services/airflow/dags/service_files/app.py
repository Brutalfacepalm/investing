import pandas as pd
from utils import Parser, get_logger, PostgresLoader
from datetime import datetime
import pymongo
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern

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
    print(data_as_df)
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
    cursor = client['investing']
    # if 'gazp' not in cursor.list_collection_names():
    #     cursor.create_collection(name='gazp', capped=True, size=2128 * 100, max=100)
    cursor = cursor['gazp']
    print(cursor)
    print('cursor')
    print(client.list_database_names())
    print('list_database_names')
    print(len(data_as_df))
    df_as_arr = data_as_df
    # cursor.create_index([("time", pymongo.ASCENDING)],
    #                     background=True,
    #                     unique=True)
    # to_insert = list(map(lambda x: dict(zip(['time', 'open', 'high', 'low', 'close', 'volume'], x)), df_as_arr[-10:]))
    # print(to_insert)
    # cursor.with_options(write_concern=WriteConcern(w=0)).insert_many(to_insert, ordered=False)
    # for i_frame in range(len(data_as_df)-100, len(data_as_df)):
    #     frame = dict(zip(['time', 'open', 'high', 'low', 'close', 'volume'], df_as_arr[i_frame]))
    #
    #     cursor.insert_one(frame)

    print(cursor.count_documents({}))
    request_mongo = cursor.find(sort=[("time", -1)]).limit(1)[0]['time']
    # print(list(zip(*[list(fr.values()) for fr in request_mongo])))
    print(request_mongo)
    # times, opens, highs, lows, closes, volumes = list(zip(*[list(fr.values()) for fr in request_mongo]))
    times = request_mongo
    print(times)
    # print(times[-1])
    # print(closes[-1])
    # print(type(closes[-1]))
    client.close()
    print('DONE')
    # logger.info('START CONNECTION')
    # print('START CONNECTION')
    # db_connect = PostgresLoader('investing_db',
    #                             'investing_db',
    #                             'investing_db',
    #                             '192.168.0.15',
    #                             '5432')
    # logger.info('DONE CONNECTION')
    # print('DONE CONNECTION')
    #
    # write_db(logger, db_connect, data_as_df)
    # read_db(logger, db_connect)