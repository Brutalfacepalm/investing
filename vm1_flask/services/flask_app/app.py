from flask import Flask, render_template, request
import click
import plotly
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
from pymongo import MongoClient
from prometheus_flask_exporter import PrometheusMetrics
from utils import Parser, PostgresLoader
from graph_plotly import get_figure

app = Flask(__name__)
app.config.update(
    CSRF_ENABLED=True,
    # SECRET_KEY='you-will-never-guess',
)
metrics = PrometheusMetrics(app)


@app.route("/")
def index():
    # db_connect = PostgresLoader('investing_db',
    #                             'investing_db',
    #                             'investing_db',
    #                             '192.168.0.15',
    #                             '5432')
    # times, opens, highs, lows, closes, _ = db_connect.read_from_sql('GAZP')
    client = MongoClient('mongodb://operate_database:operate_database@mongo:27017/')
    cursor = client['investing']['gazp']
    request_mongo = list(cursor.find(sort=[('time', -1)],
                                     projection={'_id': False,
                                       'time': True,
                                       'open': True,
                                       'high': True,
                                       'low': True,
                                       'close': True,
                                       'volume': True}).limit(200))[::-1]

    times, opens, highs, lows, closes, volumes = list(zip(*[list(fr.values()) for fr in request_mongo]))
    # last_time = times[-1]
    times = list(map(lambda x: datetime.strptime(x, '%Y-%m-%d %H:00:00'), times))
    # print(times)
    # print(last_time)
    # print(type(last_time))
    predict_cursor = client['investing']['gazp_predictions']
    predicts = list(predict_cursor.find(sort=[('time', -1)],
                                            projection={'_id': False}).limit(100))[::-1]
    # print(last_predict)
    # predictions = {'t': last_predict['t_delta'],
    #                'v': last_predict['predict'],
    #                'lh': last_predict['lh']}
    times_predict, opens_predict, highs_predict, lows_predict, closes_predict = list(zip(*[list(fr.values()) for fr in predicts]))
    times_predict = list(map(lambda x: datetime.strptime(x, '%Y-%m-%d %H:00:00'), times_predict))
    # [t + timedelta(days=1) for t in times[-50:]]

    # print(predictions)


    client.close()
    # frames = list(zip(*[list(fr.values()) for fr in request_mongo]))
    # frames[0] = list(map(lambda x: int(x.timestamp()), frames[0]))
    # frames = list(zip(*frames))
    # predictions = {'t': 11,
    #                'v': closes[-1] * 0.01,
    #                'lh': 0.87}
    fig = get_figure(times, opens, highs, lows, closes, volumes,
                     times_predict, opens_predict, highs_predict, lows_predict, closes_predict)
    # fig = go.Figure(data=[go.Candlestick(x=[t.strftime("%d.%m.%y:%H") for t in times],
    #                                      open=opens,
    #                                      high=highs,
    #                                      low=lows,
    #                                      close=closes, )],
    #                 skip_invalid=True)
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    # graphJSON = json.dumps(frames)
    # print('JSON')
    # print(graphJSON)
    # print(type(graphJSON))

    response = {'tickers': ['SBER', 'GAZP', 'LUKH', 'GMNK', 'SNGS'],
                'graphs': [('g1', '1d', '1 days', graphJSON)],
                }
    return render_template('index.html', response=response)


@click.command()
@click.option('--host', '-h', default='127.0.0.1')
@click.option('--port', '-p', default='8000')
@click.option('--debug', default=True)
def run_app(host, port, debug):
    app.run(host, port, debug)


if __name__ == '__main__':
    # metrics.start_http_server(5099)
    run_app()
