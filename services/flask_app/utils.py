import sys
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
import json
import pandas as pd
import numpy as np
from pymongo import MongoClient
from plotly.subplots import make_subplots
from plotly import graph_objects as go
from plotly.utils import PlotlyJSONEncoder


def get_figure(times, opens, highs, lows, closes, volumes,
               times_predict, opens_predict, highs_predict, lows_predict, closes_predict, period=9):
    """

    :param times:
    :param opens:
    :param highs:
    :param lows:
    :param closes:
    :param volumes:
    :param times_predict:
    :param opens_predict:
    :param highs_predict:
    :param lows_predict:
    :param closes_predict:
    :param period:
    :return:
    """

    missing_dates = pd.date_range(start=times[0], end=times_predict[-1], freq='1H')
    missing_dates = missing_dates.difference(list(times) + list(times_predict)).tolist()

    colors_value = ['rgba(199, 52, 59, .3)' if closes[i] < opens[i] else 'rgba(35, 132, 24, .3)' for i in
                    range(len(closes))]

    go_candles = go.Candlestick(x=times,
                                open=opens,
                                high=highs,
                                low=lows,
                                close=closes,
                                showlegend=False,
                                hoverinfo='x+y',
                                )
    go_candles.increasing.fillcolor = 'rgba(35, 132, 24, 1)'
    go_candles.increasing.line.color = 'rgba(35, 132, 24, 1)'
    go_candles.increasing.line.width = 1
    go_candles.decreasing.fillcolor = 'rgba(199, 52, 59, 1)'
    go_candles.decreasing.line.color = 'rgba(199, 52, 59, 1)'
    go_candles.decreasing.line.width = 1

    go_volumes = go.Bar(x=times,
                        y=volumes,
                        showlegend=False,
                        marker=dict(color=colors_value),
                        marker_line=dict(width=0),
                        hoverinfo='y+x',
                        hoverlabel=dict(bordercolor=colors_value, bgcolor=colors_value),
                        )
    go_compare = go.Candlestick(x=times_predict[:-period],
                                open=opens_predict[:-period],
                                high=highs_predict[:-period],
                                low=lows_predict[:-period],
                                close=closes_predict[:-period],
                                showlegend=False,
                                hoverinfo='skip',
                                opacity=0.33
                                )
    go_compare.increasing.fillcolor = 'blue'
    go_compare.increasing.line.color = 'blue'
    go_compare.increasing.line.width = 1
    go_compare.decreasing.fillcolor = 'grey'
    go_compare.decreasing.line.color = 'grey'
    go_compare.decreasing.line.width = 1

    go_predictions = go.Candlestick(x=times_predict[-period:],
                                    open=opens_predict[-period:],
                                    high=highs_predict[-period:],
                                    low=lows_predict[-period:],
                                    close=closes_predict[-period:],
                                    showlegend=False,
                                    hoverinfo='y+x',
                                    )
    go_predictions.increasing.fillcolor = 'rgba(35, 132, 24, 0.75)'
    go_predictions.increasing.line.color = 'rgba(35, 132, 24, 0.75)'
    go_predictions.increasing.line.width = 1
    go_predictions.decreasing.fillcolor = 'rgba(199, 52, 59, 0.75)'
    go_predictions.decreasing.line.color = 'rgba(199, 52, 59, 0.75)'
    go_predictions.decreasing.line.width = 1

    fig = make_subplots(specs=[[{"secondary_y": True, "r": -0.06}]])
    fig.add_trace(go_candles, secondary_y=False)
    fig.add_trace(go_volumes, secondary_y=True)

    for x in [t for t in np.append(times, times_predict) if t.hour == 10]:
        fig.add_vline(x=x, line_dash="dash", opacity=0.2, line_width=1, line_color='grey')

    fig.add_trace(go_compare, secondary_y=False)
    fig.add_trace(go_predictions, secondary_y=False)

    fig.update_layout(xaxis_rangeslider_visible=False,
                      dragmode='pan',
                      margin=dict(l=0, r=0, b=0, t=0, pad=10),
                      hovermode="x",
                      xaxis_tickformat='%d',
                      plot_bgcolor='rgba(0, 0, 0, 0)',
                      paper_bgcolor='rgba(0, 0, 0, 0)',
                      hoverlabel=dict(font_size=12)
                      )
    fig.update_xaxes(rangebreaks=[dict(values=missing_dates, dvalue=60 * 60 * 1000)],
                     tick0=datetime(times[0].year, times[0].month, times[0].day, 10),
                     dtick=24 * 60 * 60 * 1000,
                     showspikes=True,
                     spikemode="across",
                     spikesnap="cursor",
                     spikecolor='grey',
                     spikethickness=-2,
                     color='grey'
                     )
    fig.update_yaxes(showspikes=True,
                     spikemode='across',
                     spikesnap="cursor",
                     spikecolor='grey',
                     spikethickness=-2,
                     color='grey'
                     )

    fig.update_layout({'yaxis2': {'side': 'left',
                                  'visible': False,
                                  'showgrid': False,
                                  },
                       'yaxis': {'showgrid': False,
                                 },
                       'xaxis': {'showgrid': False,
                                 'range': [times[-75], times_predict[-1] + timedelta(hours=2)],
                                 'hoverformat': '%d.%m - %H:00',
                                 }
                       })

    return fig


def get_candles(ticker,
                mongo_conn='mongodb://operate_database:operate_database@mongo:27017/',
                collection='investing', ):
    """


    :return:
    """
    client = MongoClient(mongo_conn)
    cursor = client[collection][ticker]
    request_mongo = list(cursor.find(sort=[('time', -1)],
                                     projection={'_id': False,
                                                 'time': True,
                                                 'open': True,
                                                 'high': True,
                                                 'low': True,
                                                 'close': True,
                                                 'volume': True}).limit(500))[::-1]

    times, opens, highs, lows, closes, volumes = list(zip(*[list(fr.values()) for fr in request_mongo]))
    times = list(map(lambda x: datetime.strptime(x, '%Y-%m-%d %H:00:00'), times))
    predict_cursor = client['investing'][f'{ticker}_predictions']
    predicts = list(predict_cursor.find(sort=[('time', -1)],
                                        projection={'_id': False}).limit(100))[::-1]
    times_pr, opens_pr, highs_pr, lows_pr, closes_pr = list(zip(*[list(fr.values()) for fr in predicts]))
    times_pr = list(map(lambda x: datetime.strptime(x, '%Y-%m-%d %H:00:00'), times_pr))

    client.close()

    fig = get_figure(times, opens, highs, lows, closes, volumes,
                     times_pr, opens_pr, highs_pr, lows_pr, closes_pr)
    graph_plotly = json.dumps(fig, cls=PlotlyJSONEncoder)

    return graph_plotly


def get_logger():
    """

    :return:
    """
    logger = logging.getLogger('logger')
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")

    stream_logging = logging.StreamHandler(sys.stdout)
    stream_logging.setFormatter(formatter)
    stream_logging.setLevel(logging.INFO)

    file_logging = RotatingFileHandler('logs/app.log', 'w', maxBytes=1024 * 5, backupCount=2, encoding='utf-8')
    file_logging.setFormatter(formatter)
    file_logging.setLevel(logging.INFO)

    file_logging_error = RotatingFileHandler('logs/app_error.log', 'w', maxBytes=1024 * 5, backupCount=2,
                                             encoding='utf-8')
    file_logging_error.setFormatter(formatter)
    file_logging_error.setLevel(logging.ERROR)

    logger.addHandler(stream_logging)
    logger.addHandler(file_logging)
    logger.addHandler(file_logging_error)

    return logger
