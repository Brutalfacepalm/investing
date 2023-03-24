from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from plotly.subplots import make_subplots
import plotly.graph_objects as go


def get_figure_previous(times, opens, highs, lows, closes, volumes, predictions):
    predictions_t = np.array([])
    t_start = times[-1]

    for i in range(0, predictions['t']):
        t = t_start + timedelta(hours=i)
        if t.hour > 19:
            t_start += timedelta(hours=14)
            t += timedelta(hours=14)
        predictions_t = np.append(predictions_t, t)

    predictions_v = np.array([round(closes[-1] + round(predictions['v'], 2)*i/10, 2) for i in range(0, predictions['t'])])

    predictions_std = np.array([closes[-1]*predictions['lh']*i/1000 for i in range(0, predictions['t'])])

    missing_dates = pd.date_range(start=times[0],
                                  end=predictions_t[-1], freq='1H').difference(list(times) + list(predictions_t)).tolist()

    colors_value = ['rgba(199, 52, 59, .3)' if closes[i] < opens[i] else 'rgba(35, 132, 24, .3)' for i in
                    range(len(closes))]

    go_candles = go.Candlestick(x=times,
                                open=opens,
                                high=highs,
                                low=lows,
                                close=closes,
                                showlegend=False,
                                hoverinfo='y+x',
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
                        hoverlabel=dict(bordercolor=colors_value, bgcolor=colors_value)
                        )
    go_predictions = [go.Scatter(x=predictions_t,
                                 y=predictions_v,
                                 showlegend=False,
                                 hoverinfo='y+x',
                                 mode='lines',
                                 marker=dict(color='rgba(199, 52, 59, .3)' if predictions_v[-1] < closes[
                                     -1] else 'rgba(35, 132, 24, .3)'),
                                 ),
                      go.Scatter(x=predictions_t,
                                 y=predictions_v + predictions_std,
                                 marker=dict(color='rgba(199, 52, 59, .1)' if predictions_v[-1] < closes[
                                     -1] else 'rgba(35, 132, 24, .1)'),
                                 line=dict(width=0),
                                 showlegend=False,
                                 hoverinfo='y+x',
                                 mode='lines'
                                 ),
                      go.Scatter(x=predictions_t,
                                 y=predictions_v - predictions_std,
                                 marker=dict(color='rgba(199, 52, 59, .1)' if predictions_v[-1] < closes[
                                     -1] else 'rgba(35, 132, 24, .1)'),
                                 line=dict(width=0),
                                 showlegend=False,
                                 hoverinfo='y+x',
                                 fill='tonexty',
                                 mode='lines'
                                 ),
                      ]

    fig = make_subplots(specs=[[{"secondary_y": True, "r": -0.06}]])

    fig.add_trace(go_candles,
                  secondary_y=False)
    fig.add_trace(go_volumes,
                  secondary_y=True)

    for x in [t for t in np.append(times, predictions_t) if t.hour == 10]:
        fig.add_vline(x=x, line_dash="dash", opacity=0.2, line_width=1, line_color='white')

    for p in go_predictions:
        fig.add_trace(p,
                      secondary_y=False)

    fig.update_layout(xaxis_rangeslider_visible=False,
                      dragmode='pan',
                      margin=dict(l=0, r=0, b=0, t=0, pad=0),
                      hovermode="x",
                      xaxis_tickformat='%d',
                      plot_bgcolor='rgba(0, 0, 0, 0)',
                      paper_bgcolor='rgba(0, 0, 0, 0)',
                      hoverlabel=dict(font_size=12)
                      )
    fig.update_xaxes(rangebreaks=[dict(values=missing_dates,
                                       dvalue=60 * 60 * 1000)],
                     tick0=datetime(times[0].year, times[0].month, times[0].day, 10),
                     dtick=24 * 60 * 60 * 1000,
                     showspikes=True,
                     spikemode="across",
                     spikesnap="cursor",
                     spikecolor='white',
                     spikethickness=-2,
                     color='white'
                     )
    fig.update_yaxes(showspikes=True,
                     spikemode='across',
                     spikesnap="cursor",
                     spikecolor='white',
                     spikethickness=-2,
                     color='white'
                     )
    fig.update_layout({'yaxis2': {'side': 'left',
                                  'visible': False,
                                  'showgrid': False,
                                  },
                       'yaxis': {'showgrid': False,
                                 }})
    fig.update_layout({'xaxis': {'showgrid': False,
                                 'range': [times[-50], predictions_t[-1]],
                                 'hoverformat': '%d.%m - %H:00',
                                 }})


    return fig
    # fig.show(config=config)


def get_figure(times, opens, highs, lows, closes, volumes,
               times_predict, opens_predict, highs_predict, lows_predict, closes_predict):

    missing_dates = pd.date_range(start=times[0],
                                  end=times_predict[-1], freq='1H').difference(
        list(times) + list(times_predict)).tolist()

    colors_value = ['rgba(199, 52, 59, .3)' if closes[i] < opens[i] else 'rgba(35, 132, 24, .3)' for i in
                    range(len(closes))]

    go_candles = go.Candlestick(x=times,
                                open=opens,
                                high=highs,
                                low=lows,
                                close=closes,
                                showlegend=False,
                                hoverinfo='y+x',
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
                        hoverlabel=dict(bordercolor=colors_value, bgcolor=colors_value)
                        )
    go_compare = go.Candlestick(x=times_predict[:-9],
                                open=opens_predict[:-9],
                                high=highs_predict[:-9],
                                low=lows_predict[:-9],
                                close=closes_predict[:-9],
                                showlegend=False,
                                hoverinfo='skip',
                                opacity=0.25

                                )
    go_compare.increasing.fillcolor = 'blue'
    go_compare.increasing.line.color = 'blue'
    go_compare.increasing.line.width = 1

    go_compare.decreasing.fillcolor = 'grey'
    go_compare.decreasing.line.color = 'grey'
    go_compare.decreasing.line.width = 1

    go_predictions = go.Candlestick(x=times_predict[-9:],
                                    open=opens_predict[-9:],
                                    high=highs_predict[-9:],
                                    low=lows_predict[-9:],
                                    close=closes_predict[-9:],
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

    fig.add_trace(go_candles,
                  secondary_y=False)
    fig.add_trace(go_volumes,
                  secondary_y=True)

    for x in [t for t in np.append(times, times_predict) if t.hour == 10]:
        fig.add_vline(x=x, line_dash="dash", opacity=0.2, line_width=1, line_color='white')

    fig.add_trace(go_compare,
                  secondary_y=False)
    fig.add_trace(go_predictions,
                  secondary_y=False)

    fig.update_layout(xaxis_rangeslider_visible=False,
                      dragmode='pan',
                      margin=dict(l=0, r=0, b=0, t=0, pad=0),
                      hovermode="x",
                      xaxis_tickformat='%d',
                      plot_bgcolor='rgba(0, 0, 0, 0)',
                      paper_bgcolor='rgba(0, 0, 0, 0)',
                      hoverlabel=dict(font_size=12)
                      )
    fig.update_xaxes(rangebreaks=[dict(values=missing_dates,
                                       dvalue=60 * 60 * 1000)],
                     tick0=datetime(times[0].year, times[0].month, times[0].day, 10),
                     dtick=24 * 60 * 60 * 1000,
                     showspikes=True,
                     spikemode="across",
                     spikesnap="cursor",
                     spikecolor='white',
                     spikethickness=-2,
                     color='white'
                     )
    fig.update_yaxes(showspikes=True,
                     spikemode='across',
                     spikesnap="cursor",
                     spikecolor='white',
                     spikethickness=-2,
                     color='white'
                     )
    fig.update_layout({'yaxis2': {'side': 'left',
                                  'visible': False,
                                  'showgrid': False,
                                  },
                       'yaxis': {'showgrid': False,
                                 }})
    fig.update_layout({'xaxis': {'showgrid': False,
                                 'range': [times[-50], times_predict[-1]],
                                 'hoverformat': '%d.%m - %H:00',
                                 }})

    return fig