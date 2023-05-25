from flask import Flask, render_template, request
import click
from utils import get_logger, get_candles

app = Flask(__name__)
app.config.update(
    CSRF_ENABLED=True,
    # SECRET_KEY='you-will-never-guess',
)
START_TICKER = 'gazp'
logger = get_logger()


@app.route("/", methods=('GET', 'POST'))
def index():
    """


    :return:
    """
    response = {'tickers': ['SBER', 'GAZP', 'LKOH', 'GMKN', 'SNGS', 'AAPL', 'FDX', 'GS', 'IBM', 'TIC5',
                            'TIC6', 'TIC7', 'TIC8', 'TIC9', 'TI10', 'TI11', 'TI12', 'TI13', 'TI14', 'TI15'],
                'active': START_TICKER.upper(),
                'graphs': [['g1', '1d', '1 days', None]],
                }

    if request.method == 'POST':
        if request.form.get('select_ticker'):
            update_ticker = request.form.get('select_ticker')
            response['active'] = update_ticker.upper()
            try:
                figure = get_candles(update_ticker.lower())
                response['graphs'][-1][-1] = figure
            except Exception as e:
                print(f'Error getting candlesticks by ticker {update_ticker}: {e}.')

        return render_template('index.html', response=response)

    elif request.method == 'GET':
        try:
            figure = get_candles(START_TICKER.lower())
            response['graphs'][-1][-1] = figure
        except Exception as e:
            print(f'Error getting candlesticks by ticker {START_TICKER}: {e}.')

        return render_template('index.html', response=response)


@click.command()
@click.option('--host', '-h', default='127.0.0.1')
@click.option('--port', '-p', default='8000')
@click.option('--debug', default=True)
def run_app(host, port, debug):
    """


    :param host:
    :param port:
    :param debug:
    :return:
    """
    app.run(host, port, debug)


if __name__ == '__main__':
    run_app()
