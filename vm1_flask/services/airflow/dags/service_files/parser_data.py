import pandas as pd

import pendulum
from pendulum import from_format
from datetime import datetime
from time import sleep
from urllib.parse import urlencode
from urllib.request import Request, urlopen


PERIOD = 7
FINAM_URL = 'http://export.finam.ru/'
MARKET = 1
APPLY = 0
DTF = 1
TMF = 1
MSOR = 0
MSTIME = 'on'
MSTIMEVER = 1
SEP = 1
SEP2 = 1
DATF = 1
AT = 0


class Parser:
    def __init__(self, ticker, from_, to_=None,
                 split_period='month',
                 is_feature=False,
                 meta_df=None,
                 subdata=False):
        self.ticker = ticker
        self.from_ = from_
        self.to_ = to_
        self.split_period = split_period
        self.is_feature = is_feature
        self.subdata = subdata
        self.short_code = {'BR': 'BR',
                           'BZ': 'BZ',
                           'NG': 'NG',
                           'PT': 'PLT',
                           'GD': 'GOLD',
                           'GC': 'GC',
                           'SV': 'SILV',
                           }
        self.features_codes = dict(zip(range(12),
                                       ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']))
        if self.to_:
            self.last_date_time = from_format(self.to_, 'DD.MM.YYYY HH:00:00', tz='Europe/Moscow')
        else:
            self.last_date_time = pendulum.today(tz='Europe/Moscow')

        self.meta_information = meta_df
        self.headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) '
                                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        self.url_suffix = [('dtf', DTF), ('tmf', TMF), ('MSOR', MSOR), ('mstime', MSTIME),
                           ('mstimever', MSTIMEVER), ('sep', SEP), ('sep2', SEP2), ('datf', DATF), ('at', AT)]

        self.ticker_horly_data = []

    def get_dates_by_period(self, from_, to_):
        dates = []
        new_from_ = from_
        while True:
            if self.split_period == 'month':
                new_to_ = from_format(new_from_, 'DD.MM.YYYY HH:00:00').add(months=1).strftime('%d.%m.%Y %H:00:00')
            elif self.split_period == 'quarter':
                new_to_ = from_format(new_from_, 'DD.MM.YYYY HH:00:00').add(months=3).strftime('%d.%m.%Y %H:00:00')
            elif self.split_period == 'year':
                new_to_ = from_format(new_from_, 'DD.MM.YYYY HH:00:00').add(years=1).strftime('%d.%m.%Y %H:00:00')

            if pd.Timestamp(to_) < pd.Timestamp(new_to_):
                dates.append([new_from_, to_])
                break
            else:
                dates.append([new_from_, new_to_])
            new_from_ = new_to_
        return dates

    def get_url_period(self, ticker, start, end):

        start_date = from_format(start, 'DD.MM.YYYY HH:00:00')
        end_date = from_format(end, 'DD.MM.YYYY HH:00:00')
        file_name = f'{ticker}_{start_date.strftime("%d%m%y")}_{end_date.strftime("%d%m%y")}'
        file_name_ext = '.txt'

        if self.is_feature:
            m = (start_date.month // 3 + 1) * 3 % 15 + ((start_date.month // 3 + 1) * 3 // 15) * 3
            code = f"{self.ticker}{self.features_codes[m - 1]}{start_date.add(months=1).year % 10}"
            name = f"{self.short_code[self.ticker]}-{m}.{start_date.add(months=1).strftime('%y')}"
            curr_df = self.meta_information[(self.meta_information['code'] == code) &
                                            (self.meta_information['name'].str.contains(name))]
            if curr_df.empty:
                return None

            # self.em = curr_df['id'].values[0]
            # self.market = curr_df['market'].values[0]


        else:
            code = self.ticker
            curr_df = self.meta_information[(self.meta_information['code'] == self.ticker) &
                                            (self.meta_information['market'].isin([1, 5, 24, 25, 45]))]

        self.em = curr_df['id'].values[0]
        self.market = curr_df['market'].values[0]

        params = urlencode([
                               ('market', self.market),
                               ('em', self.em),
                               ('code', code),
                               ('apply', APPLY),
                               ('df', start_date.day),
                               ('mf', start_date.month - 1),
                               ('yf', start_date.year),
                               ('from', start_date.strftime("%d.%m.%Y")),
                               ('dt', end_date.day),
                               ('mt', end_date.month - 1),
                               ('yt', end_date.year),
                               ('to', end_date.strftime("%d.%m.%Y")),
                               ('p', PERIOD),
                               ('f', file_name),
                               ('e', file_name_ext),
                               ('cn', code),
                           ] + self.url_suffix)

        url_period = FINAM_URL + file_name + f'{file_name_ext}?' + params
        return url_period

    def get_data_period(self, url):
        print(url)
        query = Request(url)
        query.add_header('User-Agent', self.headers['User-Agent'])

        data_period = urlopen(query).readlines()

        return data_period

    def get_candles(self):
        all_data = []
        for start, end in self.get_dates_by_period(self.from_, self.to_):

            url_period = self.get_url_period(self.ticker, start, end)
            if url_period:
                data_period = self.get_data_period(url_period)
                sleep(.5)
                all_data += data_period

        return all_data

    def get_all_data_as_df(self, all_data):
        for h_data in all_data:
            h_data = h_data.strip().decode("utf-8").split(',')
            y = int(h_data[2][:4])
            m = int(h_data[2][4:6])
            d = int(h_data[2][6:])
            h = int(h_data[3][:2])
            date_time = pd.Timestamp(datetime(y, m, d, h, 0, 0), tz='Europe/Moscow')
            if self.last_date_time < date_time:
                break
            if self.market in [5, 14, 17, 24, 25, 45]:
                start_time = 10
                end_time = 23
            else:
                start_time = 10
                end_time = 19


            if date_time.hour >= start_time and date_time.hour < end_time:
                if self.subdata:
                    self.ticker_horly_data.append([date_time.strftime('%Y-%m-%d %H:00:00'),
                                                   float(h_data[7])])
                else:
                    self.ticker_horly_data.append([date_time.strftime('%Y-%m-%d %H:00:00'),
                                                   float(h_data[4]),
                                                   float(h_data[5]),
                                                   float(h_data[6]),
                                                   float(h_data[7]),
                                                   int(h_data[8])])
        return self.ticker_horly_data

    def parse(self):
        all_data = self.get_candles()
        return self.get_all_data_as_df(all_data)

