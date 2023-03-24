import logging
from logging.handlers import RotatingFileHandler
import sys
import json
import pandas as pd
import numpy as  np
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from datetime import datetime
import psycopg2
import psycopg2.extras as extras
from sqlalchemy import create_engine


PERIOD=7
FINAM_URL = 'http://export.finam.ru/'
MARKET = 1
APPLY=0
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
    def __init__(self, ticker, from_, to_):
        self.ticker = ticker
        self.from_ = from_
        self.to_ = to_
        self.tickers_code = self.get_tickers_codes()
        self.headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) '
                                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        self.url_suffix = [('dtf', DTF), ('tmf', TMF), ('MSOR', MSOR), ('mstime', MSTIME), 
                           ('mstimever', MSTIMEVER), ('sep', SEP), ('sep2', SEP2), ('datf', DATF), ('at', AT)]
        self.ticker_horly_data = {'time':[], 
                                  'open':[], 'high':[], 'low':[], 'close':[], 'volume':[]}
    
    def get_tickers_codes(self):
        with open('tickers.json', 'r+') as f:
            tickers = json.load(f)
        return tickers

    
    def get_dates_by_year(self, from_, to_):
        dates = []
        new_from_ = from_
        while True:
            new_to_ = (pd.Timestamp(new_from_)+pd.Timedelta(value=365, unit='D')).strftime('%d.%m.%Y')
            if datetime.now().date() < pd.Timestamp(new_to_):
                dates.append([new_from_, datetime.now().date().strftime('%d.%m.%Y')])        
                break
            elif pd.Timestamp(to_) < pd.Timestamp(new_to_):    
                break
            else:
                dates.append([new_from_, new_to_])
            new_from_ = new_to_
        return dates
    
    
    def get_url_period(self, ticker, start, end):

        start_date = datetime.strptime(start, '%d.%m.%Y')
        end_date = datetime.strptime(end, '%d.%m.%Y')
        file_name = f'{ticker}_{start.replace(".", "")}_{end.replace(".", "")}'
        file_name_ext = '.txt'

        params = urlencode([
            ('market', MARKET),
            ('em', self.tickers_code[self.ticker]),
            ('code', self.ticker),
            ('apply', APPLY),
            ('df', start_date.day),
            ('mf', start_date.month - 1),
            ('yf', start_date.year),
            ('from', start),
            ('dt', end_date.day),
            ('mt', end_date.month - 1),
            ('yt', end_date.year),
            ('to', end),
            ('p', PERIOD),
            ('f', file_name),
            ('e', file_name_ext),
            ('cn', self.ticker),
            ] + self.url_suffix)

        url_period = FINAM_URL + file_name + f'{file_name_ext}?' + params
        return url_period
    
    
    def get_data_period(self, url):
        query = Request(url)
        query.add_header('User-Agent', self.headers['User-Agent'])
        
        data_period = urlopen(query).readlines()
        
        return data_period
    
    
    def get_candles(self):
        all_data = []
        for start, end in self.get_dates_by_year(self.from_, self.to_):
            url_period = self.get_url_period(self.ticker, start, end)
            data_period = self.get_data_period(url_period)
            all_data += data_period
            
        return all_data
    
    
    def get_all_data_as_df(self, all_data):
        for h_data in all_data:
            h_data = h_data.strip().decode("utf-8").split(',')
            
            y = int(h_data[2][:4])
            m = int(h_data[2][4:6])
            d = int(h_data[2][6:])
            h = int(h_data[3][:2])
            date_time = pd.Timestamp(datetime(y, m, d, h, 0, 0))
            
            self.ticker_horly_data['time'].append(date_time)
            self.ticker_horly_data['open'].append(h_data[4])
            self.ticker_horly_data['high'].append(h_data[5])
            self.ticker_horly_data['low'].append(h_data[6])
            self.ticker_horly_data['close'].append(h_data[7])
            self.ticker_horly_data['volume'].append(h_data[8])
            
        data_as_df = pd.DataFrame.from_dict(self.ticker_horly_data)
        data_as_df[['open', 'high', 'low', 'close']] = data_as_df[['open', 'high', 'low', 'close']].astype(float)
        data_as_df[['volume']] = data_as_df[['volume']].astype(int)
        return data_as_df
    
    
    def parse(self):
        all_data = self.get_candles()
        self.data_as_df = self.get_all_data_as_df(all_data)
        self.data_as_df = self.data_as_df.loc[self.data_as_df['time'].dt.hour.between(10, 19)]
        return self.data_as_df


class FeatureCreator:
    
    def __init__(self, data_as_df):
        self.df = data_as_df
        self.hours_ma = [5, 10, 20, 30, 40, 50, 70, 140, 200, 250, 300, 350]
        self.hours_rsi = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 120, 140, 160,
                          200, 250, 300, 350, 400, 500, 600, 700, 800, 1000]
        self.hours_macd = [[12, 26, 9], [16, 32, 12], [20, 38, 15], 
                           [24, 44, 18], [28, 50, 21], [32, 56, 24], 
                           [36, 62, 27], [40, 68, 30], [44, 74, 33],
                           [48, 80, 36], [52, 86, 39], [56, 92, 42],
                           [60, 98, 45], [68, 110, 51], [76, 122, 57],
                           [84, 134, 63], [92, 146, 69], [100, 158, 75], [108, 170, 81]]
        self.hours_atr = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 120, 140, 160]
        self.hours_general_points = [10, 20, 40, 80, 160, 320, 640, 1280]
        self.hours_trens = [10, 15, 20, 25, 30, 50, 100, 150, 200, 300, 500, 1000]
        self.hours_ao = [[5, 34], [10, 68], [15, 102], [20, 136], [30, 204], [40, 272], [50, 340], [100, 680]]
        self.hours_aligator = [[3, 5, 8, 13],
                               [8, 12, 18, 28],
                               [12, 20, 32, 52],
                               [20, 36, 60, 10],
                               [36, 68, 116, 196],
                               [68, 132, 228, 388],
                               [132, 260, 452, 772],]

        
    def diff_and_div(self, df):
        """
        Формируем следующие признаки:
            (c-o)/o - разница закрытия и открытия по отношению к открытию
                показывает насколько за период мы изменились по отношению к открытию
                напрмиер открылись по 100, закрылись на 110, итого отношение 0.1
            (l-o)/o - разница минимума и открытия по отношению к открытию
                показывает насколько за период мы падали от открытия по отношению к открытию
                напрмиер открылись по 100, нижняя точка за период была на 90, итого отношение -0.1
            (h-o)/o - разница максимума и открытия по отношению к открытию
                показывает насколько за период мы росли от открытия по отношению к открытию
                напрмиер открылись по 100, максимум был на 120, итого отношение 0.2
            (c-o)/c - разница закрытия и открытия по отношению к закрытию
                показывает насколько за период мы изменились по отношению к закрытию
                напрмиер открылись по 100, закрылись на 110, итого отношение ~0.0909
            (c-l)/c - разница закрытия и минимума по отношению к закрытию
                показывает насколько за период мы падали к закрытию по отношению к закрытию
                напрмиер закрылись мы по 110, нижняя точка за период была на 90, итого отношение 0.2
            (c-h)/c - разница закрытия и максимума по отношению к закрытию
                показывает насколько за период мы росли к закрытию по отношению к закрытию
                напрмиер закрылись мы по 110, максимум был на 120, итого отношение -0.1

            (c-l)/(o-l) - разница закрытия и минимума по отношению к разнице открытия и минмума
                показывает какая доля к закрытию отыграна по отношению к падению от открытия
                напрмиер открылись по 100, закрылись по 105, минимум был на 90, итого отношение 1.5
            (h-c)/(h-o) - разница максимума и открытия по отношению к разнице максимума и закрытия
                показывает на какую долю мы упали от максимума к закрытию по отношению к скачку от открытия до максимума
                напрмиер открылись по 100, закрылись по 105, максимум был на 120, итого отношение 0.75

            (c-o)/(h-l) - разница закрытия и окрытия по отношению к разнице максимума и минмума
                показывает насколько скакала свеча к телу
                напрмиер открылись по 100, закрылись по 105, минимум был на 90, максимум на 120 итого отношение ~0.1666
                
            (h-max(o,c))/(min(o,c)-l) - показывает отношенией теней свечи
                напрмиер открылись по 100, закрылись по 105, минимум был на 90, максимум на 120 итого отношение 1.5
            (h-max(o,c))/abs(c-o) - показывает отношенией верхней тени свечи к телу
                напрмиер открылись по 100, закрылись по 105, максимум на 120 итого отношение 3
            (min(o,c)-l)/abs(c-o) - показывает отношенией нижней тени свечи к телу
                напрмиер открылись по 100, закрылись по 105, минимум на 90 итого отношение 2            
        """
    
        df['(c-o)/o'] = (df['close'] - df['open'])/df['open']
        df['(l-o)/o'] = (df['low'] - df['open'])/df['open']
        df['(h-o)/o'] = (df['high'] - df['open'])/df['open']

        df['(c-o)/c'] = (df['close'] - df['open'])/df['close']
        df['(c-l)/c'] = (df['close'] - df['low'])/df['close']
        df['(c-h)/c'] = (df['close'] - df['high'])/df['close']

        df['(c-l)/(o-l)'] = (df['close'] - df['low'])/(df['open'] - df['low'])
        df['(h-c)/(h-o)'] = (df['high'] - df['close'])/(df['high'] - df['open'])
        df['(c-o)/(h-l)'] = (df['close'] - df['open'])/(df['high'] - df['low'])

        df['(h-max(o,c))/(min(o,c)-l)'] = (df['high'] - np.max([df['open'], df['close']], axis=0)) / \
                                          (np.min([df['open'], df['close']], axis=0) - df['low'])

        df['(h-max(o,c))/abs(c-o)'] = (df['high'] - np.max([df['open'], df['close']], axis=0))/ \
                                        abs(df['close'] - df['open'])
        df['(min(o,c)-l)/abs(c-o)'] = (np.min([df['open'], df['close']], axis=0) - df['low'])/ \
                                        abs(df['close'] - df['open'])

        return df
    

    def gap(self, df):
        df['gap'] = np.append([np.nan], df['open'].to_numpy()[1:]-df['close'].to_numpy()[:-1])
        df['gap/o'] = df['gap']/df['open']

        return df


    def ma(self, df):
        for hour in self.hours_ma:
            df[f'ma_close_{hour}h'] = df['close'].rolling(hour).mean()
            df[f'(ma_close_{hour}h-c)/c'] = (df[f'ma_close_{hour}h'] - df['close'])/df['close']

        df['oc'] = df['close'] - df['open']
        df['m_p'] = (df['open'] + df['close'])/2 - df['close']
        df['v_p'] = df['high'] - df['low']

        df['percent_oc_c'] = df['oc']/df['close']
        df['percent_m_p_c'] = df['m_p']/df['close']
        df['percent_v_p_c'] = df['v_p']/df['close']
        df['percent_oc_o'] = df['oc']/df['open']
        df['percent_m_p_o'] = df['m_p']/df['open']
        df['percent_v_p_o'] = df['v_p']/df['open']

        for hour in self.hours_ma:
            df[f'ma_oc_{hour}h'] = df['oc'].rolling(hour).mean()
            df[f'ma_percent_oc_{hour}h'] = df['percent_oc_c'].rolling(hour).mean()
            df[f'ma_oc_{hour}h/o'] = df[f'ma_oc_{hour}h']/df['open']
            df[f'ma_oc_{hour}h/c'] = df[f'ma_oc_{hour}h']/df['close']

        return df

    
    def rsi(self, df):
        for hour in self.hours_rsi:
            delta = df['close'].diff()
            up = delta.clip(lower=0)
            down = -1*delta.clip(upper=0)
            ema_up = up.ewm(com=hour, adjust=False).mean()
            ema_down = down.ewm(com=hour, adjust=False).mean()
            rs = ema_up/ema_down

            rsi = 100 - (100/(1 + rs))
            df[f'rsi_{hour}'] = rsi
        return df

    
    def macd(self, df):
        for hour_i, hour_j, hour_k in self.hours_macd:
            exp1 = df['close'].ewm(span=hour_i, adjust=False).mean()
            exp2 = df['close'].ewm(span=hour_j, adjust=False).mean()
            macd = exp1 - exp2
            exp3 = macd.ewm(span=hour_k, adjust=False).mean()
            df[f'macd_{hour_i}_{hour_j}_{hour_k}'] = macd
            df[f'macd_signal_line_{hour_i}_{hour_j}_{hour_k}'] = exp3
            df[f'macd_diff_{hour_i}_{hour_j}_{hour_k}'] = macd - exp3
        return df

    
    def atr(self, df):
        high_low = df['high'] - df['low']
        for hour in self.hours_atr:
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            true_range = np.max(ranges, axis=1)
            atr = true_range.rolling(hour).sum()/hour

            df[f'atr_{hour}'] = atr
        return df


    def general_points(self, df):
        df['support_10'] = df['close'].rolling(10).min()
        df['resist_10'] = df['close'].rolling(10).max()
        
        for hour in self.hours_general_points:
            df[f'support_{hour*2}'] = df[f'support_{hour}'].rolling(hour).min()
            df[f'support_{hour*2}_mean'] = df[f'support_{hour}'].rolling(hour).mean()
            df[f'resist_{hour*2}'] = df[f'resist_{hour}'].rolling(hour).max()
            df[f'resist_{hour*2}_mean'] = df[f'resist_{hour}'].rolling(hour).mean()

            if hour >= 80:
                df[f'support_{hour*2}'].fillna(df[f'support_{hour}'], inplace=True)
                df[f'support_{hour*2}_mean'].fillna(df[f'support_{hour}_mean'], inplace=True)
                df[f'resist_{hour*2}'].fillna(df[f'resist_{hour}'], inplace=True)
                df[f'resist_{hour*2}_mean'].fillna(df[f'resist_{hour}_mean'], inplace=True)
                

            df[f'support_{hour*2}-close'] = df[f'support_{hour*2}'] - df['close']
            df[f'support_{hour*2}_mean-close'] = df[f'support_{hour*2}_mean'] - df['close']
            df[f'resist_{hour*2}-close'] = df[f'resist_{hour*2}'] - df['close']
            df[f'resist_{hour*2}_mean-close'] = df[f'resist_{hour*2}_mean'] - df['close']
        return df


    def trend(self, df):
        for hour in self.hours_trens:
            mean_values = []
            std_values = []
            diff_values = []

            for i, _ in enumerate(df['close']):
                idx_start = i//hour * hour
                idx_end = (i//hour + 1) * hour

                mean_values.append(df['close'].iloc[idx_start: idx_end].mean())
                std_values.append(df['close'].iloc[idx_start: idx_end].std())

                if i < hour:
                    diff_values.append(0)
                else:
                    diff_values.append(mean_values[-1]-mean_values[-hour-1])        



            merge_mean_values = []
            trend_mean_values = {df['time'].iloc[0]: df['close'].iloc[0]}
            trend_values = []

            diff_idx_start = 0
            diff_idx_end = 1

            while diff_idx_end < len(diff_values):
                if np.sign(diff_values[diff_idx_start]) == np.sign(diff_values[diff_idx_end]):
                    diff_idx_end += 1
                else:
                    merge_mean_values.extend([np.mean(mean_values[diff_idx_start: diff_idx_end])]*(diff_idx_end-diff_idx_start))

                    end_trend = mean_values[diff_idx_end-1]
                    trend_mean_values[df['time'].iloc[diff_idx_end-1]] = end_trend
                    step_trend = (mean_values[diff_idx_end-1] - 
                                  mean_values[diff_idx_start]) / \
                                 (diff_idx_end - diff_idx_start)
                    trend_values.append(mean_values[diff_idx_start])
                    trend_values.extend([mean_values[diff_idx_start] + step_trend*i for i in range(1, diff_idx_end - diff_idx_start)])

                    diff_idx_start = diff_idx_end
                    diff_idx_end += 1

                if diff_idx_end == len(diff_values):
                    merge_mean_values.extend([np.mean(mean_values[diff_idx_start: diff_idx_end])]*(diff_idx_end-diff_idx_start))

                    end_trend = mean_values[diff_idx_end-1]
                    trend_mean_values[df['time'].iloc[-1]] = end_trend
                    step_trend = (mean_values[diff_idx_end-1] - 
                                  mean_values[diff_idx_start]) / \
                                 (diff_idx_end - diff_idx_start)
                    trend_values.append(mean_values[diff_idx_start])
                    trend_values.extend([mean_values[diff_idx_start] + step_trend*i for i in range(1, diff_idx_end - diff_idx_start)])

                    break

            df[f'mean_subtrend_{hour}'] = mean_values
            df[f'std_subtrend_{hour}'] = std_values
            df[f'trend_values_{hour}'] = trend_values
            df[f'diff_trend_values_{hour}'] = df['close'] - trend_values
        return df


    def ao(self, df):
        for hour_mn, hour_mj in self.hours_ao:
            median_price = (df['high'] + df['low'])/2
            sma_mn = median_price.rolling(hour_mn).mean()
            sma_mj = median_price.rolling(hour_mj).mean()
            ao = sma_mn - sma_mj
            df[f'ao_{hour_mn}_{hour_mj}'] = ao
        return df


    def ac(self, df):
        for hour_mn, hour_mj in self.hours_ao:
            median_price = (df['high'] + df['low'])/2
            sma_mn = median_price.rolling(hour_mn).mean()
            sma_mj = median_price.rolling(hour_mj).mean()
            ao = sma_mn - sma_mj
            sma_ao = ao.rolling(hour_mn).mean()
            df[f'ac_{hour_mn}'] = ao - sma_ao
        return df


    def alligator(self, df):
        median_price = (df['high'] + df['low']) / 2
        for h1, h2, h3, h4 in self.hours_aligator:
            sum_lips = median_price.rolling(h2).sum()
            diff_lips = (sum_lips / h2).shift(h1)
            smma_lips = (sum_lips - diff_lips + median_price) / h2

            sum_teeth = median_price.rolling(h3).sum()
            diff_teeth = (sum_teeth / h3).shift(h2)
            smma_teeth = (sum_teeth - diff_teeth + median_price) / h3

            sum_jaw = median_price.rolling(h4).sum()
            diff_jaw = (sum_jaw / h4).shift(h3)
            smma_jaw = (sum_jaw - diff_jaw + median_price) / h4

            df[f'lips_{h2}_{h1}'] = smma_lips
            df[f'teeth_{h3}_{h2}'] = smma_teeth
            df[f'jaw_{h4}_{h3}'] = smma_jaw
        return df


    def ema(self, df):
        for hour in self.hours_ma:
            df[f'ewa_{hour}'] = df['close'].ewm(span=hour,
                                                min_periods=0,
                                                adjust=False,
                                                ignore_na=False).mean()
        return df


    def brp(self, df):
        for hour in self.hours_ma:
            df[f'bears_{hour}'] = df['low'] - df['low'].ewm(span=hour,
                                                            min_periods=0,
                                                            adjust=False,
                                                            ignore_na=False).mean()
        return df


    def blp(self, df):
        for hour in self.hours_ma:
            df[f'bulls_{hour}'] = df['high'] - df['high'].ewm(span=hour,
                                                            min_periods=0,
                                                            adjust=False,
                                                            ignore_na=False).mean()
        return df


    def cci(self, df):
        tp = (df['high'] + df['low'] + df['close']) / 3
        eps = 1e-6
        for hour in self.hours_ma:
            sma_tp = tp.rolling(hour).mean()
            d = tp - sma_tp + eps
            m = d.rolling(hour).mean() * 0.015
            df[f'cci_{hour}'] = m / d
        return df


    def dem(self, df):
        eps = 1e-6
        for hour in self.hours_ma:
            demax = df['high'] - df['high'].shift(hour)
            demin = df['low'].shift(hour) - df['low']
            demax = demax.mask(demax < 0, 0)
            demin = demin.mask(demin < 0, 0)
            ma_demax = demax.rolling(hour).mean() + eps
            ma_demin = demin.rolling(hour).mean() + eps
            df[f'demark_{hour}'] = ma_demax/(ma_demax + ma_demin)
        return df


    def envlp(self, df):
        k = 10
        for hour in self.hours_ma:
            upper_band = df['close'].rolling(hour).mean() * (1 + k/1000)
            lower_band = df['close'].rolling(hour).mean() * (1 - k/1000)
            df[f'envlp_up_{hour}'] = upper_band
            df[f'envlp_low_{hour}'] = lower_band
            df[f'diff_nearest_band_{hour}'] = np.min((abs(df['close']-lower_band), abs(upper_band-df['close'])), axis=0)
        return df


    def mf(self, df):
        hl = df['high'] - df['low']
        df['mf_0'] = hl / df['volume']
        for hour in self.hours_ma:
            df[f'mf_{hour}'] = hl.rolling(hour).mean() / df['volume'].rolling(hour).mean()
        return df


    def mfi(self, df):
        tp = (df['high'] + df['low'] + df['close']) / 3
        money_flow = tp * df['volume']

        pmf = money_flow.mask(money_flow.diff(1) < 0, 0)
        nmf = money_flow.mask(money_flow.diff(1) > 0, 0)

        for hour in self.hours_ma:
            money_ratio = pmf.rolling(hour).sum()/nmf.rolling(hour).sum()
            df[f'mfi_{hour}'] = 100 - (100 / (1 + money_ratio))
        return df


    def obv(self, df):
        for hour in self.hours_ma:
            obv_0 = 0
            closes = df['close'].values
            volumes = df['close'].values
            obv_i = [0]*hour
            for i in range(hour, df.shape[0]):
                if closes[i] > closes[i-hour]:
                    obv_i.append(obv_0 + volumes[i])
                    obv_0 += volumes[i]
                elif closes[i] < closes[i-hour]:
                    obv_i.append(obv_0 - volumes[i])
                    obv_0 -= volumes[i]
                else:
                    obv_i.append(obv_0)
            df[f'obv_{hour}'] = obv_i
        return df


    def sar(self, df):
        acceleration = 0.02
        for hour in self.hours_ma:
            highs = df['high'].values
            sar_i_high = [0]*hour
            for i in range(hour, df.shape[0]):
                sar_0_high = (highs[i - hour] - sar_i_high[i - hour]) * acceleration + sar_i_high[i - hour]
                sar_i_high.append(sar_0_high)

            df[f'sar_{hour}'] = sar_i_high
        return df


    def sd(self, df):
        for hour in self.hours_ma:
            df[f'sd_{hour}'] = (((df['close'] - df['close'].rolling(hour).mean())**2).rolling(hour).mean()) ** 0.5
        return df


    def so(self, df):
        for hour in self.hours_ma:
            k = (df['close'] - df['low'].rolling(hour).min()) / \
            (df['high'].rolling(hour).max() - df['low'].rolling(hour).min()) * 100
            d = k.rolling(hour).mean()
            df[f'so_{hour}'] = d
        return df


    def generate_feature(self):
        self.df = self.df.loc[self.df['time'].dt.hour.between(10, 19)]
        from time import time
        t0 = time()
        tf = time()
        self.df = self.diff_and_div(self.df)
        print('diff_and_div DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.gap(self.df)
        print('gap DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.ma(self.df)
        print('ma DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.rsi(self.df)
        print('rsi DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.macd(self.df)
        print('macd DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.atr(self.df)
        print('atr DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.general_points(self.df)
        print('general_points DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.trend(self.df)
        print('trend DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()

        self.df = self.ao(self.df)
        print('ao DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.ac(self.df)
        print('ac DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.alligator(self.df)
        print('alligator DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.ema(self.df)
        print('ema DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.brp(self.df)
        print('bears DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.blp(self.df)
        print('bulls DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.cci(self.df)
        print('cci DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.dem(self.df)
        print('dem DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.envlp(self.df)
        print('envlp DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.mf(self.df)
        print('mf DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.mfi(self.df)
        print('mfi DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.obv(self.df)
        print('obv DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.sar(self.df)
        print('sar DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.sd(self.df)
        print('sd DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        tf = time()
        self.df = self.so(self.df)
        print('so DONE')
        print(self.df.dropna(axis=0).shape)
        print(time()-tf)
        print(time()-t0)


        self.df = self.df.dropna(axis=0)

        return self.df


class PostgresLoader:
    def __init__(self, db_name, db_user, db_pass, db_host, db_port):
        # conn_string = f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
        # self.db = create_engine(conn_string)
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name


    def write_to_sql(self, df, ticker):
        # conn = self.db.connect()
        print('CONN')
        connect_db = psycopg2.connect(
            database=self.db_name,
            user=self.db_user,
            password=self.db_pass,
            host=self.db_host,
            port=self.db_port
        )
        print('CONN1')
        # cursor = connect_db.cursor()
        print('cursor')
        with connect_db.cursor() as cursor:
            sql_create_table = 'CREATE TABLE IF NOT EXISTS "{}" (time timestamp, open float, ' \
                               'high float, low float, close float, volume int);'.format(ticker)
            print(sql_create_table)
            cursor.execute(sql_create_table)
            print('execute')
            tuples = [tuple(x) for x in df.to_numpy()]
            print(tuples)
            columns = ','.join(list(df.columns))
            sql_insert = 'INSERT INTO "%s" (%s) VALUES %%s' % (ticker, columns)
            print(sql_insert)
            extras.execute_values(cursor, sql_insert, tuples)
            connect_db.commit()
            print('commit')

        # df.to_sql(f'{ticker}', conn, if_exists='replace', index=False)
        # print('to_sql')
        # #
        # connect_db.commit()
        # print('commit')
        # connect_db.close()
        print('close')

    def read_from_sql(self, ticker):
        conn1 = psycopg2.connect(
            database=self.db_name,
            user=self.db_user,
            password=self.db_pass,
            host=self.db_host,
            port=self.db_port
        )
        cursor = conn1.cursor()
        sql1 = f'SELECT * FROM "{ticker}"'
        print(sql1)
        cursor.execute(sql1)
        cnt = 100
        for i in cursor.fetchall():
            print(i)
            cnt -= 1
            if not cnt:
                break
        conn1.commit()
        conn1.close()


def get_logger():
    logger = logging.getLogger('logger')
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")

    stream_logging = logging.StreamHandler(sys.stdout)
    stream_logging.setFormatter(formatter)
    stream_logging.setLevel(logging.INFO)

    # file_logging = RotatingFileHandler('logs/app.log', 'w', maxBytes=1024 * 5, backupCount=2, encoding='utf-8')
    # file_logging.setFormatter(formatter)
    # file_logging.setLevel(logging.INFO)
    #
    # file_logging_error = RotatingFileHandler('logs/app_error.log', 'w', maxBytes=1024 * 5, backupCount=2,
    #                                          encoding='utf-8')
    # file_logging_error.setFormatter(formatter)
    # file_logging_error.setLevel(logging.ERROR)

    logger.addHandler(stream_logging)
    # logger.addHandler(file_logging)
    # logger.addHandler(file_logging_error)

    return logger