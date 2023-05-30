import numpy as np
import pandas as pd
import holidays
import warnings
from numba import jit
from copy import copy

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)


@jit(nopython=True)
def apply_rolling_table(x):
    """

    :param x:
    :return:
    """
    op = x[0, 0]
    max_cl = max(x[:, 1])
    min_cl = min(x[:, 1])
    d = max(abs(op - max_cl) / op, abs(op - min_cl) / op) * 100
    return d


class FeatureCreator:
    """

    """
    def __init__(self, data_as_df):
        """

        :param data_as_df:
        """
        self.eps = 1e-5
        self.df = data_as_df
        self.hours_ma = [20, 30, 40, 50, 60, 70, 80, 100, 125, 150, 175]
        self.hours_rsi = [20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130,
                          140, 150, 160, 175]
        self.hours_macd = [[12, 26, 9], [16, 32, 12], [20, 38, 15],
                           [24, 44, 18], [28, 50, 21], [32, 56, 24],
                           [36, 62, 27], [40, 68, 30], [44, 74, 33],
                           [48, 80, 36], [52, 86, 39], [56, 92, 42],
                           [60, 98, 45], [64, 104, 48], [68, 110, 51],
                           [72, 106, 54], [76, 112, 57], [80, 118, 60], [84, 124, 63]]
        self.hours_atr = [20, 30, 40, 50, 60, 70, 80, 90, 100, 120, 140, 160]
        self.hours_general_points = [80, 120, 160, 200, 240, 280]
        self.hours_trens = [15, 20, 25, 30, 40, 50, 75, 100, 125, 150]
        self.hours_ao = [[5, 34], [10, 44], [15, 54], [20, 64], [25, 74], [30, 84]]
        self.hours_aligator = [[3, 5, 8, 13],
                               [5, 8, 13, 21],
                               [8, 13, 21, 34],
                               [13, 21, 34, 55],
                               [21, 34, 55, 89], ]

    def diff_and_div(self, df):
        """

        :param df:
        :return:
        """
        df['(c-o)/o'] = (df['close'] - df['open']) / df['open']
        df['(l-o)/o'] = (df['low'] - df['open']) / df['open']
        df['(h-o)/o'] = (df['high'] - df['open']) / df['open']

        df['(c-o)/c'] = (df['close'] - df['open']) / df['close']
        df['(c-l)/c'] = (df['close'] - df['low']) / df['close']
        df['(c-h)/c'] = (df['close'] - df['high']) / df['close']

        df['(c-l)/(o-l)'] = (df['close'] - df['low']) / (df['open'] - df['low'] + self.eps)
        df['(h-c)/(h-o)'] = (df['high'] - df['close']) / (df['high'] - df['open'] + self.eps)
        df['(c-o)/(h-l)'] = (df['close'] - df['open']) / (df['high'] - df['low'] + self.eps)

        df['(h-max(o,c))/(min(o,c)-l)'] = (df['high'] - np.max([df['open'], df['close']], axis=0)) / \
                                          (np.min([df['open'], df['close']], axis=0) - df['low'] + self.eps)

        df['(h-max(o,c))/abs(c-o)'] = (df['high'] - np.max([df['open'], df['close']], axis=0)) / \
                                      abs(df['close'] - df['open'] + self.eps)
        df['(min(o,c)-l)/abs(c-o)'] = (np.min([df['open'], df['close']], axis=0) - df['low']) / \
                                      abs(df['close'] - df['open'] + self.eps)

        return df

    @staticmethod
    def gap(df):
        """

        :param df:
        :return:
        """
        df['gap'] = np.append([0], df['open'].to_numpy()[1:] - df['close'].to_numpy()[:-1])
        df['gap/o'] = df['gap'] / df['open']

        return df

    def ma(self, df):
        """

        :param df:
        :return:
        """
        for hour in self.hours_ma:
            df[f'ma_close_{hour}h'] = df['close'].rolling(window=hour, min_periods=1).mean()
            df[f'(ma_close_{hour}h-c)/c'] = (df[f'ma_close_{hour}h'] - df['close']) / df['close']

        df['oc'] = df['close'] - df['open']
        df['m_p'] = (df['open'] + df['close']) / 2 - df['close']
        df['v_p'] = df['high'] - df['low']

        df['percent_oc_c'] = df['oc'] / df['close']
        df['percent_m_p_c'] = df['m_p'] / df['close']
        df['percent_v_p_c'] = df['v_p'] / df['close']
        df['percent_oc_o'] = df['oc'] / df['open']
        df['percent_m_p_o'] = df['m_p'] / df['open']
        df['percent_v_p_o'] = df['v_p'] / df['open']

        for hour in self.hours_ma:
            df[f'ma_oc_{hour}h'] = df['oc'].rolling(window=hour, min_periods=1).mean()
            df[f'ma_percent_oc_{hour}h'] = df['percent_oc_c'].rolling(window=hour, min_periods=1).mean()
            df[f'ma_oc_{hour}h/o'] = df[f'ma_oc_{hour}h'] / df['open']
            df[f'ma_oc_{hour}h/c'] = df[f'ma_oc_{hour}h'] / df['close']

        return df

    def ma_subdata(self, df):
        """

        :param df:
        :return:
        """
        for hour in self.hours_ma:
            df[f'ma_close_s1_{hour}h_sd'] = df['close_s1'].rolling(window=hour, min_periods=1).mean()
            df[f'(ma_close_s1_{hour}h-c)/c_sd'] = (df[f'ma_close_s1_{hour}h_sd'] - df['close_s1']) / (
                        df['close_s1'] + self.eps)
            df[f'ma_close_s2_{hour}h_sd'] = df['close_s2'].rolling(window=hour, min_periods=1).mean()
            df[f'(ma_close_s2_{hour}h-c)/c_sd'] = (df[f'ma_close_s2_{hour}h_sd'] - df['close_s2']) / (
                        df['close_s2'] + self.eps)
            df[f'ma_close_s3_{hour}h_sd'] = df['close_s3'].rolling(window=hour, min_periods=1).mean()
            df[f'(ma_close_s3_{hour}h-c)/c_sd'] = (df[f'ma_close_s3_{hour}h_sd'] - df['close_s3']) / (
                        df['close_s3'] + self.eps)
            df[f'ma_close_s4_{hour}h_sd'] = df['close_s4'].rolling(window=hour, min_periods=1).mean()
            df[f'(ma_close_s4_{hour}h-c)/c_sd'] = (df[f'ma_close_s4_{hour}h_sd'] - df['close_s4']) / (
                        df['close_s4'] + self.eps)

        return df

    def rsi(self, df):
        """

        :param df:
        :return:
        """
        for hour in self.hours_rsi:
            delta = df['close'].diff()
            delta.fillna(0, inplace=True)
            up = delta.clip(lower=0)
            down = -1 * delta.clip(upper=0)
            ema_up = up.ewm(com=hour, min_periods=1, adjust=False).mean()
            ema_down = down.ewm(com=hour, min_periods=1, adjust=False).mean()
            rs = ema_up / (ema_down + self.eps)

            rsi = 100 - (100 / (1 + rs))
            df[f'rsi_{hour}'] = rsi / 100
        return df

    def macd(self, df):
        """

        :param df:
        :return:
        """
        for hour_i, hour_j, hour_k in self.hours_macd:
            exp1 = df['close'].ewm(span=hour_i, min_periods=1, adjust=False).mean()
            exp2 = df['close'].ewm(span=hour_j, min_periods=1, adjust=False).mean()
            macd = exp1 - exp2
            exp3 = macd.ewm(span=hour_k, min_periods=1, adjust=False).mean()
            df[f'macd_{hour_i}_{hour_j}_{hour_k}'] = macd
            df[f'macd_signal_line_{hour_i}_{hour_j}_{hour_k}'] = exp3
            df[f'macd_diff_{hour_i}_{hour_j}_{hour_k}'] = macd - exp3 + self.eps
        return df

    def atr(self, df):
        """

        :param df:
        :return:
        """
        high_low = df['high'] - df['low']
        for hour in self.hours_atr:
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            true_range = np.max(ranges, axis=1)
            atr = true_range.rolling(window=hour, min_periods=1).sum() / hour

            df[f'atr_{hour}'] = atr
        return df

    def general_points(self, df):
        """

        :param df:
        :return:
        """
        df['support_80'] = df['close'].rolling(window=80, min_periods=1).min()
        df['support_80_mean'] = df['close'].rolling(window=80, min_periods=1).mean()
        df['resist_80'] = df['close'].rolling(window=80, min_periods=1).max()
        df['resist_80_mean'] = df['close'].rolling(window=80, min_periods=1).mean()
        df['support_80-close'] = df['support_80'] - df['close'] + self.eps
        df['support_80_mean-close'] = df['support_80_mean'] - df['close'] + self.eps
        df['resist_80-close'] = df['resist_80'] - df['close'] + self.eps
        df['resist_80_mean-close'] = df['resist_80_mean'] - df['close'] + self.eps

        for hour in self.hours_general_points:
            next_hour = hour + 40
            df[f'support_{next_hour}'] = df[f'support_{hour}'].rolling(window=hour, min_periods=1).min()
            df[f'support_{next_hour}_mean'] = df[f'support_{hour}'].rolling(window=hour, min_periods=1).mean()
            df[f'resist_{next_hour}'] = df[f'resist_{hour}'].rolling(window=hour, min_periods=1).max()
            df[f'resist_{next_hour}_mean'] = df[f'resist_{hour}'].rolling(window=hour, min_periods=1).mean()

            if hour >= 80:
                df[f'support_{next_hour}'].fillna(df[f'support_{hour}'], inplace=True)
                df[f'support_{next_hour}_mean'].fillna(df[f'support_{hour}_mean'], inplace=True)
                df[f'resist_{next_hour}'].fillna(df[f'resist_{hour}'], inplace=True)
                df[f'resist_{next_hour}_mean'].fillna(df[f'resist_{hour}_mean'], inplace=True)

            df[f'support_{next_hour}-close'] = df[f'support_{next_hour}'] - df['close'] + self.eps
            df[f'support_{next_hour}_mean-close'] = df[f'support_{next_hour}_mean'] - df['close'] + self.eps
            df[f'resist_{next_hour}-close'] = df[f'resist_{next_hour}'] - df['close'] + self.eps
            df[f'resist_{next_hour}_mean-close'] = df[f'resist_{next_hour}_mean'] - df['close'] + self.eps
        return df

    def trend(self, df):
        """

        :param df:
        :return:
        """
        for hour in self.hours_trens:
            mean_values = []
            std_values = []
            diff_values = []

            for i, _ in enumerate(df['close']):
                idx_start = i // hour * hour
                idx_end = (i // hour + 1) * hour
                idx_end = min(idx_end, len(df['close']))
                if idx_start >= idx_end:
                    mean_values.append(df['close'].iloc[idx_start])
                    std_values.append(std_values[-1])
                else:
                    mean_values.append(df['close'].iloc[idx_start: idx_end].mean())
                    std_values.append(df['close'].iloc[idx_start: idx_end].std())

                if i < hour:
                    diff_values.append(0)
                else:
                    diff_values.append(mean_values[-1] - mean_values[-hour - 1])

            merge_mean_values = []
            trend_mean_values = {df['time'].iloc[0]: df['close'].iloc[0]}
            trend_values = []

            diff_idx_start = 0
            diff_idx_end = 1

            while diff_idx_end < len(diff_values):
                if np.sign(diff_values[diff_idx_start]) == np.sign(diff_values[diff_idx_end]):
                    diff_idx_end += 1
                else:
                    merge_mean_values.extend(
                        [np.mean(mean_values[diff_idx_start: diff_idx_end])] * (diff_idx_end - diff_idx_start))

                    end_trend = mean_values[diff_idx_end - 1]
                    trend_mean_values[df['time'].iloc[diff_idx_end - 1]] = end_trend
                    step_trend = (mean_values[diff_idx_end - 1] -
                                  mean_values[diff_idx_start]) / \
                                 (diff_idx_end - diff_idx_start + self.eps)
                    trend_values.append(mean_values[diff_idx_start])
                    trend_values.extend(
                        [mean_values[diff_idx_start] + step_trend * i for i in range(1, diff_idx_end - diff_idx_start)])

                    diff_idx_start = diff_idx_end
                    diff_idx_end += 1

                if diff_idx_end == len(diff_values):
                    merge_mean_values.extend(
                        [np.mean(mean_values[diff_idx_start: diff_idx_end])] * (diff_idx_end - diff_idx_start))

                    end_trend = mean_values[diff_idx_end - 1]
                    trend_mean_values[df['time'].iloc[-1]] = end_trend
                    step_trend = (mean_values[diff_idx_end - 1] -
                                  mean_values[diff_idx_start]) / \
                                 (diff_idx_end - diff_idx_start + self.eps)
                    trend_values.append(mean_values[diff_idx_start])
                    trend_values.extend(
                        [mean_values[diff_idx_start] + step_trend * i for i in range(1, diff_idx_end - diff_idx_start)])

                    break

            df[f'mean_subtrend_{hour}'] = mean_values
            df[f'std_subtrend_{hour}'] = std_values
            df[f'std_subtrend_{hour}'].fillna(0, inplace=True)
            df[f'trend_values_{hour}'] = trend_values
            df[f'diff_trend_values_{hour}'] = df['close'] - trend_values
        return df

    def ao(self, df):
        """

        :param df:
        :return:
        """
        for hour_mn, hour_mj in self.hours_ao:
            median_price = (df['high'] + df['low']) / 2
            sma_mn = median_price.rolling(window=hour_mn, min_periods=1).mean()
            sma_mj = median_price.rolling(window=hour_mj, min_periods=1).mean()
            ao = sma_mn - sma_mj
            df[f'ao_{hour_mn}_{hour_mj}'] = ao
        return df

    def ac(self, df):
        """

        :param df:
        :return:
        """
        for hour_mn, hour_mj in self.hours_ao:
            median_price = (df['high'] + df['low']) / 2
            sma_mn = median_price.rolling(window=hour_mn, min_periods=1).mean()
            sma_mj = median_price.rolling(window=hour_mj, min_periods=1).mean()
            ao = sma_mn - sma_mj
            sma_ao = ao.rolling(window=hour_mn, min_periods=1).mean()
            df[f'ac_{hour_mn}'] = ao - sma_ao
        return df

    def alligator(self, df):
        """

        :param df:
        :return:
        """
        median_price = (df['high'] + df['low']) / 2
        for h1, h2, h3, h4 in self.hours_aligator:
            sum_lips = median_price.rolling(window=h2, min_periods=1).sum()
            diff_lips = (sum_lips / h2).shift(h1)
            diff_lips.fillna(0, inplace=True)
            smma_lips = (sum_lips - diff_lips + median_price) / h2

            sum_teeth = median_price.rolling(window=h3, min_periods=1).sum()
            diff_teeth = (sum_teeth / h3).shift(h2)
            diff_teeth.fillna(0, inplace=True)
            smma_teeth = (sum_teeth - diff_teeth + median_price) / h3

            sum_jaw = median_price.rolling(window=h4, min_periods=1).sum()
            diff_jaw = (sum_jaw / h4).shift(h3)
            diff_jaw.fillna(0, inplace=True)
            smma_jaw = (sum_jaw - diff_jaw + median_price) / h4

            df[f'lips_{h2}_{h1}'] = smma_lips
            df[f'teeth_{h3}_{h2}'] = smma_teeth
            df[f'jaw_{h4}_{h3}'] = smma_jaw
        return df

    def ema(self, df):
        """

        :param df:
        :return:
        """
        for hour in self.hours_ma:
            df[f'ewa_{hour}'] = df['close'].ewm(span=hour, min_periods=1, adjust=False, ignore_na=False).mean()
        return df

    def brp(self, df):
        """

        :param df:
        :return:
        """
        for hour in self.hours_ma:
            df[f'bears_{hour}'] = df['low'] - df['low'].ewm(span=hour, min_periods=1,
                                                            adjust=False, ignore_na=False).mean()
        return df

    def blp(self, df):
        """

        :param df:
        :return:
        """
        for hour in self.hours_ma:
            df[f'bulls_{hour}'] = df['high'] - df['high'].ewm(span=hour, min_periods=1,
                                                              adjust=False, ignore_na=False).mean()
        return df

    def cci(self, df):
        """

        :param df:
        :return:
        """
        tp = (df['high'] + df['low'] + df['close']) / 3

        for hour in self.hours_ma:
            sma_tp = tp.rolling(window=hour, min_periods=1).mean()
            d = tp - sma_tp
            m = np.abs(d).rolling(window=hour, min_periods=1).mean() * 0.015 + self.eps
            cci = d / m
            df[f'cci_{hour}'] = cci
        return df

    def dem(self, df):
        """

        :param df:
        :return:
        """
        for hour in self.hours_ma:
            demax = df['high'] - df['high'].shift(hour)
            demin = df['low'].shift(hour) - df['low']
            demax.fillna(0, inplace=True)
            demin.fillna(0, inplace=True)
            demax = demax.mask(demax < 0, 0)
            demin = demin.mask(demin < 0, 0)
            ma_demax = demax.rolling(window=hour, min_periods=1).mean() + self.eps
            ma_demin = demin.rolling(window=hour, min_periods=1).mean() + self.eps
            df[f'demark_{hour}'] = ma_demax / (ma_demax + ma_demin)
        return df

    def envlp(self, df):
        """

        :param df:
        :return:
        """
        k = 10
        for hour in self.hours_ma:
            upper_band = df['close'].rolling(window=hour, min_periods=1).mean() * (1 + k / 1000)
            lower_band = df['close'].rolling(window=hour, min_periods=1).mean() * (1 - k / 1000)
            df[f'envlp_up_{hour}'] = upper_band
            df[f'envlp_low_{hour}'] = lower_band
            df[f'diff_nearest_band_{hour}'] = np.min((abs(df['close'] - lower_band), abs(upper_band - df['close'])),
                                                     axis=0)
        return df

    def mf(self, df):
        """

        :param df:
        :return:
        """
        hl = df['high'] - df['low']
        df['mf_0'] = hl / df['volume']
        for hour in self.hours_ma:
            df[f'mf_{hour}'] = hl.rolling(window=hour, min_periods=1).mean() /\
                               df['volume'].rolling(window=hour, min_periods=1).mean()
        return df

    def mfi(self, df):
        """

        :param df:
        :return:
        """
        tp = (df['high'] + df['low'] + df['close']) / 3
        money_flow = tp * df['volume']

        pmf = money_flow.mask(money_flow.diff(1) < 0, 0)
        nmf = money_flow.mask(money_flow.diff(1) > 0, 0)

        for hour in self.hours_ma:
            money_ratio = pmf.rolling(window=hour, min_periods=1).sum() / (
                        nmf.rolling(window=hour, min_periods=1).sum() + self.eps)
            df[f'mfi_{hour}'] = (100 - (100 / (1 + money_ratio))) / 100
        return df

    def obv(self, df):
        """

        :param df:
        :return:
        """
        for hour in self.hours_ma:
            obv_0 = 0
            closes = df['close'].values
            volumes = df['volume'].values
            obv_i = [0] * min(hour, df.shape[0])
            for i in range(min(hour, df.shape[0]), df.shape[0]):
                if closes[i] > closes[i - min(hour, df.shape[0])]:
                    obv_i.append(obv_0 + volumes[i])
                    obv_0 += volumes[i]
                elif closes[i] < closes[i - min(hour, df.shape[0])]:
                    obv_i.append(obv_0 - volumes[i])
                    obv_0 -= volumes[i]
                else:
                    obv_i.append(obv_0)
            df[f'obv_{hour}'] = obv_i
        return df

    def sar(self, df):
        """

        :param df:
        :return:
        """
        acceleration = 0.02
        for hour in self.hours_ma:
            highs = df['high'].values
            sar_i_high = [0] * min(hour, df.shape[0])
            for i in range(min(hour, df.shape[0]), df.shape[0]):
                sar_0_high = (highs[i - min(hour, df.shape[0])] - sar_i_high[
                    i - min(hour, df.shape[0])]) * acceleration + sar_i_high[i - min(hour, df.shape[0])]
                sar_i_high.append(sar_0_high)

            df[f'sar_{hour}'] = sar_i_high
        return df

    def sd(self, df):
        """

        :param df:
        :return:
        """
        for hour in self.hours_ma:
            sd_hour = ((df['close'] - df['close'].rolling(window=hour, min_periods=1).mean()) ** 2)
            df[f'sd_{hour}'] = (sd_hour.rolling(window=hour, min_periods=1).mean()) ** 0.5
        return df

    def so(self, df):
        """

        :param df:
        :return:
        """
        for hour in self.hours_ma:
            k = (df['close'] - df['low'].rolling(window=hour, min_periods=1).min()) / \
                (df['high'].rolling(window=hour,
                                    min_periods=1).max() - df['low'].rolling(window=hour,
                                                                             min_periods=1).min() + self.eps)
            d = k.rolling(window=hour, min_periods=1).mean()
            df[f'so_{hour}'] = d
        return df

    def date_features(self, df):
        """

        :param df:
        :return:
        """
        df['hour_of_day'] = df['time'].apply(lambda x: pd.Timestamp(x).hour)
        df['day_of_week'] = df['time'].apply(lambda x: pd.Timestamp(x).day_of_week + 1)
        df['week_of_year'] = df['time'].apply(lambda x: pd.Timestamp(x).weekofyear)

        df['is_last_hour'] = 0
        df.loc[df['hour_of_day'] == max(df['hour_of_day'].unique()), 'is_last_hour'] = 1
        df['is_last_day'] = 0
        df.loc[df['day_of_week'] == max(df['day_of_week'].unique()), 'is_last_day'] = 1
        df['is_last_week'] = 0
        df.loc[df['week_of_year'] >= 50, 'is_last_week'] = 1

        df['till_holidays'] = df['time'].apply(lambda x: min(
            [(pd.Timestamp(h) - pd.Timestamp(x)).days for h in self.ru_holidays.keys() if
             pd.Timestamp(h) >= pd.Timestamp(x)])).values
        df['holidays_7'] = 0
        df.loc[df['till_holidays'] <= 7, 'holidays_7'] = 1

        return df

    @staticmethod
    def clear_df(df):
        """

        :param df:
        :return:
        """
        df['anomaly_tend'] = df[['open', 'close']].rolling(window=18,
                                                           min_periods=1,
                                                           method='table').apply(apply_rolling_table,
                                                                                 raw=True,
                                                                                 engine='numba').iloc[:, 0]

        extrimal_intervals = []
        extrimal_dates = df[df['anomaly_tend'] > 15]['time'].dt.date.sort_values().unique()

        i = 0
        while i < len(extrimal_dates):
            start_date = extrimal_dates[i]
            i += 1
            if i >= len(extrimal_dates):
                end_date = extrimal_dates[i - 1]
                extrimal_intervals.append(f'{start_date} - {end_date}')
                break
            while i < len(extrimal_dates) and (
                    pd.to_datetime(extrimal_dates[i]) - pd.to_datetime(extrimal_dates[i - 1])).days <= 3:
                i += 1
            end_date = extrimal_dates[i - 1]

            extrimal_intervals.append(f'{start_date} - {end_date}')

        df.drop(columns=['anomaly_tend'], inplace=True)
        dataframes = []
        d2_ = copy(df)

        for interval in extrimal_intervals:
            start_date, end_date = interval.split(' - ')
            d1_, d2_ = d2_.loc[d2_['time'] < start_date], d2_.loc[d2_['time'] > end_date]
            dataframes.append(d1_)

        dataframes.append(d2_)

        return dataframes

    def generate_feature_one_df(self, df):
        """

        :param df:
        :return:
        """
        df = df.loc[df['time'].dt.hour.between(10, 22)]

        df = self.diff_and_div(df)
        df = self.gap(df)
        df = self.ma(df)
        df = self.ma_subdata(df)
        df = self.rsi(df)
        df = self.macd(df)
        df = self.atr(df)
        df = self.general_points(df)
        df = self.trend(df)

        df = self.ao(df)
        df = self.ac(df)
        df = self.alligator(df)
        df = self.ema(df)
        df = self.brp(df)
        df = self.blp(df)
        df = self.cci(df)
        df = self.dem(df)
        df = self.envlp(df)
        df = self.mf(df)
        df = self.mfi(df)
        df = self.obv(df)
        df = self.sar(df)
        df = self.sd(df)
        df = self.so(df)
        df = self.date_features(df)

        return df

    def generate_feature(self, do_clear=False):
        """

        :param do_clear:
        :return:
        """
        self.ru_holidays = holidays.RU(years=range(self.df['time'].apply(lambda x: pd.Timestamp(x)).dt.year.min(),
                                                   self.df['time'].apply(lambda x: pd.Timestamp(x)).dt.year.max() + 2))

        if do_clear:
            self.df = self.clear_df(self.df)
        else:
            self.df = [self.df]

        self.df_feature = []

        for df in self.df:
            df_f = self.generate_feature_one_df(df)
            self.df_feature.append(df_f)

        self.df = pd.concat(self.df_feature)

        return self.df

