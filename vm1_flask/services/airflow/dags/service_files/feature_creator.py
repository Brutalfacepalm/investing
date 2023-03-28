import numpy as np
import pandas as pd
import holidays
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)


class FeatureCreator:

    def __init__(self, data_as_df):
        # self.eps = 1e-5
        # self.df = data_as_df
        # self.hours_ma = [10, 20, 40, 60, 80, 100, 120, 150, 200, 250, 300, 350, 400, 450, 500]
        # self.hours_rsi = [10, 20, 40, 60, 80, 100, 120, 150, 200, 250, 300, 350, 400, 450, 500]
        # self.hours_macd = [[12, 26, 9], [16, 32, 12], [20, 38, 15], [24, 44, 18], [28, 50, 21],
        #                    [32, 56, 24], [36, 62, 27], [40, 68, 30], [44, 74, 33], [48, 80, 36],
        #                    [52, 86, 39], [56, 92, 42], [60, 98, 45],
        #                    [68, 110, 51], [76, 122, 57],[84, 134, 63],
        #                    [92, 146, 69], [100, 158, 75], [108, 170, 81],
        #                    [120, 190, 89], [132, 210, 97],
        #                    [144, 230, 105], [156, 250, 113]]
        # self.hours_atr = [10, 20, 40, 60, 80, 100, 120, 150, 200, 250, 300, 350, 400]
        # self.hours_general_points = [20, 40, 60, 80, 120, 160, 200, 240, 280,
        #                              320, 360, 400, 440, 480, 520, 560, 600, 640]
        # self.hours_trens = [20, 60, 100, 120, 150, 200, 250, 300, 350, 400, 500]
        # self.hours_ao = [[5, 34], [10, 68], [15, 102], [20, 136], [25, 204], [30, 272], [35, 340],
        #                  [40, 374], [45, 408], [50, 442], [55, 476], [60, 510]]
        # self.hours_aligator = [[3, 5, 8, 13],
        #                        [8, 12, 18, 24],
        #                        [20, 28, 36, 50],
        #                        [48, 59, 76, 107],
        #                        [107, 128, 163, 226],
        #                        [235, 275, 343, 476], ]
        # self.eps = 1e-5
        # self.df = data_as_df
        # self.hours_ma = [20, 30, 40, 50, 60, 70, 140, 200, 250, 300, 350]
        # self.hours_rsi = [20, 30, 40, 50, 60, 70, 80, 90, 100, 120, 140, 160,
        #                   200, 250, 300, 350]
        # self.hours_macd = [[12, 26, 9], [16, 32, 12], [20, 38, 15],
        #                    [24, 44, 18], [28, 50, 21], [32, 56, 24],
        #                    [36, 62, 27], [40, 68, 30], [44, 74, 33],
        #                    [48, 80, 36], [52, 86, 39], [56, 92, 42],
        #                    [60, 98, 45], [68, 110, 51], [76, 122, 57],
        #                    [84, 134, 63], [92, 146, 69], [100, 158, 75], [108, 170, 81]]
        # self.hours_atr = [20, 30, 40, 50, 60, 70, 80, 90, 100, 120, 140, 160]
        # self.hours_general_points = [20, 40, 80, 160, 320, 640]
        # self.hours_trens = [15, 20, 25, 30, 50, 100, 150, 200, 300, 500]
        # self.hours_ao = [[10, 68], [15, 102], [20, 136], [30, 204], [40, 272], [50, 340]]
        # self.hours_aligator = [[8, 12, 18, 28],
        #                        [12, 20, 32, 52],
        #                        [20, 36, 60, 10],
        #                        [36, 68, 116, 196],
        #                        [68, 132, 228, 388], ]
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

    def gap(self, df):
        df['gap'] = np.append([np.nan], df['open'].to_numpy()[1:] - df['close'].to_numpy()[:-1])
        df['gap/o'] = df['gap'] / df['open']

        return df

    def ma(self, df):
        for hour in self.hours_ma:
            df[f'ma_close_{hour}h'] = df['close'].rolling(hour).mean()
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
            df[f'ma_oc_{hour}h'] = df['oc'].rolling(hour).mean()
            df[f'ma_percent_oc_{hour}h'] = df['percent_oc_c'].rolling(hour).mean()
            df[f'ma_oc_{hour}h/o'] = df[f'ma_oc_{hour}h'] / df['open']
            df[f'ma_oc_{hour}h/c'] = df[f'ma_oc_{hour}h'] / df['close']

        return df

    def ma_subdata(self, df):
        for hour in self.hours_ma:
            df[f'ma_close_s1_{hour}h_sd'] = df['close_s1'].rolling(hour).mean()
            df[f'(ma_close_s1_{hour}h-c)/c_sd'] = (df[f'ma_close_s1_{hour}h_sd'] - df['close_s1']) / (df['close_s1'] + self.eps)
            df[f'ma_close_s2_{hour}h_sd'] = df['close_s2'].rolling(hour).mean()
            df[f'(ma_close_s2_{hour}h-c)/c_sd'] = (df[f'ma_close_s2_{hour}h_sd'] - df['close_s2']) / (df['close_s2'] + self.eps)
            df[f'ma_close_s3_{hour}h_sd'] = df['close_s3'].rolling(hour).mean()
            df[f'(ma_close_s3_{hour}h-c)/c_sd'] = (df[f'ma_close_s3_{hour}h_sd'] - df['close_s3']) / (df['close_s3'] + self.eps)
            df[f'ma_close_s4_{hour}h_sd'] = df['close_s4'].rolling(hour).mean()
            df[f'(ma_close_s4_{hour}h-c)/c_sd'] = (df[f'ma_close_s4_{hour}h_sd'] - df['close_s4']) / (df['close_s4'] + self.eps)

        return df

    def rsi(self, df):
        for hour in self.hours_rsi:
            delta = df['close'].diff()
            up = delta.clip(lower=0)
            down = -1 * delta.clip(upper=0)
            ema_up = up.ewm(com=hour, adjust=False).mean()
            ema_down = down.ewm(com=hour, adjust=False).mean()
            rs = ema_up / ema_down

            rsi = 100 - (100 / (1 + rs))
            df[f'rsi_{hour}'] = rsi / 100
        return df

    def macd(self, df):
        for hour_i, hour_j, hour_k in self.hours_macd:
            exp1 = df['close'].ewm(span=hour_i, adjust=False).mean()
            exp2 = df['close'].ewm(span=hour_j, adjust=False).mean()
            macd = exp1 - exp2
            exp3 = macd.ewm(span=hour_k, adjust=False).mean()
            df[f'macd_{hour_i}_{hour_j}_{hour_k}'] = macd
            df[f'macd_signal_line_{hour_i}_{hour_j}_{hour_k}'] = exp3
            df[f'macd_diff_{hour_i}_{hour_j}_{hour_k}'] = macd - exp3 + self.eps
        return df

    def atr(self, df):
        high_low = df['high'] - df['low']
        for hour in self.hours_atr:
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            true_range = np.max(ranges, axis=1)
            atr = true_range.rolling(hour).sum() / hour

            df[f'atr_{hour}'] = atr
        return df

    def general_points(self, df):
        # df['support_20'] = df['close'].rolling(20).min()
        # df['support_20_mean'] = df['close'].rolling(20).mean()
        # df['resist_20'] = df['close'].rolling(20).max()
        # df['resist_20_mean'] = df['close'].rolling(20).mean()
        # df['support_20-close'] = df['support_20'] - df['close'] + self.eps
        # df['support_20_mean-close'] = df['support_20_mean'] - df['close'] + self.eps
        # df['resist_20-close'] = df['resist_20'] - df['close'] + self.eps
        # df['resist_20_mean-close'] = df['resist_20_mean'] - df['close'] + self.eps
        #
        # for hour in self.hours_general_points:
        #     new_hour = hour + 20
        #     if hour >= 80:
        #         new_hour += 20
        #
        #     df[f'support_{new_hour}'] = df[f'support_{hour}'].rolling(hour).min()
        #     df[f'support_{new_hour}_mean'] = df[f'support_{hour}'].rolling(hour).mean()
        #     df[f'resist_{new_hour}'] = df[f'resist_{hour}'].rolling(hour).max()
        #     df[f'resist_{new_hour}_mean'] = df[f'resist_{hour}'].rolling(hour).mean()
        #
        #     if hour >= 80:
        #         df[f'support_{new_hour}'].fillna(df[f'support_{hour}'], inplace=True)
        #         df[f'support_{new_hour}_mean'].fillna(df[f'support_{hour}_mean'], inplace=True)
        #         df[f'resist_{new_hour}'].fillna(df[f'resist_{hour}'], inplace=True)
        #         df[f'resist_{new_hour}_mean'].fillna(df[f'resist_{hour}_mean'], inplace=True)
        #
        #     df[f'support_{new_hour}-close'] = df[f'support_{new_hour}'] - df['close'] + self.eps
        #     df[f'support_{new_hour}_mean-close'] = df[f'support_{new_hour}_mean'] - df['close'] + self.eps
        #     df[f'resist_{new_hour}-close'] = df[f'resist_{new_hour}'] - df['close'] + self.eps
        #     df[f'resist_{new_hour}_mean-close'] = df[f'resist_{new_hour}_mean'] - df['close'] + self.eps
        df['support_20'] = df['close'].rolling(20).min()
        df['support_20_mean'] = df['close'].rolling(20).mean()
        df['resist_20'] = df['close'].rolling(20).max()
        df['resist_20_mean'] = df['close'].rolling(20).mean()
        df['support_20-close'] = df['support_20'] - df['close'] + self.eps
        df['support_20_mean-close'] = df['support_20_mean'] - df['close'] + self.eps
        df['resist_20-close'] = df['resist_20'] - df['close'] + self.eps
        df['resist_20_mean-close'] = df['resist_20_mean'] - df['close'] + self.eps

        for hour in self.hours_general_points:
            df[f'support_{hour * 2}'] = df[f'support_{hour}'].rolling(hour).min()
            df[f'support_{hour * 2}_mean'] = df[f'support_{hour}'].rolling(hour).mean()
            df[f'resist_{hour * 2}'] = df[f'resist_{hour}'].rolling(hour).max()
            df[f'resist_{hour * 2}_mean'] = df[f'resist_{hour}'].rolling(hour).mean()

            if hour >= 80:
                df[f'support_{hour * 2}'].fillna(df[f'support_{hour}'], inplace=True)
                df[f'support_{hour * 2}_mean'].fillna(df[f'support_{hour}_mean'], inplace=True)
                df[f'resist_{hour * 2}'].fillna(df[f'resist_{hour}'], inplace=True)
                df[f'resist_{hour * 2}_mean'].fillna(df[f'resist_{hour}_mean'], inplace=True)

            df[f'support_{hour * 2}-close'] = df[f'support_{hour * 2}'] - df['close'] + self.eps
            df[f'support_{hour * 2}_mean-close'] = df[f'support_{hour * 2}_mean'] - df['close'] + self.eps
            df[f'resist_{hour * 2}-close'] = df[f'resist_{hour * 2}'] - df['close'] + self.eps
            df[f'resist_{hour * 2}_mean-close'] = df[f'resist_{hour * 2}_mean'] - df['close'] + self.eps
        return df

    def trend(self, df):
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
            df[f'trend_values_{hour}'] = trend_values
            df[f'diff_trend_values_{hour}'] = df['close'] - trend_values
        return df

    def ao(self, df):
        for hour_mn, hour_mj in self.hours_ao:
            median_price = (df['high'] + df['low']) / 2
            sma_mn = median_price.rolling(hour_mn).mean()
            sma_mj = median_price.rolling(hour_mj).mean()
            ao = sma_mn - sma_mj
            df[f'ao_{hour_mn}_{hour_mj}'] = ao
        return df

    def ac(self, df):
        for hour_mn, hour_mj in self.hours_ao:
            median_price = (df['high'] + df['low']) / 2
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

        for hour in self.hours_ma:
            sma_tp = tp.rolling(hour).mean()
            d = tp - sma_tp
            m = np.abs(d).rolling(hour).mean() * 0.015 + self.eps
            cci = d / m
            df[f'cci_{hour}'] = cci
        return df

    def dem(self, df):

        for hour in self.hours_ma:
            demax = df['high'] - df['high'].shift(hour)
            demin = df['low'].shift(hour) - df['low']
            demax = demax.mask(demax < 0, 0)
            demin = demin.mask(demin < 0, 0)
            ma_demax = demax.rolling(hour).mean() + self.eps
            ma_demin = demin.rolling(hour).mean() + self.eps
            df[f'demark_{hour}'] = ma_demax / (ma_demax + ma_demin)
        return df

    def envlp(self, df):
        k = 10
        for hour in self.hours_ma:
            upper_band = df['close'].rolling(hour).mean() * (1 + k / 1000)
            lower_band = df['close'].rolling(hour).mean() * (1 - k / 1000)
            df[f'envlp_up_{hour}'] = upper_band
            df[f'envlp_low_{hour}'] = lower_band
            df[f'diff_nearest_band_{hour}'] = np.min((abs(df['close'] - lower_band), abs(upper_band - df['close'])),
                                                     axis=0)
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
            money_ratio = pmf.rolling(hour).sum() / (nmf.rolling(hour).sum() + self.eps)
            df[f'mfi_{hour}'] = (100 - (100 / (1 + money_ratio))) / 100
        return df

    def obv(self, df):
        for hour in self.hours_ma:
            obv_0 = 0
            closes = df['close'].values
            volumes = df['volume'].values
            obv_i = [0] * hour
            for i in range(hour, df.shape[0]):
                if closes[i] > closes[i - hour]:
                    obv_i.append(obv_0 + volumes[i])
                    obv_0 += volumes[i]
                elif closes[i] < closes[i - hour]:
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
            sar_i_high = [0] * hour
            for i in range(hour, df.shape[0]):
                sar_0_high = (highs[i - hour] - sar_i_high[i - hour]) * acceleration + sar_i_high[i - hour]
                sar_i_high.append(sar_0_high)

            df[f'sar_{hour}'] = sar_i_high
        return df

    def sd(self, df):
        for hour in self.hours_ma:
            df[f'sd_{hour}'] = (((df['close'] - df['close'].rolling(hour).mean()) ** 2).rolling(hour).mean()) ** 0.5
        return df

    def so(self, df):
        for hour in self.hours_ma:
            k = (df['close'] - df['low'].rolling(hour).min()) / \
                (df['high'].rolling(hour).max() - df['low'].rolling(hour).min() + self.eps)
            d = k.rolling(hour).mean()
            df[f'so_{hour}'] = d
        return df

    def date_features(self, df):
        df['hour_of_day'] = df['time'].apply(lambda x: pd.Timestamp(x).hour)
        df['day_of_week'] = df['time'].apply(lambda x: pd.Timestamp(x).day_of_week + 1)
        df['week_of_year'] = df['time'].apply(lambda x: pd.Timestamp(x).weekofyear)

        df['is_last_hour'] = 0
        df.loc[df['hour_of_day'] == max(df['hour_of_day'].unique()), 'is_last_hour'] = 1
        df['is_last_day'] = 0
        df.loc[df['day_of_week'] == max(df['day_of_week'].unique()), 'is_last_day'] = 1
        df['is_last_week'] = 0
        df.loc[df['week_of_year'] >= 50, 'is_last_week'] = 1

        ru_holidays = holidays.RU(years=range(df['time'].apply(lambda x: pd.Timestamp(x)).dt.year.min(),
                                              df['time'].apply(lambda x: pd.Timestamp(x)).dt.year.max() + 1))

        df['till_holidays'] = df['time'].apply(lambda x: min(
            [(pd.Timestamp(h) - pd.Timestamp(x)).days for h in ru_holidays.keys() if
             pd.Timestamp(h) >= pd.Timestamp(x)])).values
        df['holidays_7'] = 0
        df.loc[df['till_holidays'] <= 7, 'holidays_7'] = 1

        return df

    def generate_feature(self):
        self.df = self.df.loc[self.df['time'].dt.hour.between(10, 18)]
        self.df.iloc[:, 1:] = np.log(self.df.iloc[:, 1:])

        # from time import time
        # t0 = time()
        # tf = time()
        self.df = self.diff_and_div(self.df)
        # print('diff_and_div DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.gap(self.df)
        # print('gap DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.ma(self.df)
        # print('ma DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.ma_subdata(self.df)
        # print('ma_subdata DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.rsi(self.df)
        # print('rsi DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.macd(self.df)
        # print('macd DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.atr(self.df)
        # print('atr DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.general_points(self.df)
        # print('general_points DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.trend(self.df)
        # print('trend DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()

        self.df = self.ao(self.df)
        # print('ao DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.ac(self.df)
        # print('ac DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.alligator(self.df)
        # print('alligator DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.ema(self.df)
        # print('ema DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.brp(self.df)
        # print('bears DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.blp(self.df)
        # print('bulls DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.cci(self.df)
        # print('cci DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.dem(self.df)
        # print('dem DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.envlp(self.df)
        # print('envlp DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.mf(self.df)
        # print('mf DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.mfi(self.df)
        # print('mfi DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.obv(self.df)
        # print('obv DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.sar(self.df)
        # print('sar DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.sd(self.df)
        # print('sd DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.so(self.df)
        # print('so DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # tf = time()
        self.df = self.date_features(self.df)
        # print('date_features DONE')
        # print(self.df.dropna(axis=0).shape)
        # print(time() - tf)
        # print(time() - t0)

        self.df = self.df.dropna(axis=0)

        return self.df

