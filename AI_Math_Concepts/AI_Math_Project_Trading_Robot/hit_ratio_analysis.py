from candlestick import candlestick
import matplotlib.pyplot as plt
import mplfinance as mpf
import numpy as np
import pandas as pd

# TODO - Correct CHART_MIN:CHART_MAX - get it dynamic by signal data or some other way
CHART_MIN = -200
CHART_MAX = -1
MARKERSIZE = 50
DOWN_INDICATOR = 0.98
UP_INDICATOR = 1.02
STOP_LEVEL = 0.02
STOP_LEVEL_LOST = 0.01
HOLDING_PERIOD = 5

# Top 20 S&P 500 Stocks by Index Weight
shares = {
    'Facebook, Inc. (FB)': 'FB.csv',
    'Moderna, Inc. (MRNA)': 'MRNA.csv',
    'Amazon.com, Inc. (AMZN)': 'AMZN.csv',
    'Alphabet Inc. (GOOG)': 'GOOG.csv',
    'Microsoft Corporation (MSFT)': 'MSFT.csv',
    'Apple Inc. (AAPL)': 'AAPL.csv',
    'Tesla, Inc. (TSLA)': 'TSLA.csv',
    'Berkshire Hathaway Inc. (BRK-B)': 'BRK.csv',
    'JPMorgan Chase & Co. (JPM)': 'JPM.csv',
    'Johnson & Johnson (JNJ)': 'JNJ.csv',
    "Visa Inc. (V)": "V.csv",
    "UnitedHealth Group Incorporated (UNH)": "UNH.csv",
    "NVIDIA Corporation (NVDA)": "NVDA.csv",
    "The Home Depot, Inc. (HD)": "HD.csv",
    "The Procter and Gamble Company (PG)": "PG.csv",
    "Mastercard Incorporated (MA)": "MA.csv",
    "Bank of America Corporation (BAC)": "BAC.csv",
    "PayPal Holdings, Inc. (PYPL)": "PYPL.csv",
    "Intel Corporation (INTC)": "INTC.csv",
    "Comcast Corporation (CMCSA)": "CMCSA.csv",
}

signals_parameters = {'BullishEngulfing': 'bull',
                      'BearishEngulfing': 'bear',
                      'EveningStar': 'bear',
                      'InvertedHammers': 'bull',
                      'Hammer': 'bull',
                      'MorningStar': 'bull'}

df_coefficients = {'BullishEngulfing': [],
                   'BearishEngulfing': [],
                   'EveningStar': [],
                   'InvertedHammers': [],
                   'Hammer': [],
                   'MorningStar': []}

signal_names = [k for k in signals_parameters.keys()]

mapper = {'BullishEngulfing': candlestick.bullish_engulfing,
          'BearishEngulfing': candlestick.bearish_engulfing,
          'EveningStar': candlestick.evening_star,
          'InvertedHammers': candlestick.inverted_hammer,
          'Hammer': candlestick.hammer,
          'MorningStar': candlestick.morning_star}


def read_data_from_file(data_file):
    with open(f"data/{data_file}") as f:
        df = pd.read_csv(f, index_col=0, parse_dates=["date"])
    f.close()

    candles_df = pd.DataFrame(df,
                              columns=['T', 'open', 'high', 'low', 'close', 'volume', 'CT', 'QV', 'N', 'TB', 'TQ', 'I'])
    candles_df.index.name = 'date'

    # Add Exponential moving average EWM
    ewm_short = pd.DataFrame(df['adj close'].ewm(span=21, adjust=False).mean())
    candles_df['EWM'] = ewm_short

    # calculate Relative Strength Index (RSI)
    delta = df['adj close'].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ema_up = up.ewm(com=13, adjust=False).mean()
    ema_down = down.ewm(com=13, adjust=False).mean()
    rs = ema_up / ema_down

    candles_df['RSI'] = 100 - (100 / (1 + rs))

    # add signals column
    for t, f in mapper.items():
        target = t
        candles_df = f(candles_df, target=target)

    return candles_df


# TODO - Get data directly from web
def read_data_from_web():
    pass


def signal_chart(serries, price, up_down_indicator):
    signal = []
    for i in range(0, len(serries)):
        if serries[i] == True:
            signal.append(price[i] * up_down_indicator)
        else:
            signal.append(np.nan)
    return signal


def make_chart(candles_df):
    try:
        td_bull_esc = signal_chart(candles_df['BullishEngulfing'], candles_df['close'], DOWN_INDICATOR)
        td_bear_esc = signal_chart(candles_df['BearishEngulfing'], candles_df['close'], UP_INDICATOR)
        td_inv_ham = signal_chart(candles_df['InvertedHammers'], candles_df['close'], DOWN_INDICATOR)
        td_ham = signal_chart(candles_df['Hammer'], candles_df['close'], DOWN_INDICATOR)
        td_mor_star = signal_chart(candles_df['MorningStar'], candles_df['high'], DOWN_INDICATOR)
        td_eve_star = signal_chart(candles_df['EveningStar'], candles_df['low'], UP_INDICATOR)

        # TODO - Correct CHART_MIN:CHART_MAX and make cycle/comprehension for get different plots
        adps = [mpf.make_addplot(candles_df['EWM'][CHART_MIN:CHART_MAX], color='r', linestyle='dotted'),
                mpf.make_addplot(candles_df['RSI'][CHART_MIN:CHART_MAX], panel=1, color='black', linestyle='dotted',
                                 secondary_y=True),
                mpf.make_addplot(td_bull_esc[CHART_MIN:CHART_MAX], type='scatter', markersize=MARKERSIZE, marker='^',
                                 color='green'),
                mpf.make_addplot(td_bear_esc[CHART_MIN:CHART_MAX], type='scatter', markersize=MARKERSIZE, marker='v',
                                 color='red'),
                mpf.make_addplot(td_inv_ham[CHART_MIN:CHART_MAX], type='scatter', markersize=MARKERSIZE, marker='^',
                                 color='grey'),
                mpf.make_addplot(td_ham[CHART_MIN:CHART_MAX], type='scatter', markersize=MARKERSIZE, marker='^',
                                 color='orange'),
                mpf.make_addplot(td_mor_star[CHART_MIN:CHART_MAX], type='scatter', markersize=MARKERSIZE, marker='^',
                                 color='cyan'),
                mpf.make_addplot(td_eve_star[CHART_MIN:CHART_MAX], type='scatter', markersize=MARKERSIZE, marker='v',
                                 color='magenta'),
                ]
        mpf.plot(candles_df[CHART_MIN:CHART_MAX], type='candle', style='yahoo', volume=True, volume_panel=1,
                 figratio=(15, 7), addplot=adps,
                 title=f"\n\n{company_name} prices,\n six popular signals",
                 savefig=f"pictures/{company_name}.png")  # , savefig=f"pictures/{company_name}.png"
    except:
        return


def plot_hit_ratio(df_coefficients):
    # TODO How STOP_LEVEL impact to hit_ratio_mean - make chart for different stop-levels
    mean_hit_ratios = {}
    combined_label = ''

    for name, data in df_coefficients.items():
        mean_hit_ratio = round(np.array(data).mean(), 2)
        mean_hit_ratios[name] = mean_hit_ratio

    for k, v in mean_hit_ratios.items():
        combined_label += f"{k} : {v}\n"

    x = [k for k in mean_hit_ratios.keys()]
    y = [v for k, v in mean_hit_ratios.items()]
    plt.figure(figsize=(15, 8))
    plt.barh(x, y, color="green", label=combined_label)
    plt.title('Hit Ratio means', fontsize=24)
    legend = plt.legend()
    plt.yticks(fontsize=12)
    plt.xticks(fontsize=12)
    plt.ylabel('Signals', fontsize=16)
    plt.xlabel('Hit Ratio', fontsize=16)
    plt.savefig("pictures/hit_ratio_barh.png")
    plt.show()


def plot_df_coefficients_histograms(df_coefficients):
    coef = pd.DataFrame.from_dict(df_coefficients)
    for name, data in df_coefficients.items():
        c_mean = round(coef[name].mean(), 4)
        c_std = round(coef[name].std(), 4)
        c_skew = round(coef[name].skew(), 4)
        c_kurtosis = round(coef[name].kurtosis(), 4)
        statistical_indicators = f"Mean: {c_mean}\nStandard deviation: {c_std}\nSkewness: {c_skew}\nKurtosis: {c_kurtosis}"

        plt.title(f'Hit ratios {name} signal')
        plt.ylabel('Count of companies')
        plt.yticks(range(6), range(6))
        plt.xlabel('Hit Ratio')
        plt.hist(coef[name], bins=10, label=statistical_indicators)
        legend = plt.legend()
        plt.savefig(f"pictures/hit_ratios_{name}_histogram.png")
        plt.show()


def get_count_of_success(signal_name, df):
    count_of_success = 0
    price_level = ''
    price_stop_coefficient = 1
    if signals_parameters[signal_name] == 'bull':
        price_level = "high"
        price_stop_coefficient = 1
        # rsi_level = 30
    else:
        price_level = "low"
        price_stop_coefficient = -1
        # rsi_level = 70
    for ind in df.index:
        if df[signal_name][ind] == True:  # and (df['RSI']>70 or df['RSI']<30)
            price = df[price_level][ind]
            price_stop = price + price * price_stop_coefficient * STOP_LEVEL
            # rsi_signal = df['RSI'][ind]
            idx = ind
            for i in range(1, HOLDING_PERIOD):
                date = pd.to_datetime(idx)
                try:
                    idx = next(j for j in df.index if j > date)
                    if price_stop_coefficient == 1:
                        if df[price_level][idx] >= price_stop:
                            count_of_success += 1
                            break
                    elif price_stop_coefficient == -1:
                        if df[price_level][idx] <= price_stop:
                            count_of_success += 1
                            break
                except:
                    continue
    return count_of_success


def get_signal_coefficients(signal_name, df):
    count_of_signals = len(df.loc[df[signal_name] == True])
    count_of_success = get_count_of_success(signal_name, df)
    hit_ratio = round(count_of_success / count_of_signals, 2)
    # profit_factor = 5
    # expectancy = 5
    # risk_reward = 5 #price_t0*stop_level
    return (hit_ratio, count_of_signals, count_of_success)


# My program start here
for company_name, data_file in shares.items():
    df = read_data_from_file(data_file)
    # with open("data/samples.csv", "a") as fl:
    #     fl.write(f'{company_name}, {len(df)}\n')
    # make_chart(df)
    for signal_name in signal_names:
        signal_coefficients = get_signal_coefficients(signal_name, df)
        df_coefficients[signal_name].append(signal_coefficients[0])

plot_df_coefficients_histograms(df_coefficients)
plot_hit_ratio(df_coefficients)
