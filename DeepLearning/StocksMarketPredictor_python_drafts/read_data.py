import numpy as np
import pandas as pd
import matplotlib.pylab as plt
from pandas_datareader import data as web
import datetime
# import tensorflow as tf
import os
import glob
import scipy.sparse
from core import min_max_normalize


os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2' #https://stackoverflow.com/questions/66092421/how-to-rebuild-tensorflow-with-the-compiler-flags

current_path = os.getcwd ()+'\\data\\'

# TICKER = "^GSPC"
START_YEAR = 1990
END_YEAR = 2022
STEP = 1

companies = pd.read_csv(os.path.join ( current_path, "sp_companies.csv"), sep = ";")
tsi_sp = pd.read_excel(os.path.join ( current_path, "tsi_sp.xls")) #https://www.spglobal.com/spdji/en/indices/strategy/sp-500-twitter-sentiment-index/#overview // data/export

def read_data_from_yahoo(ticker):
    start = datetime.datetime(START_YEAR,1,1)
    end = datetime.datetime(END_YEAR,1,1)
    print ('reading data of', ticker ,' from Yahoo Finance..')
    raw_data = web.DataReader(ticker, "yahoo", start, end) #read data with panda_datareader ----> "google", "yahoo"

    # Add Exponential moving average EWM
    ewm_short = pd.DataFrame ( raw_data['Adj Close'].ewm ( span=21, adjust=False ).mean () )
    raw_data['ewm_short'] = ewm_short

    # Add Moving average MA
    ma_short = pd.DataFrame ( raw_data['Adj Close'].rolling(21).mean() )
    raw_data['ma_short'] = ma_short

    # calculate Relative Strength Index (RSI) momentum oscillator
    delta = raw_data['Adj Close'].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ema_up = up.ewm(com=13, adjust=False).mean()
    ema_down = down.ewm(com=13, adjust=False).mean()
    rs = ema_up / ema_down

    raw_data['rsi'] = 100 - (100 / (1 + rs))

    # calculate MACD
    # Get the 26-day EMA of the closing price
    k = raw_data['Adj Close'].ewm ( span=12, adjust=False, min_periods=12 ).mean ()
    # Get the 12-day EMA of the closing price
    d = raw_data['Adj Close'].ewm ( span=26, adjust=False, min_periods=26 ).mean ()
    # Subtract the 26-day EMA from the 12-Day EMA to get the MACD
    macd = k - d

    raw_data['macd'] = macd

    # calculate Bollinger bands
    tp = (raw_data['Close'] + raw_data['Low'] + raw_data['High']) / 3
    std = tp.rolling ( 20 ).std ( ddof=0 )
    ma_tp = tp.rolling ( 20 ).mean ()
    bolu = ma_tp + 2 * std
    bold = ma_tp - 2 * std
    raw_data['bolu'] = bolu
    raw_data['bold'] = bold

    # open = raw_data.Open.tolist()
    # close = raw_data.Close.tolist()
    # high = raw_data.High.tolist()
    # low = raw_data.Low.tolist()
    # volume = raw_data.Volume.tolist()
    # adj_close = raw_data["Adj Close"].tolist()
    raw_data.to_csv ( r'data/output_data/raw_df.csv', index=False, header=True )
    # data = tf.convert_to_tensor(raw_data)
    data_scaled =min_max_normalize(raw_data)
    # data_scaled = tft.scale_by_min_max ( data, output_min=0, output_max=1.0 )

    return raw_data

def normalise_data(df):
    pass
    return df_normalized


def preprocessing_data():
    for i in range (0, len(companies)):
        company_data = read_data_from_yahoo(companies.ticker[i])
        print ( company_data )


df =  read_data_from_yahoo("^GSPC")
df_normalized = normalise_data(df)
print(1)





