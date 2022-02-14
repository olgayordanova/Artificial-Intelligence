import pandas as pd
from pandas_datareader import data as web #conda install -c anaconda pandas-datareader
import datetime
import os


os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2' #https://stackoverflow.com/questions/66092421/how-to-rebuild-tensorflow-with-the-compiler-flags

current_path = os.getcwd ()+'\\data\\'

START_YEAR = 2018
END_YEAR = 2022
STEP = 1

START_DATE = "2018-02-06"
END_DATE = "2021-12-31"

tickers = pd.read_csv(os.path.join ( current_path, "indicators_list.csv"), sep = ";")

def parser(x):
    return datetime.datetime.strptime(x,'%Y-%m-%d')

def read_data_from_yahoo(ticker):
    start = datetime.datetime(START_YEAR,1,1)
    end = datetime.datetime(END_YEAR,1,1)
    print ('reading data of', ticker ,' from Yahoo Finance..')
    raw_data = web.DataReader(ticker, "yahoo", start, end)

    return raw_data

def add_data_to_df(ticker, stock_df):
    df = pd.read_csv ( f'data/stocks_data/{ticker}.csv', header=0, parse_dates=[0],
                             date_parser=parser )
    mask = (df['Date'] > START_DATE) & (df['Date'] <= END_DATE)
    df = df.loc[mask]
    df.reset_index ( inplace=True, drop=True )
    if len(stock_df) == 0:
        stock_df = df[["Date"]]
    stock_df[ticker] = df["Adj Close"]
    return stock_df

def scraping_data(stock_df, tickers):
    for i in range ( 0, len ( tickers ) ):
        try:
            if tickers.Source[i] == "Yahoo":
                company_data = read_data_from_yahoo ( tickers.Ticker[i] )
                company_data.to_csv ( f'data/stocks_data/{tickers.Ticker[i]}.csv', index="Data", header=True )
                stock_df = add_data_to_df(tickers.Ticker[i], stock_df)
        except:
            continue
    return stock_df

stock_df = pd.DataFrame()
stock_df = scraping_data(stock_df,tickers)
stock_df.to_csv ( f'data/stocks_data/dataset_4.csv', index="Data", header=True )

