import requests
import pandas as pd
import numpy as np

current_path = f'C:\\Users\\o.yordanova\\Desktop\\Work\\InsCompaniesPortfolios\\out_files\\'
codes = ["EUBG", "FORM", "SYN"]
std_dict = {}

def extract_number (string_row):
    newstr = ''.join((ch if ch in '0123456789.' else '') for ch in string_row)
    if ".." in newstr:
        newstr = newstr[2:]

    return newstr

def get_data_from_site(code):
    headers = {
        'origin': 'https://www.infostock.bg/',
    }

    params = {
        "fromDateToDatePeriod": "-4",
        "timePeriod": "86400"  # daily
    }

    row_data = requests.get("https://www.infostock.bg/infostock/control/pricestats/"+code,
                                       params=params, headers=headers)
    historical_data =pd.DataFrame()
    row_data_df = pd.DataFrame(row_data.text.split('\r'))
    for i in range(1,len(row_data_df)):
        if "Исторически данни" in str(row_data_df.iloc[i][0]) :
            historical_data = row_data_df[i:]
            break
    list_hd=[]
    for i in range(1,len(historical_data)):
        if "<td align" in str(historical_data.iloc[i][0]) and "</span>" not in str(historical_data.iloc[i][0]):
            list_hd.append(str(historical_data.iloc[i][0]).strip())

    return list_hd

def sent_data_to_df(list_hd, code):
    d_prices ={}
    for i in range(0,len(list_hd)-1,7):
        el = extract_number (list_hd[i])
        d_prices[el] = [extract_number (list_hd[j]) for j in range (i+1, i+7)]
    df_prices = pd.DataFrame.from_dict(d_prices).T
    df_prices.index.name  = "Date"
    df_prices.columns=[ "Open","Low","High","Close","Volume","Total"]
    file_name = current_path + code + ".csv"
    df_prices.to_csv(file_name)

    return df_prices


for code in codes:
    list_hd = get_data_from_site(code)
    df_prices = sent_data_to_df(list_hd, code)
    df_prices_shifted =df_prices["Close"].shift ( periods=-1 )
    df_prices ['shift_prices'] = df_prices_shifted
    df_prices = df_prices.iloc[:-1]
    df_prices['diff_prices'] = df_prices ['Close'].astype(float)-df_prices ['shift_prices'].astype(float)
    std_dict[code] = np.sqrt(((df_prices["diff_prices"].astype(float).std()**2)*(len(df_prices)-1)/250))

std_df = pd.DataFrame.from_dict(std_dict, orient='index')
std_df.index.name = "Code"
std_df.columns=["StDev"]
std_df.to_csv(current_path + "stdev.csv")