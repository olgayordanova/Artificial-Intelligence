from datetime import date
import pandas as pd
from core import get_registered_emission_from_db


def send_spread_to_spread_table(spread_data):
    pass

def calculate_spread(offers_buy,offers_sell):
    buy_price = 0.00
    sell_price = 0.00
    buy_sum = 0
    sell_sum = 0

    for i in range (0,len(offers_buy)):
        buy_sum += int(offers_buy.iat[i, 0])*float(offers_buy.iat[i, 1])/1000
        if buy_sum >=20000:
            buy_price = float(offers_buy.iat[i, 1])/1000
            break

    for i in range (0,len(offers_sell)):
        sell_sum += int(offers_sell.iat[i, 0]) * float(offers_sell.iat[i, 1] )/ 1000
        if sell_sum >=20000:
            sell_price = float(offers_sell.iat[i, 1])/1000
            break

    if sell_price ==0 or buy_price == 0:
        spread = 0.00
    else:
        spread = (sell_price-buy_price)/sell_price

    return spread


def get_data_from_web(codies):
    today = date.today ()
    spread_data = pd.DataFrame ( [] )
    for code in codies:
        html_str = f"https://www.investor.bg/companies/{code}/view/"
        offers_buy = pd.read_html ( html_str, encoding="utf-8" )[3][:-1]
        offers_sell = pd.read_html ( html_str, encoding="utf-8" )[4][:-1]
        spread = calculate_spread ( offers_buy, offers_sell )
        row = {'spread_bse_code_investor': code, 'spread_date': today, 'spread': spread}
        spread_data = spread_data.append ( row, ignore_index=True )
        a=5

    return spread_data


registered_emission = get_registered_emission_from_db('emission_bse_code_investor')
codies = [registered_emission[i][0] for i in range(0, len(registered_emission))]
spread_data = get_data_from_web(codies)
print(spread_data)
send_spread_to_spread_table(spread_data)

# get bse_code_investor from emission
# make to list or some other iterable object
# for code in bse_code_investor
#     make html
#     take from web data
#     get date
#     calculate spread and add it to DataFrame - code, date, spread
#     send data to db table
