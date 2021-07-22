import datetime
import pandas as pd
import psycopg2 as pg
from core import get_registered_emission_from_db

"""
Този модул чете данни за текущите котировки при наблюдаваните компании и изчислява спреда между тях.
Модула се изпълнява веднъж дневно в периода 10:30:00 до 17:00:00 часа.

Модула тегли данни от сайна на Инвестор.бг:
    https://www.investor.bg/companies/{code}/view/

След като се изчисли, спреда се записва в таблица spread на sofix_db.
"""

def send_spread_to_spread_table(spread_data):
    connection = pg.connect ( "host='127.0.0.1' port='5432' dbname='sofix_db' user='postgres' password='1234'" )
    cur = connection.cursor ()
    df = spread_data
    #TODO Това няма да работи за по стари файлове - избира само последната обработена дата - да се оправи
    sql_str = "select spread_date from spread order by spread_date desc limit 1"
    cur.execute(sql_str)
    try:
        last_date = cur.fetchone()[0]
        if datetime.datetime.strptime(str(last_date), '%Y-%m-%d') == datetime.date.today ():
            print(f'The file has already been processed')
            return
    except:
        pass

    cur.execute ( "select spread_id from spread" )
    id = cur.rowcount+1

    for index, row in df.iterrows ():
        spread_id = str(id)
        spread_date = str(row[1])
        spread_bse_code_ninvestor = str ( row[0] )
        spread = str(round(float(row[2]),6))
        insertdata = "('" + spread_id + "','" + spread_bse_code_ninvestor + "', '" + spread_date + "', '" + spread + "')"

        print ( "insertdata :", insertdata )
        try:
            cur.execute ( "INSERT INTO spread values " + insertdata )
            id += 1
            print ( "row inserted:", insertdata )
        except pg.IntegrityError:
            print ( "Row already exist " )
            pass
        except Exception as e:
            print ( "some insert error:", e, "ins: ", insertdata )
        connection.commit ()

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
    today = datetime.date.today ()
    spread_data = pd.DataFrame ( [] )
    for code in codies:
        html_str = f"https://www.investor.bg/companies/{code}/view/"
        offers_buy = pd.read_html ( html_str, encoding="utf-8" )[3][:-1]
        offers_sell = pd.read_html ( html_str, encoding="utf-8" )[4][:-1]
        spread = calculate_spread ( offers_buy, offers_sell )
        row = {'spread_bse_code_investor': code, 'spread_date': today, 'spread': spread}
        spread_data = spread_data.append ( row, ignore_index=True )

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
