import pandas as pd
import psycopg2 as pg
import datetime
from core import get_date_from_filename
import os
import glob
"""
Този модул чете данни за сключените на БФБ сделки.
Модула се изпълнява ежедневно по данни от предходния ден.

Модула обработва 1 файл:
    DealsInfostock-ddmmyyyy.xlsx; Източник на данни: infostock.bg, https://www.infostock.bg/infostock/control/transactions

Взема информация за всички сключени сделки с наблюдаваните емисии и я записва в таблица count_deals на sofix_db.
"""
# TODO clean table if there new period - better do this with all DB before new period df

current_path = os.getcwd ()+'\\data\\daily_data\\'
paths = glob.glob ( os.path.join ( current_path, "*.xlsx" ) )

def send_data_to_count_deals_table(df_deals, path):
    connection = pg.connect ( "host='127.0.0.1' port='5432' dbname='sofix_db' user='postgres' password='1234'" )
    cur = connection.cursor ()
    df = df_deals
    #TODO Това няма да работи за по стари файлове - избира само последната обработена дата - да се оправи
    cur.execute("select deal_date from count_deals order by deal_id desc limit 1")
    try:
        last_date = cur.fetchone()[0]
        if datetime.datetime.strptime(str(last_date), '%Y-%m-%d') == datetime.datetime.strptime(get_date_from_filename(path, format_data_type='ddmmyyyy'), '%Y-%m-%d'):
            print(f'The file has already been processed')
            return
    except:
        pass

    cur.execute ( "select deal_id from count_deals" )
    id = cur.rowcount+1

    for index, row in df.iterrows ():
        deal_id = str(id)
        deal_date = str(row[4])
        deal_time = str ( row[0] )
        deal_price = str ( row[1] )
        deal_bse_code_new = str(row[3])
        deal_count = str ( row[2] )
        insertdata = "('" + deal_id + "','" + deal_date + "', '" + deal_time + "', '" + deal_bse_code_new + "','" + deal_price + "','" + deal_count + "')"

        print ( "insertdata :", insertdata )
        try:
            cur.execute ( "INSERT INTO count_deals values " + insertdata )
            id += 1
            print ( "row inserted:", insertdata )
        except pg.IntegrityError:
            print ( "Row already exist " )
            pass
        except Exception as e:
            print ( "some insert error:", e, "ins: ", insertdata )
        connection.commit ()


def read_deals_infostock(path):
    current_date=get_date_from_filename(path, format_data_type = 'ddmmyyyy')
    df_deals = pd.read_excel(path, sheet_name='Sheet1')
    df_bse_code = df_deals['Код'].str.split(pat=" / ", expand=True)[0]
    df_deals = df_deals.filter(['Час', 'Цена', 'Брой'], axis=1)
    df_deals.index.name = 'index'
    df_bse_code.index.name = 'index'
    df_bse_code.name = 'bse_code'
    df_deals_result = pd.merge ( df_deals, df_bse_code, how="left", on='index', validate="one_to_one" )
    df_deals_result['current_date'] = current_date
    return df_deals_result

for path in paths:
    df_deals_data = read_deals_infostock ( path )
    send_data_to_count_deals_table ( df_deals_data, path )


