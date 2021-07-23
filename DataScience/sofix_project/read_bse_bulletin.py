import numpy as np
import pandas as pd
import psycopg2 as pg
import datetime
import os
import glob
from core import get_date_from_filename, get_registered_emission_from_db, get_current_week

"""
Този модул чете данни за цените и изтъргуваните обеми на наблюдаваните компании.
Модула се изпълнява ежедневно по данни от предходния ден.

Модула обработва 1 файл:
    Bltexddmmyyy.xls; Източник на данни: БФБ - официален бюлетин

Взема информация за дневната търговия на наблюдаваните емисии и ги записва в таблица prices_count_shares на sofix_db.
"""

current_path = os.getcwd ()+'\\data\\daily_data\\'
paths = glob.glob ( os.path.join ( current_path, "*.xls" ) )

def send_data_to_prices_count_shares_table(df_bse_bulletin_data):
    connection = pg.connect ( "host='127.0.0.1' port='5432' dbname='sofix_db' user='postgres' password='1234'" )
    cur = connection.cursor ()
    df = df_bse_bulletin_data
    #TODO Това няма да работи за по стари файлове - избира само последната обработена дата - да се оправи
    sql_str = "select pcs_date from prices_count_shares order by pcs_date desc limit 1"
    cur.execute(sql_str)
    try:
        last_date = cur.fetchone()[0]
        if datetime.datetime.strptime(str(last_date), '%Y-%m-%d') == datetime.datetime.strptime(get_date_from_filename(path, format_data_type = 'yyyymmdd'), '%Y-%m-%d'):
            print(f'The file has already been processed')
            return
    except:
        pass

    cur.execute ( "select pcs_id from prices_count_shares" )
    id = cur.rowcount+1

    for index, row in df.iterrows ():
        pcs_id = str(id)
        pcs_date = str(row[4])
        pcs_bse_code_new = str ( row[0] )
        pcs_product_price = str(round(float(row[2]), 4))
        pcs_price = str(round(float(row[3]),4))
        pcs_daily_count = str(int(row[1]))
        pcs_week = str(int(row[5]))
        insertdata = "('" + pcs_id + "','" + pcs_date + "', '" + pcs_bse_code_new + "', '" + pcs_price + "','" + pcs_daily_count + "','" + pcs_week + "','" + pcs_product_price + "')"

        print ( "insertdata :", insertdata )
        try:
            cur.execute ( "INSERT INTO prices_count_shares values " + insertdata )
            id += 1
            print ( "row inserted:", insertdata )
        except pg.IntegrityError:
            print ( "Row already exist " )
            pass
        except Exception as e:
            print ( "some insert error:", e, "ins: ", insertdata )
        connection.commit ()


def merge_data(df_bse_bulletin_data, registered_emission):
    registered_emission = pd.DataFrame(registered_emission, columns=["emission_bse_code_new"])
    registered_emission = registered_emission.set_index('emission_bse_code_new')
    merged_data = pd.merge ( registered_emission, df_bse_bulletin_data, how="left", on='emission_bse_code_new', validate="one_to_one" )
    return merged_data


def read_bse_bulletin(path):
    current_date=get_date_from_filename(path, format_data_type = 'ddmmyyyy')
    df_bse_data = pd.read_excel(path, sheet_name='Пазари акции и др. недългови ЦК')
    df_bse_data =  df_bse_data.iloc[18:]
    df_bse_data = df_bse_data[['Unnamed: 0', 'Unnamed: 7', 'Unnamed: 12', 'Unnamed: 13']]
    df_bse_data.columns=['emission_bse_code_new', 'daily_count', 'product_price', 'price']
    df_bse_data['daily_count'] = df_bse_data['daily_count'].replace(np.nan, 0)
    df_bse_data['current_date'] = current_date
    df_bse_data['current_week'] = get_current_week(path, format_data_type = 'ddmmyyyy')
    return df_bse_data

for path in paths:
    df_bse_bulletin_data = read_bse_bulletin ( path )
    df_bse_bulletin_data = merge_data(df_bse_bulletin_data, get_registered_emission_from_db('emission_bse_code_new'))
    send_data_to_prices_count_shares_table(df_bse_bulletin_data)
