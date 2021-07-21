import numpy as np
import pandas as pd
# import tabula
from tabula import read_pdf
import psycopg2 as pg
import re
import datetime

from core import get_registered_emission_from_db, get_date_from_filename, read_data_from_cd

"""
Този модул чете данни за фрий флоута на компаниите регистрирани в Централен депозитар.
Модула се изпълнява ежедневно по данни от предходния ден.

Модула обработва 1 файл:
    FREE_FLOAT_-yyyymmdd.pdf; Източник на данни: Централен депозитар, http://www.csd-bg.bg/index.php?menu=statistika_emitent

Взема информация за всички регистрирани в ЦД емисии, отсява данни само наблюдаваните емисии и ги записва в таблица free_float на sofix_db.
"""

PATH_TO_DAILY_PDF_DATA = 'data/daily_data/FREE_FLOAT_20210716.pdf'

def merge_data(cd_data, registered_emission):
    registered_emission = pd.DataFrame(registered_emission, columns=["isin"])
    registered_emission = registered_emission.set_index('isin')
    freefloat_data = pd.merge ( registered_emission, cd_data, how="left", on='isin', validate="one_to_one" )
    return freefloat_data

def send_data_to_freefloat_table(freefloat_data):
    connection = pg.connect ( "host='127.0.0.1' port='5432' dbname='sofix_db' user='postgres' password='1234'" )
    cur = connection.cursor ()
    df = freefloat_data
    #TODO Това няма да работи за по стари файлове - избира само последната обработена дата - да се оправи
    sql_str = "select free_float_date from free_float order by free_float_date desc limit 1"
    cur.execute(sql_str)
    try:
        last_date = cur.fetchone()[0]
        if datetime.datetime.strptime(str(last_date), '%Y-%m-%d') == datetime.datetime.strptime(get_date_from_filename(PATH_TO_DAILY_PDF_DATA, format_data_type = 'yyyymmdd'), '%Y-%m-%d'):
            print(f'The file has already been processed')
            return
    except:
        pass

    cur.execute ( "select free_float_id from free_float" )
    id = cur.rowcount+1

    for index, row in df.iterrows ():
        free_float_id = str(id)
        free_float_date = str(row[5])
        free_float_isin = str ( index )
        emission_shares_count = str(int(row[2]))
        free_float_shares_count = str(int(row[3]))
        shareholders_count = str ( int(row[4] ))
        insertdata = "('" + free_float_id + "','" + free_float_date + "', '" + free_float_isin + "', '" + emission_shares_count + "','" + free_float_shares_count + "','" + shareholders_count + "')"

        print ( "insertdata :", insertdata )
        try:
            cur.execute ( "INSERT INTO free_float values " + insertdata )
            id += 1
            print ( "row inserted:", insertdata )
        except pg.IntegrityError:
            print ( "Row already exist " )
            pass
        except Exception as e:
            print ( "some insert error:", e, "ins: ", insertdata )
        connection.commit ()


cd_data = read_data_from_cd ( PATH_TO_DAILY_PDF_DATA, shareholders_restriction_count = False )
free_float_data = merge_data(cd_data, get_registered_emission_from_db('isin'))
send_data_to_freefloat_table(free_float_data)

