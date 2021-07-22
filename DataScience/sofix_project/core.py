import re
from datetime import datetime

import pandas as pd
import psycopg2 as pg
from tabula import read_pdf


def get_registered_emission_from_db(ind_col):
    connection = pg.connect ( "host='127.0.0.1' port='5432' dbname='sofix_db' user='postgres' password='1234'" )
    cur = connection.cursor ()
    sql_str = f"select {ind_col} from emission"
    cur.execute ( sql_str )
    registered_emission = cur.fetchall()
    cur.close()
    connection.close()
    return registered_emission

def get_date_from_filename(path, format_data_type):
    file_name = path.split('/')[-1]
    number = re.search(r'\d+', file_name).group(0)
    if format_data_type =='yyyymmdd': # FREE_FLOAT_-yyyymmdd.pdf
        day, month, year = number[-2:], number[4:6], number[0:4]
    else:
        day, month, year = number[0:2], number[2:4], number[-4:] #  DealsInfostock-ddmmyyyy.xlsx
    return f'{year}-{month}-{day}'

def get_current_week(path, format_data_type):
    current_date = get_date_from_filename(path, format_data_type)
    current_date=datetime.strptime(current_date,  '%Y-%m-%d')
    week_number =current_date.strftime ( "%U" )
    return week_number

def read_data_from_cd(path, shareholders_restriction_count):
    cd_data = read_pdf ( path, pages="all", area=(11, 0, 100, 110), stream=True, relative_area=True )
    for i in range ( 0, len ( cd_data ) ):
        cd_data[i].columns = ['name', 'isin', 'total_count', 'free_float', 'shareholders_count']
    cd_data_df = pd.concat ( [el for el in cd_data], ignore_index=True, axis=0 )
    cd_data_df = cd_data_df.dropna ()
    if shareholders_restriction_count:
        cd_data_df = cd_data_df[cd_data_df.shareholders_count >= 750]
    cd_data_df = cd_data_df.reset_index ()
    cd_data = cd_data_df.set_index ( 'isin' )
    if not shareholders_restriction_count:
        current_date = get_date_from_filename(path, format_data_type = 'yyyymmdd')
        cd_data['current_date'] = current_date
    return cd_data


