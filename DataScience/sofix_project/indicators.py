import numpy as np
import pandas as pd
import psycopg2 as pg
import datetime

def count_deals_indicator():
    connection = pg.connect ( "host='127.0.0.1' port='5432' dbname='sofix_db' user='postgres' password='1234'" )
    cur = connection.cursor ()
    cur.execute ( "select * from count_deals_view order by count desc" )
    count_deals = pd.DataFrame(cur.fetchall())
    cur.close()
    connection.close()
    return count_deals

def count_spread_factor(df, code, count_dates):
    df_codes = df[df[0]==code]
    if len(df_codes)/len(count_dates)< 2/3:
        return False
    return True

def spread():
    connection = pg.connect ( "host='127.0.0.1' port='5432' dbname='sofix_db' user='postgres' password='1234'" )
    cur = connection.cursor ()
    cur.execute ( "select * from spread_view" )
    spread = pd.DataFrame(cur.fetchall())
    count_dates = spread[1].unique()
    spread_index = spread[(spread[2]==0.0)].index
    spread.drop ( spread_index, inplace=True )

    for code in  spread[0].unique():
        count_factor = count_spread_factor(spread, code, count_dates)
        if not count_factor:
            spread.drop(spread[spread[0] == code].index, inplace=True)

    spread = spread.groupby ( by=[0] ).mean ().sort_values ( by=[2] )
    cur.close()
    connection.close()
    return spread

def get_free_float_coef(df):
    coef = pd.DataFrame(df.groupby ( ['bse_code_new'] )['free_float_coef'].median ().sort_values (ascending=False))
    coef.columns = ['free_float_coef_median'] #
    coef.index.name = 'bse_code_new'
    return coef

def get_free_float_mc_last_date(df):
    df_id = df[(df['date'] != df.date.max ())].index
    if not df_id.empty:
        df.drop ( df_id, inplace=True )
    return df

def free_float_market_capitalization():
    connection = pg.connect ( "host='127.0.0.1' port='5432' dbname='sofix_db' user='postgres' password='1234'" )
    cur = connection.cursor ()
    cur.execute ( "select * from free_float_mc_view" )
    free_float_mc = pd.DataFrame(cur.fetchall())
    free_float_mc.columns = ['bse_code_new', 'date', 'price', 'free_float_shares_count', 'emission_shares_count']
    free_float_mc ['free_float_coef'] =(free_float_mc['free_float_shares_count'] / free_float_mc['emission_shares_count'])#*(free_float_mc['price'].apply ( lambda x: float ( x ) )))

    free_float_coef = get_free_float_coef(free_float_mc) # изчисляваме free_float_coef
    free_float_mc_last_date = get_free_float_mc_last_date(free_float_mc) # вземаме последната дата и цената на емисията за нея

    free_float_mc = pd.merge ( free_float_mc_last_date, free_float_coef, how="left", on='bse_code_new', validate="one_to_one" )
    free_float_mc['market_capitalization'] = free_float_mc['emission_shares_count']*free_float_mc['free_float_coef_median']*(free_float_mc['price'].apply ( lambda x: float ( x ) ))
    free_float_mc = free_float_mc.filter(['bse_code_new', 'market_capitalization'], axis=1).sort_values(by=['market_capitalization'],ascending=False )
    df_id = free_float_mc[(free_float_mc['market_capitalization'] < 10000000)].index
    free_float_mc.drop ( df_id, inplace=True )

    cur.close()
    connection.close()
    return free_float_mc

def weekly_volume():
    connection = pg.connect ( "host='127.0.0.1' port='5432' dbname='sofix_db' user='postgres' password='1234'" )
    cur = connection.cursor ()
    cur.execute ( "select * from weekly_volume_view" )
    weekly_volume = pd.DataFrame(cur.fetchall())
    weekly_volume_result = pd.DataFrame(weekly_volume.groupby([0, 2])[1].median().sort_values(ascending=False))
    cur.close()
    connection.close()
    return weekly_volume_result

def report_competition(count_deals,spread,free_float_market_capitalization,weekly_volume ):
    # реиндексиране по емисия за всеки сет с едно и също име на емисия и после конкатениране. Сума от ранговете и след това сорт по тази сума
    count_deals['rank']=count_deals[1].rank( method='min', ascending=False)*0.25
    spread['rank'] = spread[2].rank(method='min')*0.25
    free_float_market_capitalization['rank'] = free_float_market_capitalization['market_capitalization'].rank(method='min', ascending=False)*0.25
    weekly_volume['rank'] = weekly_volume[1].rank(method='min', ascending=False)*0.25
    competitors  = pd.concat(count_deals,spread,free_float_market_capitalization,weekly_volume, keys=[0])
    # pd.concat(frames, keys=["x", "y", "z"])
    return


count_deals = count_deals_indicator()
spread = spread()
free_float_market_capitalization = free_float_market_capitalization()
weekly_volume = weekly_volume()
report_competition = report_competition(count_deals,spread,free_float_market_capitalization,weekly_volume )


print(len(count_deals))
print(len(spread))
print(len(free_float_market_capitalization))
print(len(weekly_volume))
print((count_deals))
print((spread))
print((free_float_market_capitalization))
print((weekly_volume))