import pandas as pd
# import tabula
from tabula import read_pdf
import psycopg2 as pg

"""
Този модул чете първоначалните данни от Централен депозитар, БФБ и инвестор-бг
и определя емисиите, подлежащи на наблюдение в текущият период.
Критериите са:
1. Емисията да е допусната до търговия на основния пазар на Борсата.
2. Да има повече от 750 акционери.

Модула се изпълнява два пъти годишно, на 2 март и 2 септември с данни от съответният предходен ден.

Модула обработва 3 файла:
    FREE_FLOAT.pdf; Източник на данни: Централен Депозитар, http://www.csd-bg.bg/index.php?menu=statistika_emitent
    Base_Market_Emission.csv; Източник на данни: Българска фондова борса, https://bse-sofia.bg/bg/market-segmentation
    mapping_bse_codies.xlsx; Източник на данни: Българска фондова борса, https://bse-sofia.bg/bg/market-segmentation

Обединява необходимата информация и я записва в таблица emission на sofix_db.

"""
# TODO clean table if there new period - better do this with all DB before new period


PATH_CD_START_POINT_DATA = 'data/initial_data/FREE_FLOAT.pdf'
PATH_BASE_MARKET_EMISSION_DATA = 'data/initial_data/Base_Market_Emission.csv'
PATH_BSE_CODE_INVESTOR = 'data/initial_data/mapping_bse_codies.xlsx'


def send_data_to_emission_table(participants):
    connection = pg.connect ( "host='127.0.0.1' port='5432' dbname='sofix_db' user='postgres' password='1234'" )
    cur = connection.cursor ()
    df = participants  # pd.read_csv('input_file.csv',parse_dates=['timestamp'],index_col=0)
    ids = cur.execute ( 'select emission_id from "emission"' )
    id = cur.rowcount+1
    for index, row in df.iterrows ():
        emission_id = str ( id )
        emission_name = str ( row[1] )
        emission_isin = str ( index )
        emission_bse_code_old = str ( row[6] )
        emission_bse_code_new = str ( row[0] )
        emission_bse_code_investor = str ( row[5] )
        insertdata = "('" + emission_id + "','" + emission_name + "', '" + emission_isin + "','" + emission_bse_code_old + "','" + emission_bse_code_new + "','" + emission_bse_code_investor + "')"
        id += 1
        print ( "insertdata :", insertdata )
        try:
            cur.execute ( "INSERT INTO emission values " + insertdata )
            print ( "row inserted:", insertdata )
        except pg.IntegrityError:
            print ( "Row already exist " )
            pass
        except Exception as e:
            print ( "some insert error:", e, "ins: ", insertdata )
        connection.commit ()


def read_data_from_cd(path):
    # TODO - Евентуално четенето да го преизползвам  - да го разбия на обща и специфична част.
    cd_data = read_pdf ( path, pages="all", area=(11, 0, 100, 110), stream=True, relative_area=True )
    for i in range ( 0, len ( cd_data ) ):
        cd_data[i].columns = ['name', 'isin', 'total_count', 'free_float', 'shareholders_count']
    cd_data_df = pd.concat ( [el for el in cd_data], ignore_index=True, axis=0 )
    cd_data_df = cd_data_df.dropna ()
    cd_data_df_clean = cd_data_df[cd_data_df.shareholders_count >= 750]
    cd_data_df_clean = cd_data_df_clean.reset_index ()
    cd_data = cd_data_df_clean.set_index ( 'isin' )
    return cd_data


def read_data_from_bse(path):
    base_market_emission_data = pd.read_csv ( path, sep=';' )
    base_market_emission_data = base_market_emission_data[
        (base_market_emission_data['ПАЗАРЕН СЕГМЕНТ'] == ('Сегмент акции Standard')) | (
                    base_market_emission_data['ПАЗАРЕН СЕГМЕНТ'] == ('Сегмент акции Premium')) | (
                    base_market_emission_data['ПАЗАРЕН СЕГМЕНТ'] == (
                'Сегмент за дружества със специална инвестиционна цел'))]
    emission_data = base_market_emission_data.filter ( ['КОД', 'ИМЕ', 'ISIN', 'БРОЙ ЦЕННИ КНИЖА'], axis=1 )
    emission_data = emission_data.reset_index ()
    bse_data = emission_data.set_index ( 'ISIN' )
    return bse_data


def add_bse_code_investor(participants, path):
    mapping_codes_table = pd.read_excel ( path, sheet_name='Sheet1' )
    mapping_codes_table = mapping_codes_table.reset_index ()
    mapping_codes_table = mapping_codes_table.set_index ( 'isin' )
    mapping_codes_table = mapping_codes_table.filter ( ['bse_code_investor', 'bse_code_old'], axis=1 )
    end_result = pd.merge ( participants, mapping_codes_table, how="left", on='isin', validate="one_to_one" )
    return end_result


def generate_participants(bse_data, cd_data, path):
    participants = pd.concat ( [bse_data, cd_data], axis=1, join="inner" )
    participants = participants.loc[:, ~participants.columns.duplicated ( keep='first' )]
    participants = participants.filter ( ['КОД', 'ИМЕ', 'total_count', 'free_float', 'shareholders_count'], axis=1 )
    participants.columns = ['bse_code', 'name', 'total_count', 'free_float', 'shareholders_count']
    participants.index.name = 'isin'
    participants = add_bse_code_investor ( participants, path )
    return participants


cd_data = read_data_from_cd ( PATH_CD_START_POINT_DATA )
bse_data = read_data_from_bse ( PATH_BASE_MARKET_EMISSION_DATA )
participants = generate_participants ( bse_data, cd_data, PATH_BSE_CODE_INVESTOR )
send_data_to_emission_table ( participants )


# https://tabula-py.readthedocs.io/en/latest/tabula.html
# https://stackoverflow.com/questions/23284759/opening-a-pdf-and-reading-in-tables-with-python-pandas
# https://www.postgresqltutorial.com/postgresql-python/query/
