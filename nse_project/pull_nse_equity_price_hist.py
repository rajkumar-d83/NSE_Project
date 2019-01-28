from nsepy import get_history
import sqlalchemy as sa
import pandas as pd
import credentials
from pandas.io import sql
from datetime import date
import time


def mysql_connect(db_name):

    user = credentials.login['mysql_user']
    passwd = credentials.login['mysql_pass']
    __sql_alchemy_uri__ = 'mysql+mysqlconnector://' + user + ':' + passwd + '@localhost:3306/'
    __sql_db_name__ = db_name

    db_con = sa.create_engine(__sql_alchemy_uri__ + __sql_db_name__)
    return db_con


def mysql_close(db_con):
    db_con.close()


def extract_json(symbol, s_dt):

    data = get_history(symbol, start=date(s_dt.year, s_dt.month, s_dt.day), end=date(2019, 1, 11))
    data.columns = data.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')

    return data


if __name__ == "__main__":

    nse_db = mysql_connect('nse')

    query = '''select symbol,date_of_listing from equity_list where nse_pull_ok = "N" '''
    # query = '''select symbol from equity_list where symbol = "AKSHOPTFBR" '''
    df = pd.read_sql_query(query, nse_db)

    x = map(list, df.values)
    for i in x:
        df1 = extract_json(i[0], i[1])
        # d1f = df1.set_index('date')
        json_path = '/Users/raj/data-samples/nse/equity/' + i[0] + '.json'
        csv_path = '/Users/raj/data-samples/nse/equity/' + i[0] + '.csv'
        # print(json_path)
        df1.to_json(json_path, orient='table')

        df1.to_csv(csv_path, sep=',', na_rep='', float_format=None, columns=None, header=True, index=True,
                   index_label=None, mode='w', encoding=None, compression=None, quoting=None, quotechar='"',
                   line_terminator='\n', chunksize=None, tupleize_cols=None, date_format=None, doublequote=True,
                   escapechar=None, decimal='.')
        df1.to_sql(name='equity_price_history', if_exists='append', index=True, con=nse_db)

        query = '''update equity_list set nse_pull_ok ='Y' where symbol = "''' + i[0] + '''" '''
        print(query)
        sql.execute(query, nse_db)
        time.sleep(120)

        # print(df1)
