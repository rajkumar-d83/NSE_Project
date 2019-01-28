import pandas as pd
import sqlalchemy as sa
import datetime as dt
import credentials


def mysql_connect(db_name):

    user = credentials.login['mysql_user']
    passwd = credentials.login['mysql_pass']
    __sql_alchemy_uri__ = 'mysql+mysqlconnector://' + user + ':' + passwd + '@localhost:3306/'
    __sql_db_name__ = db_name

    db_con = sa.create_engine(__sql_alchemy_uri__ + __sql_db_name__)
    return db_con


def mysql_close(db_con):
    db_con.close()


if __name__ == "__main__":

    nse_db = mysql_connect('nse')
    nse_df = pd.read_csv('/Users/raj/data-samples/nse/equity/Equity_list_NSE.csv')

    nse_df.columns = nse_df.columns.str.strip().str.lower().str.replace(' ', '_').\
        str.replace('(', '').str.replace(')', '')

    nse_df['date_of_listing'] = nse_df['date_of_listing'].apply(lambda x: dt.datetime.strptime(x, '%d-%b-%Y')).dt.date

    print(nse_df.dtypes)

    nse_df.to_sql(name='equity_list', if_exists='replace', index=False, con=nse_db)


# update equity_list set nse_pull_ok = 'Y'  where symbol  in (select distinct a.symbol from equity_data a)
