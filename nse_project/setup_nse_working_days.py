import pandas as pd
import sqlalchemy as sa
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

    json_path = '/Users/raj/data-samples/nse/equity/nse_work_date.json'

    hlist_df = pd.read_csv("/Users/raj/data-samples/nse/nse_holiday_list.csv", header='infer', usecols={'Date',
                                                                                                        'Description'})
    hlist_df['h_date'] = pd.to_datetime(hlist_df['Date'])
    hlist_df.drop("Date", axis=1, inplace=True)

    date_rng = pd.date_range(start='1/1/2011', end='1/12/2019', freq='D')
    date_df = pd.DataFrame(date_rng, columns=['date'])
    # df = df.set_index('date')

    result_df = pd.merge(date_df, hlist_df, left_on='date', right_on='h_date', how='left')
    result_df['day_of_week'] = result_df['date'].dt.day_name()
    result_df['work_day_flag'] = result_df['day_of_week'].apply(lambda x:  x != 'Sunday' and x != 'Saturday')
    result_df.loc[pd.isna(result_df['Description']), 'work_day_flag'] = True
    result_df.drop("h_date", axis=1, inplace=True)
    result_df = result_df.fillna(' ')
    result_df['date'] = result_df['date'].apply(lambda x: x.date())
    result_df['price_loaded_flag'] = 'N'
    # result_df['date'] = result_df['date'].astype('object')
    print(result_df['date'].dtype)

    # mysql> update nse_work_date set work_day_flag = '0' where day_of_week in ('Saturday' , 'Sunday');
    # mysql> update nse_work_date set work_day_flag = '0' where description != ' ';

    result_df.to_json(json_path, orient='table')
    result_df.to_sql(name='nse_work_date', if_exists='replace', index=False, con=nse_db)

    print(result_df.head(30))
