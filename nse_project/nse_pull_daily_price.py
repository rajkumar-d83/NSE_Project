from nsepy.history import get_price_list
import sqlalchemy as sa
import pandas as pd
import credentials as con
from datetime import date
import boto3
# import s3fs


def get_s3_config_data():

    __session__ = boto3.Session(
        aws_access_key_id=con.aws['credential']['access_key'],
        aws_secret_access_key=con.aws['credential']['secret_key']
    )
    __s3__ = __session__.resource('s3')
    return __s3__


def s3_upload_obj(resource, bucket, key, file_name):
    data = open(file_name, 'rb')
    resource.Bucket(bucket).put_object(Key=key, Body=data)


# def s3_file_write(bucket_name, file_name, data_frame_name, file_type):
#    # Use 'w' for py3, 'wb' for py2
#    s3_fs = s3fs.S3FileSystem(key=key, secret=secret)
#    if file_type == 'csv':
#        with s3_fs.open(bucket_name + '/' + file_name, 'w') as f:
#            data_frame_name.to_csv(f)
#    else:
#        if file_type == 'json':
#            with s3_fs.open(bucket_name + '/' + file_name, 'w') as f:
#                data_frame_name.to_json(f)


def mysql_connect(db_name):

    user = con.mysql['mysql_user']
    passwd = con.mysql['mysql_pass']
    __sql_alchemy_uri__ = 'mysql+mysqlconnector://' + user + ':' + passwd + '@localhost:3306/'
    __sql_db_name__ = db_name

    db_con = sa.create_engine(__sql_alchemy_uri__ + __sql_db_name__)
    return db_con


def mysql_close(db_con):
    db_con.close()


def extract_nse_daily(s_dt):

    data = get_price_list(dt=date(s_dt.year, s_dt.month, s_dt.day))
    data.columns = data.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')

    return data


if __name__ == "__main__":

    nse_db = mysql_connect('nse')
    s3 = get_s3_config_data()
    s3_bucket = con.aws['s3']['bucket']

    query = ''' select date from nse_work_date where date > "2019-01-01" and work_day_flag = 1 
                and price_received_flag = "N" limit 10; '''

    df = pd.read_sql_query(query, nse_db)

    x = map(list, df.values)
    for i in x:

        # extract the daily nse equity price file
        df1 = extract_nse_daily(i[0])

        json_path = '/Users/raj/data-samples/nse/equity/nse_price_' + str(i[0]) + '.json'
        csv_path = '/Users/raj/data-samples/nse/equity/nse_price_' + str(i[0]) + '.csv'
        # print(json_path)

        # copy into local directory
        df1.to_json(json_path, orient='table')
        df1.to_csv(csv_path, sep=',', na_rep='', mode='w', quotechar='"', line_terminator='\n')

        # upload the files to AWS - s3 bucket
        s3_key = con.aws['s3']['key'] + 'csv/nse_price_' + str(i[0]) + '.csv'
        s3_upload_obj(s3, s3_bucket, s3_key, csv_path)
        # s3_file_write(s3_bucket, s3_key, df1, 'csv')

        s3_key = con.aws['s3']['key'] + 'json/nse_price_' + str(i[0]) + '.json'
        s3_upload_obj(s3, s3_bucket, s3_key, json_path)
        # s3_file_write(s3_bucket, s3_key, df1, 'json')

        # upload to local mysql table
        df1.to_sql(name='equity_price_hist', if_exists='append', index=True, con=nse_db)

        # update the flag int he control file for successful processing
        query = ''' update nse_work_date set price_received_flag ="Y" where date = "''' + str(i[0]) + '''" '''
        print(query)
        pd.execute(query, nse_db)

        # take a break
        # time.sleep(120)

