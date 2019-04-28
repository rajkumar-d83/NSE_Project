import sqlalchemy as sa
import pandas as pd
import datetime
import requests
import json


import boto3
# import s3fs

import sys

sys.path.append('..')

import credentials as con


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


def mysql_connect(db_name):

    user = con.mysql['mysql_user']
    passwd = con.mysql['mysql_pass']
    mhost = con.mysql['mysql_host']
    __sql_alchemy_uri__ = 'mysql+mysqlconnector://' + user + ':' + passwd + '@' + mhost + ':3306/'
    __sql_db_name__ = db_name

    db_con = sa.create_engine(__sql_alchemy_uri__ + __sql_db_name__)
    return db_con


def mysql_close(db_con):
    db_con.close()


def extract_business_news():

    url = 'https://newsapi.org/v2/top-headlines?country=in&category=business&apiKey=68c0d2e172284f0694be5d1880d89a85'
    r = requests.get(url)
    data = json.loads(r.content.decode())
    return data


if __name__ == "__main__":

    news_db = mysql_connect('news')
    s3 = get_s3_config_data()
    s3_bucket = con.aws['s3']['news_bucket']

    now = datetime.datetime.now()
    timestamp = now.strftime("%Y-%m-%d-%H-%M")

    json_data = extract_business_news()
    df1 = pd.DataFrame(json_data)

    json_path = '/Users/raj/data-samples/news/india/buss_news_' + timestamp + '.json'
    # json_path = '/home/ec2-user/nse_project/news/buss_news_' + timestamp + '.json'
    df1.to_json(json_path, orient='table', index=False)

    s3_key = con.aws['s3']['news_key'] + 'news_feed_' + timestamp + '.json'
    s3_upload_obj(s3, s3_bucket, s3_key, json_path)

    x = map(list, df1.values)
    for i in x:

        # json_data2 = json.loads(i[2])
        df2 = pd.DataFrame(i[2])

        # upload to local mysql table
        df2.to_sql(name='in_business_news', if_exists='append', index=False, con=news_db)





