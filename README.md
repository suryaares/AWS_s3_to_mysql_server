# AWS_s3_to_mysql_server

import mysql.connector as mysql
import pandas as pd
import numpy as np
import pymysql
import mysql.connector
import MySQLdb
from mysql.connector import Error
import time
from datetime import datetime
from datetime import datetime as dt
import boto3
import pandas
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication import row_event

client = boto3.client(
    's3',
    aws_access_key_id='AKIAxxxxxNWQALKMJ',
    aws_secret_access_key='IoYBZuxxxxxxxkBxw4djSKafL51',
    region_name='us-east-1'
)
resource = boto3.resource(
    's3',
    aws_access_key_id='AKIxxxxxxxNWQALKMJ',
    aws_secret_access_key='IoYBxxxxxxxxZN5RcakBxw4djSKafL51',
    region_name='us-east-1'

)
obj = client.get_object(
    Bucket='s3tomysqltodenodo',
    Key='firewall_log.csv'
)
df =pd.read_csv(obj['Body'])
df=df.fillna(0)
n = len(df)

df.columns
df['log_tmstmp'] = str(datetime.now())
print(df)

conn = pymysql.connect(
    host="5xxxxxx50",
    user="cldera",
    password="password",
    port=3306,
    db="denododb",
    
)

mysql_settings = {'host': '54xxxxxx0', 
                  'port': 3306, 
                  'user': 'cldxxxxra', 
                  'passwd': 'password'
                 }
try:
    conn = mysql.connector.connect(host='54.2xxxxxx.250', db='dxxxxxdb', user='cldera', passwd='password', port=3306)
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        #cursor.execute('DROP TABLE IF EXISTS akidodata;')
# in the below line please pass the create table statement which you want #to create
        #cursor.execute("CREATE TABLE akidodata (log_tmstmp VARCHAR(255),rounded_logtmstmp VARCHAR(255),domain VARCHAR(255),event_id VARCHAR(255),event_time VARCHAR(255),host VARCHAR(255),message TEXT,mnemonic_number VARCHAR(255),access_group VARCHAR(255),raw_log TEXT,source_ip VARCHAR(255),source_ip_port VARCHAR(255),destination_ip VARCHAR(255),destination_ip_port VARCHAR(255),log_type VARCHAR(255),customer_tag VARCHAR(255))")
        print("Table is created....")
        for i,row in df.iterrows():
            #here %S means string values 
            sql = "INSERT INTO akidodata VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            # the connection is not auto committed by default, so we must commit to save our changes
            conn.commit()
except Error as e:
            print("Error while connecting to MySQL", e)
