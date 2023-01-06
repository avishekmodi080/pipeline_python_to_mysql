# -*- coding: utf-8 -*-
"""
Created on Wed Jan  4 12:24:42 2023

@author: Avishek
"""

import pandas as pd
import glob
import numpy as np
import os
import mysql.connector 
from mysql.connector import Error
from sqlalchemy import create_engine

os.chdir("C:\\Users\\Avishek\\BANK_STMT")

csv_files = glob.glob('*.{}'.format('csv'))
csv_files

counter = 1

## appending all the csv files and adding cust_id
csv_appended = pd.DataFrame()
for file in csv_files:
    df_temp = pd.read_csv(file)
    df_temp['cust_id'] = counter
    counter+= 1
    csv_appended = csv_appended.append(df_temp, ignore_index=True)

## copying the original csv file to df
df = csv_appended.copy()

## replacing nan values with 0:
df = df.replace(np.nan, 0)

## changing the date format of df to sql format- %y-%m-%d:
df['txn_date'] = pd.to_datetime(df['txn_date'])
df['txn_date'] = pd.to_datetime(df['txn_date'],format="%y-%m-%d")


##cleaning the df column names: 
df.columns = df.columns.str.lower().str.replace(" ", "_")
df.reset_index(drop=True, inplace=True)
df.head()

## establishing the connection to database
user_name = 'root'
pswd_ = "Knowlvers1@1"
host_name = "localhost"
db_name = "credable"
db_connection = None

try:
    db_connection = mysql.connector.connect(host=host_name, user=user_name, password=pswd_, database=db_name)
except Error as e:
    raise e
else:
    print('Connected')

curr = db_connection.cursor()

## cretating table 
curr.execute("""CREATE TABLE table_1 (
        txn_date DATE NOT NULL,
        `description` varchar(355),
        debit float,
        credit float,
        balance double,
        account_name varchar(355),
        account_number varchar(355),
        `mode` varchar(30),
        entity varchar(30),
        source_of_trans varchar(355),
        sub_mode varchar(30),
        transaction_type varchar(30),
        bank_name varchar(30),
        lid varchar(355),
        cust_id int
                        );""")

def insert_into_table(curr, txn_date,description,debit,credit,balance,account_name,account_number,mode,entity,source_of_trans,sub_mode,transaction_type,bank_name,lid,cust_id):
    query = (""" INSERT INTO table_1 (txn_date,`description`,debit,credit,balance,account_name,account_number,`mode`,entity,source_of_trans,sub_mode,transaction_type,bank_name,lid,cust_id)
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
             """)
    row_to_insert = (txn_date,description,debit,credit,balance,account_name,account_number,mode,entity,source_of_trans,sub_mode,transaction_type,bank_name,lid,cust_id)
    curr.execute(query, row_to_insert)
    
def df_to_db(curr, df):
    for i, row in df.iterrows():
        insert_into_table(curr, row['txn_date'], row['description'], row['debit'], row['credit'], row['balance'], row['account_name'], row['account_number'], row['mode'], row['entity'], row['source_of_trans'], row['sub_mode'], row['transaction_type'], row['bank_name'], row['lid'], row['cust_id'])

df_to_db(curr, df)
db_connection.commit()













































































