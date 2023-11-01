# Code for ETL operations on Country-GDP data

# Importing the required libraries
import re
import csv
import requests
from bs4 import BeautifulSoup
import pandas as pd 
import sqlite3
import numpy as np 
from datetime import datetime

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    time_stamp=datetime.now()
    with open('code_log.txt','a') as f:
        f.write(str(time_stamp)+' : '+message+'\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) \
        Chrome/118.0.0.0 Safari/537.36'
    }
    response = requests.get(url, headers=headers).text
    html_page=BeautifulSoup(response,'html.parser')
    df=pd.DataFrame(columns=table_attribs)
    rows=html_page.find_all('table',{'class':re.compile(r'^wikitable')})[0].find_all('tr')
    for value in rows:
        if len(value)!=0 and (len(value.find_all('td'))) !=0:
            dataframe={
                'Name':value.find_all('td')[1].find_all('a')[1].contents[0],
                'MC_USD_Billion':float(value.find_all('td')[2].contents[0])
            }
            df1 = pd.DataFrame(dataframe, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    dict={}
    with open('exchange_rate.csv','r') as csv_file:
        reader = csv.reader(csv_file,delimiter=',')
        fields = next(reader)
        for line in reader:
            dict[line[0]]=float(line[1])
    df['MC_GBP_Billion']=df['MC_USD_Billion'].apply(lambda x:round(x*float(dict['GBP']),2))
    df['MC_EUR_Billion']=df['MC_USD_Billion'].apply(lambda x:round(x*float(dict['EUR']),2))
    df['MC_INR_Billion']=df['MC_USD_Billion'].apply(lambda x:round(x*float(dict['INR']),2))
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path,header=True,index=False)
    
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index =False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_output = pd.read_sql(query_statement, sql_connection)
    return query_output

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

# Declaring known values
url='https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path='https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attribs=['Name','MC_USD_Billion']
col_name=['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
output_path='./Largest_banks_data.csv'
db_name='Banks.db'
table_name='Largest_banks'
log_file='code_log.txt'
query_statement_1=f'SELECT * FROM {table_name}'
query_statement_2=f'SELECT AVG(MC_GBP_Billion) FROM {table_name}'
query_statement_3=f'SELECT Name FROM {table_name} LIMIT 5'

# Call extract() function
df=extract(url, table_attribs)
print(df)
log_progress("Data extraction complete. Initiating Transformation process.")

# Call transform() function
df=transform(df,csv_path)
print('The market capitalization of the 5th largest bank in billion EUR: ',df['MC_EUR_Billion'][4])
log_progress("Data transformation complete. Initiating loading process.")

# Call load_to_csv()
load_to_csv(df, output_path)
log_progress("Data saved to CSV file.")

# Initiate SQLite3 connection	
sql_connection=sqlite3.connect(db_name)
log_progress("SQL Connection initiated.")

# Call load_to_db()
load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to Database as table. Running the query.")

# Call run_query()
print('Query 1:',run_query(query_statement_1, sql_connection))
print('Query 2:',run_query(query_statement_2, sql_connection))
print('Query 3:',run_query(query_statement_3, sql_connection))
log_progress("Process Complete.")

# Close SQLite3 connection
sql_connection.close()
log_progress("Server Connection closed")