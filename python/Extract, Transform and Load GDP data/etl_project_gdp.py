# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd 
import sqlite3
import numpy as np 
from datetime import datetime

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'}
    response = requests.get(url, headers=headers).text
    html_page=BeautifulSoup(response,'html.parser')
    df=pd.DataFrame(columns=table_attribs)
    rows=html_page.find_all('table',{'class':True})[0].find_all('tr')
    for row in rows:
        col=row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].find('a').contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    df["GDP_USD_millions"]=df["GDP_USD_millions"].apply(lambda x: x.replace(',',''))
    df=df.astype(col_type)
    df["GDP_USD_millions"]=df["GDP_USD_millions"].apply(lambda x: round(x/1000,2))
    df.rename({'GDP_USD_millions':'GDP_USD_billions'}, axis=1, inplace=True)
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path,header=True,index=False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index =False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_output = pd.read_sql(query_statement, sql_connection)
    return query_output

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    now=datetime.now()
    with open("etl_project_log.txt","a") as f:
        f.write(str(now)+' : '+message+'\n')

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
# Declaring known values
url='https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs=["Country", "GDP_USD_millions"]
col_type={"Country":'str', "GDP_USD_millions":'float64'}
db_name='World_Economies.db'
table_name='Countries_by_GDP'
csv_path='Countries_by_GDP.csv'
query_statement=f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
log_progress("Preliminaries complete. Initiating ETL process.")

# Call extract() function
df=extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process.")

# Call transform() function
df=transform(df)
log_progress("Data transformation complete. Initiating loading process.")

# Call load_to_csv()
load_to_csv(df, csv_path)
log_progress("Data saved to CSV file.")

# Initiate SQLite3 connection	
sql_connection=sqlite3.connect('GDP.db')
log_progress("SQL Connection initiated.")

# Call load_to_db()
load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to Database as table. Running the query.")

# Call run_query()
print(run_query(query_statement, sql_connection))
log_progress("Process Complete.")

# Close SQLite3 connection
sql_connection.close()
