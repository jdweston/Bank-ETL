import requests
from bs4 import BeautifulSoup
import pandas as pd 
import numpy as np 
from datetime import datetime
import sqlite3

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion'] #final attribs ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = 'exchange_rate.csv'
output_path = './Largest_banks_data.csv'
log_file = 'code_log.txt'

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:-%M:-%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('code_log.txt', 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')

log_progress('Preliminaries complete. Initiating ETL process')

def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns = table_attribs)
    table = data.find_all('tbody')
    rows = table[0].find_all('tr')

    for row in rows:
        if row.find('td') is not None:

            col = row.find_all('td')
            data_dict = {"Name" : str(col[1].find_all('a')[1]['title']), 
                        "MC_USD_Billions" : float(col[2].contents[0][:-1])}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index = True)
    return df

df = extract(url,table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

def transform(df, csv_path):
    exch_df = pd.read_csv(csv_path)
    exchange_rates = exch_df.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = [np.round(x * exchange_rates['GBP'], 2) for x in df['MC_USD_Billions']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rates['EUR'], 2) for x in df['MC_USD_Billions']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rates['INR'], 2) for x in df['MC_USD_Billions']]
    return df

df = transform(df, csv_path)
print(df)
log_progress('Data transformation complete. Initiating Loading process')

def load_to_csv(df, output_path):
    df.to_csv(output_path, index = False)

load_to_csv(df, output_path)
log_progress('Data saved to CSV file')

conn = sqlite3.connect(db_name)
def load_to_db(df, conn, table_name):
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

load_to_db(df, conn, table_name)
log_progress('Data loaded to Database as a table, Executing queries')
