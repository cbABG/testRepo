import yaml
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime as dt
import numpy as np
import pyodbc
import traceback

# Load configuration from YAML file
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

environment = "development" 
#environment = "production"

# Select the appropriate configuration
db_config = config['database'][environment]

# Use selected configuration for database connection
conn = pyodbc.connect(
    driver=db_config['driver'],
    server=db_config['server'],
    database=db_config['database'],
    uid=db_config['uid'],
    pwd=db_config['pwd']
)
cursor = conn.cursor()

proxies = {'http': config['proxies']['http']}

# URLs list is now fetched from the config file
urls = config['urls']


def find_year(date):
    year=dt.today().year
    month=int(date.split('/')[0])
    day=int(date.split('/')[1])
    if month==12:
        if dt.today().day!=day:
            year=year-1
            return dt(day=day,month=month,year=year)
        else:
            return dt(day=day,month=month,year=year)
    else:
        return dt(day=day,month=month,year=year)

def fetch_commodity_data(url):
    try:
        response = requests.request("GET", url, proxies=proxies)
        response.raise_for_status() 
        html = response.content
        
    except requests.exceptions.ProxyError as e:
        print(f"Proxy error occurred: {e}")
        
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred: {e}")
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    
    # creating soup object 
    data = BeautifulSoup(html, 'html.parser') 
    try:
        parent = data.find("ul", class_='zwd_table no_select').findAll("li") 
        data = []
        for ele in parent:
            all_p = ele.findAll("p")
            tup = []
            for idx, p in enumerate(all_p):
                tup.append(p.get_text())
            data.append(tup)

        df = pd.DataFrame(data=data[1:], columns=data[0])
        df['Date'] = df['Date'].apply(find_year)
        df['Price'] = df['Price'].astype(float)
        df = df[['Date', 'Commodity', 'Price']]
        print(df)
        return df
    except Exception as exc:
        print(traceback.format_exc())
        print("Not able to fetch the price")
        return pd.DataFrame(columns=['Date', 'Commodity', 'Price'])

def upload_data(df:pd.DataFrame, table:str, delete_date:dt.date=None, date_col:str=None, where:dict=None):     
    if not df.empty:
        print(f"Uploading Data for {table}")
        for col in df.columns:
            df[col]=df[col].replace(np.inf, np.nan)
            if df[col].isnull().sum() > 0:
                df[col] = np.where(df[col].isnull(), None, df[col])
        
        params = list(tuple(row) for row in df.values)
        # Dynamically construct the SQL statement to include the schema
        sql = f"INSERT INTO {db_config['schema']}.{table} VALUES ({','.join(['?']*len(df.columns))})"
        if len(params) != 0:
            cursor.executemany(sql, params)
            cursor.commit()
            print("Data Uploaded..........")
        else:
            print("No Data to Upload")
    else:
        print("Nothing To Upload")

def delete_data(table,date,date_col,where=None):
        print(f"deleting data from table {table} for date {date}")
        sql=f"delete from {table} where {date_col}='{date}'"
        if where:
            where_string=""
            for key in where.keys():
                where_string+=key
                where_string+="="
                where_string+=f"'{where[key]}'"
                where_string+=' '
            # print(where_string)
            sql=" and ".join([sql,where_string])
        # print(sql)
        cursor.execute(sql)
        cursor.commit()

def delete_old_data(df):
    for date in df['Date'].unique():
        delete_data('nsr_prod_real_time_price',date=date,date_col='Date')

def run():
    combined_df = pd.DataFrame()
    for url in urls:
        df = fetch_commodity_data(url)
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    
    if not combined_df.empty:
        delete_old_data(combined_df)
        upload_data(combined_df, table=db_config['table_name'])

if __name__ == "__main__":
    run()
