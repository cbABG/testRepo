import requests
import pandas as pd
from bs4 import BeautifulSoup 
from datetime import datetime as dt
import numpy as np
import pyodbc
import traceback

# Configuration for environments
configurations = {
    'production': {
        'driver': '{ODBC Driver 17 for SQL Server}',
        'server': 'grcd-az-mdg-pp-sql-01.database.windows.net',
        'database': 'DBO',
        'uid': 'azuremdg.bihsqldb',
        'pwd': '@Zu7$3Qmi$y',
        'table_name': 'nsr_prod_real_time_price'
    },
    'development': {
        'driver': '{ODBC Driver 17 for SQL Server}',
        'server': 'grcd-az-mdg-pp-sql-01.database.windows.net',
        'database': 'DB_development',
        'uid': 'pelsqldb',
        'pwd': '$pEl*@driYOd',
        'table_name': 'nsr_prod_real_time_price'
    }
}

#environment = input("Enter environment (production/development): ").lower()
environment = "development"

# Select configuration based on the environment
config = configurations.get(environment, configurations['development'])

# Use selected configuration for database connection
conn = pyodbc.connect(
    driver=config['driver'],
    server=config['server'],
    database=config['database'],
    uid=config['uid'],
    pwd=config['pwd']
)
cursor = conn.cursor()

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
    payload = {}
    headers = {}
    http_proxy  = "185.46.212.91:80"
    proxies = {'http': http_proxy}
    response = requests.request("GET", url, proxies=proxies)
    html = response.content
    
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

def upload_data(df:pd.DataFrame,table:str,delete_date:dt.date=None,date_col:str=None,where:dict=None):     
        if not df.empty:
            print(f"Uploading Data for {table}")
            for col in df.columns:
                df[col]=df[col].replace(np.inf,np.nan)
                if df[col].isnull().sum() > 0:
                    df[col] = np.where(df[col].isnull(), None, df[col])
                    
            params=list(tuple(row) for row in df.values)
            # print(params)
            sql=f"insert into db_development.{table} values ({','.join(['?']*len(df.columns))})"
            # print(sql)
            if len(params)!=0:
                cursor.executemany(sql,params)
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
    urls = [
        "http://www.sunsirs.com/m/page/commodity-price-detail/commodity-price-detail-817.html",
        "http://www.sunsirs.com/m/page/commodity-price-detail/commodity-price-detail-236.html",
        "http://www.sunsirs.com/m/page/commodity-price-detail/commodity-price-detail-368.html",
        "http://www.sunsirs.com/m/page/commodity-price-detail/commodity-price-detail-218.html",
        "http://www.sunsirs.com/m/page/commodity-price-detail/commodity-price-detail-226.html",
        "http://www.sunsirs.com/m/page/commodity-price-detail/commodity-price-detail-355.html",
        "http://www.sunsirs.com/m/page/commodity-price-detail/commodity-price-detail-541.html",
        "http://www.sunsirs.com/m/page/commodity-price-detail/commodity-price-detail-708.html"
    ]
    
    combined_df = pd.DataFrame()
    for url in urls:
        df = fetch_commodity_data(url)
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    
    if not combined_df.empty:
        delete_old_data(combined_df)
        upload_data(combined_df, table=config['table_name']) 

if __name__ == "__main__":
    run()