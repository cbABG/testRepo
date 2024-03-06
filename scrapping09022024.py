import os
import yaml
import pyodbc

import requests
import pandas as pd
from bs4 import BeautifulSoup 
from datetime import datetime as dt
import numpy as np
import pyodbc
import traceback

# Load configuration from YAML file
with open('config.yaml', 'r') as file:
    all_config = yaml.safe_load(file)
    
# Determine environment 
if os.getenv('MY_APP_ENV') == 'production':
    environment = 'production'
else:
    environment = 'development'    

db_config = all_config['database'][environment]

# Database connection using selected configuration
conn = pyodbc.connect(
    driver=db_config['driver'],
    server=db_config['server'],
    database=db_config['database'],
    uid=db_config['uid'],
    pwd=db_config['pwd']
)

cursor = conn.cursor()
urls = all_config['urls']

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