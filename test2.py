import yaml
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

# Load configuration from YAML file
print("Loading configuration from 'configTesting15022024.yaml'...")
with open('configTesting15022024.yaml', 'r') as file:
    config = yaml.safe_load(file)
print("Configuration loaded successfully.")

# Setup proxies from configuration for requests
proxies = {'http': config['proxies']['http']}
print(f"Proxies set to: {proxies}")

def fetch_conversion_rate(url, selector):
    """Fetch the latest conversion rate from a given URL."""
    print(f"Fetching conversion rate from {url}")
    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, proxies=proxies)
    soup = BeautifulSoup(response.content, 'html.parser')
    rate_cell = soup.select_one(selector)
    if rate_cell:
        rate_text = rate_cell.get_text(strip=True).replace(',', '')
        try:
            return float(rate_text)
        except ValueError as e:
            print(f"Could not convert rate text to float: {rate_text} | Error: {e}")
    else:
        print(f"No element found using the selector {selector} at {url}")
    return None

def fetch_commodity_data(website_config, usd_inr_rate, rmb_inr_rate):
    """Fetch and process commodity data based on website configuration."""
    try:
        print(f"Fetching commodity data for {website_config['product_name']}...")
        response = requests.get(website_config['url'], headers={'User-Agent': 'Mozilla/5.0'}, proxies=proxies)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extracting data from the page
        data_rows = []
        table_rows = soup.select('ul.zwd_table.no_select li')[1:]  # Skip the header row
        for row in table_rows:
            columns = [col.get_text(strip=True) for col in row.find_all('p')]
            if len(columns) >= 2:  # Expecting at least Date and Price
                date, price = columns[0], columns[1]
                data_rows.append([
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # Current timestamp for QueryTimestamp
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # Current timestamp for TableFetchTimestamp
                    price,  # Extracted price
                    website_config['currency'],  # Currency from the configuration
                    'N/A', 'N/A', 'N/A', 'N/A', 'N/A',  # Placeholder for unavailable data
                    website_config['product_name'],  # Product name from the configuration
                    website_config['type'],  # Type from the configuration
                    website_config['WebsiteName'],  # Website name from the configuration
                    usd_inr_rate,  # USD to INR conversion rate
                    rmb_inr_rate,  # RMB to INR conversion rate
                ])

        # Create a DataFrame with the appropriate columns
        df = pd.DataFrame(data_rows, columns=[
            'QueryTimestamp', 'TableFetchTimestamp', 'Price', 'Currency',
            'Open_', 'High', 'Low', 'Volume', 'ChgPerc', 'ProductName',
            'Type', 'WebsiteName', 'USD_INR', 'RMB_INR'
        ])

        # Generate a unique IdentifierKey for each row
        df['IdentifierKey'] = df.apply(
            lambda x: f"{x['TableFetchTimestamp']}_{x['ProductName']}_{x['WebsiteName']}",
            axis=1
        )
        
        print(f"Data fetched for {website_config['product_name']}.")
        return df
    
    except Exception as e:
        print(f"An error occurred while fetching data for {website_config['product_name']}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

def run():
    print("Starting data fetching process...")
    
    # Fetch the conversion rates
    usd_inr_selector = 'table.common-table tr:nth-of-type(1) td:nth-of-type(2)'
    rmb_inr_selector = 'table.common-table tr:nth-of-type(1) td:nth-of-type(2)'
    
    usd_inr_rate = fetch_conversion_rate("https://in.investing.com/currencies/usd-inr-historical-data", usd_inr_selector)
    rmb_inr_rate = fetch_conversion_rate("https://in.investing.com/currencies/cny-inr-historical-data", rmb_inr_selector)
    
    # Check if rates were successfully fetched
    if usd_inr_rate is None or rmb_inr_rate is None:
        print("
