import yaml
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime


# Load configuration from YAML file
with open('configTesting15022024.yaml', 'r') as file:
    config = yaml.safe_load(file)

proxies = {'http': config['proxies']['http']}

def find_year(date_str):
    """Parse date string and adjust the year."""
    today = datetime.today()
    month, day = map(int, date_str.split('/'))
    year = today.year if month <= today.month else today.year - 1
    return datetime(year=year, month=month, day=day).strftime('%Y-%m-%d')

def fetch_usd_inr_conversion_rate():
    url = "https://in.investing.com/currencies/usd-inr-historical-data"
    response = requests.get(url, proxies=proxies)
    soup = BeautifulSoup(response.content, 'html.parser')
    rows = soup.select('table.common-table.medium.js-table tr')
    if rows:
        latest_row = rows[1]  # Assuming the first row after the header has the latest rates
        rate = latest_row.select_one('td:nth-child(2)').text  # Assuming the second column has the rate
        return float(rate.replace(',', ''))
    return None


def fetch_rmb_inr_conversion_rate():
    url = "https://in.investing.com/currencies/cny-inr-historical-data"
    response = requests.get(url, proxies=proxies)
    soup = BeautifulSoup(response.content, 'html.parser')
    rows = soup.select('table.common-table.medium.js-table tr')
    if rows:
        latest_row = rows[1]
        rate = latest_row.select_one('td:nth-child(2)').text
        return float(rate.replace(',', ''))
    return None

def fetch_commodity_data(website_config, usd_inr_rate, rmb_inr_rate):
    """Fetch and process commodity data based on configuration file."""
    try:
        response = requests.get(website_config['url'], headers={'User-Agent': 'Mozilla/5.0'}, proxies=proxies)
        soup = BeautifulSoup(response.content, 'html.parser')

        data_rows = []
        table_rows = soup.select('ul.zwd_table.no_select li')[1:]  # Skip the first row if it contains headers
        for row in table_rows:
            columns = [col.get_text(strip=True) for col in row.find_all('p')]
            if len(columns) >= 2:
                date, price = columns[0], columns[1]
                data_rows.append([
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # QueryTimestamp
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # TableFetchTimestamp
                    price,  # Price from the page
                    website_config['currency'],  # Currency from config
                    'N/A', 'N/A', 'N/A', 'N/A', 'N/A',  # Other values as N/A or placeholders
                    website_config['product_name'],  # ProductName from config
                    website_config['type'],  # Type from config
                    website_config['WebsiteName'],  # WebsiteName from config
                    usd_inr_rate,  # USD_INR rate from fetched data
                    rmb_inr_rate,  # RMB_INR rate from fetched data
                ])

        # Construct DataFrame
        df = pd.DataFrame(data_rows, columns=[
            'QueryTimestamp', 'TableFetchTimestamp', 'Price', 'Currency', 'Open_', 'High', 'Low',
            'Volume', 'ChgPerc', 'ProductName', 'Type', 'WebsiteName', 'USD_INR', 'RMB_INR'
        ])

        # Generate IdentifierKey
        df['IdentifierKey'] = df.apply(
            lambda x: f"{x['TableFetchTimestamp']}_{x['ProductName']}_{x['WebsiteName']}", axis=1
        )

        return df
    
    except Exception as e:
        print(f"An error occurred while fetching data for {website_config['product_name']}: {e}")
        return pd.DataFrame(columns=[
            'QueryTimestamp', 'TableFetchTimestamp', 'Price', 'Currency', 'Open_', 'High', 'Low',
            'Volume', 'ChgPerc', 'ProductName', 'Type', 'WebsiteName', 'USD_INR', 'RMB_INR', 'IdentifierKey'
        ])


def run():
    usd_inr_rate = fetch_usd_inr_conversion_rate()
    rmb_inr_rate = fetch_rmb_inr_conversion_rate()
    combined_data = []
    for website in config['websites']:
        df = fetch_commodity_data(website,usd_inr_rate, rmb_inr_rate)
        if not df.empty:
            combined_data.append(df)

    if combined_data:
        combined_df = pd.concat(combined_data, ignore_index=True)
        combined_csv_filename = "combined_commodities_data.csv"
        combined_df.to_csv(combined_csv_filename, index=False)
        print(f"Combined data saved to {combined_csv_filename}.")
    else:
        print("No data fetched.")
        
        
if __name__ == "__main__":
    run()        