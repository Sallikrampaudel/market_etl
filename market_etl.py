import csv
import json
import requests
import s3fs
from io import StringIO
import requests
import boto3

def run_market_etl():

    url = "https://api.marketstack.com/v1/eod"

    querystring = {
        "access_key": "API Key",  # Replace this with your actual API key
        "symbols": "AAPL"
    }

    response = requests.get(url, params=querystring)
    data = response.json()

    data_list = []
    # Extract the symbol and the relevant data
    if 'data' in data:
        for entry in data['data']:
            refined_data = {'symbol' : entry.get('symbol'),
            'date' : entry.get('date'),
            'open_price' : entry.get('open'),
            'high_price' : entry.get('high'),
            'low_price' : entry.get('low'),
            'close_price' : entry.get('close')
            }
            data_list.append(entry)
    else:
        print("No data found in the response")

    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerows(data)
    s3.put_object(Bucket='salik-airflow-bucket', Key='Apple_Stock_Data.csv', Body=csv_buffer.getvalue())

    print("CSV file uploaded successfully to S3.")
    #df = pd.DataFrame(data_list)
    #df.to_csv("s3://salik-airflow-bucket/Apple_Stock_Data.csv")
