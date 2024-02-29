from datetime import datetime
import os
import sys
import requests
from bs4 import BeautifulSoup
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def get_wikipedia_page(url):
    print("Getting wikipedia page...", url)
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # check if the request is successful
        return response.text
    except requests.RequestException as e:
        print(f"An error occurred: {e}")


def get_wikipedia_data(html):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find_all("table", {"class": "wikitable sortable"})[0]
    table_rows = table.find_all('tr')
    return table_rows


def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if ' ♦' in text:  # corrected if condition
        text = text.split(' ♦')[0]
    if '[' in text:  # corrected if condition
        text = text.split('[')[0]
    if ' (formerly)' in text:  # corrected if condition
        text = text.split(' (formerly)')[0]
    return text.replace('\n', '')


def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)
    data = []

    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text).replace(',', '').replace('.', ''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images': 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE",
            'home_team': clean_text(tds[6].text),
        }
        data.append(values)

    data_df = pd.DataFrame(data)
    output_file_path = "/home/airflow/footballproject/output.csv"
    data_df.to_csv(output_file_path, index=False)
    azure_storage_upload(output_file_path)
    return data


def azure_storage_upload(file_path):
    # Create BlobServiceClient using the connection string
    connection_string = "BlobEndpoint=https://amanacc.blob.core.windows.net/;QueueEndpoint=https://amanacc.queue.core.windows.net/;FileEndpoint=https://amanacc.file.core.windows.net/;TableEndpoint=https://amanacc.table.core.windows.net/;SharedAccessSignature=sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-03-01T14:50:37Z&st=2024-02-29T06:50:37Z&spr=https,http&sig=%2FODr2sbRC67NRLBsc08GlKsoC927YzXLWHmMndWTX80%3D"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get a container client
    container_name = "all-data"
    container_client = blob_service_client.get_container_client(container_name)

    # Get a blob client
    blob_name = "output.csv"
    blob_client = container_client.get_blob_client(blob_name)

    # Upload file to blob
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)


# Define the DAG
dag = DAG(
    dag_id='flow_azure',
    default_args={
        "owner": "Aman Mapari",
        "start_date": datetime(2024, 2, 27),
    },
    schedule_interval=None,
    catchup=False
)

# Define the task to extract data from Wikipedia and upload to Azure Blob Storage
extract_data_from_wikipedia = PythonOperator(
    task_id="extract_data_from_wikipedia",
    python_callable=extract_wikipedia_data,
    provide_context=True,
    op_kwargs={"url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
    dag=dag
)

# Set task dependencies
extract_data_from_wikipedia
