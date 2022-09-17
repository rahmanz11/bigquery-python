import os
import string
from google.cloud import bigquery
import requests

def create_dataset(client) -> string:
    # Set dataset_id to the ID of the dataset to create.
    dataset_id = "{}.mydataset".format(client.project)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # Specify the geographic location where the dataset should reside.
    dataset.location = "US"

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.

    dataset = client.create_dataset(dataset, timeout=30)
    # print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    return dataset_id

def create_table(client, dataset_id) -> string:
    # Set table_id to the ID of the table to create.

    table_id = "{}.mytable".format(dataset_id)
    schema = [
        bigquery.SchemaField("numFound", "INTEGER"),
        bigquery.SchemaField("start", "INTEGER"),
        bigquery.SchemaField("docs", "RECORD", mode="REPEATED", 
            fields=[
                bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("abstract", "STRING", mode="REPEATED")
                ])
    ]

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    # print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
    return table_id


def fetch_data():
    data = None
    # Fetch data
    url = "https://api.plos.org/search?q=title:%22Drosophila%22%20AND%20body:%22RNA%22&fl=id,abstract&wt=json"
    try:
        response = requests.get(url, data=None, headers={"Content-Type": "application/json"})
        if response:
            data = [response.json()['response']]
    except Exception as e:
        print("Exception occurred while fetching data {}".format(str(e))) 
    
    return data        

def insert_data(table_id, client):
    data = fetch_data()
    if data is not None:
        errors = client.insert_rows_json(table_id, data)
        if errors == []:
            print("New records have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))
    else:
        print("No data available")


if __name__=="__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="sanguine-tome-362813-96fc8db73f4f.json"
    client = bigquery.Client()

    # Initially when no dataset and table are avilable, 
    # Create dataset to get the dataset id and 
    # then create table to get the table id

    # Create dataset
    # dataset_id = create_dataset(client)
    # print("Dataset id {}".format(dataset_id))

    dataset_id = 'sanguine-tome-362813.mydataset' # dataset_id got from create_dataset
    # Create table
    table_id = create_table(client, dataset_id)
    # print("Table id {}".format(table_id))

    # table_id = "sanguine-tome-362813.mydataset.mytable" # table_id got from create_table

    # Now insert data
    insert_data(table_id, client)