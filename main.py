from prefect import flow, task
import clickhouse_connect
from datetime import datetime
from pymongo import MongoClient


@task
def extract():
    # Extraction
    client_clickhouse = clickhouse_connect.get_client(host='localhost', username='default', password='')
    result = client_clickhouse.query_df('SELECT * FROM nectarvet_dbt.dst_treatments')
    treatments_raw = result.to_dict('records')
    return treatments_raw


@task
def transform(data):
    # Trasnformation
    for treatment in data:
        #print(treatment)
        treatment["created_at"] = datetime.strptime(treatment["created_at"], '%m/%d/%Y %H:%M:%S')
        treatment["updated_at"] = datetime.strptime(treatment["updated_at"], '%m/%d/%Y %H:%M:%S')
        treatment["meta"] = {}
        treatment["satisfy_reminder"] = []
        treatment["generate_reminders"] = []
    return data


@task
def load(data):
    # Load 
    client_mongodb = MongoClient('mongodb://localhost:27019/') 
    db = client_mongodb['clinic1']
    treatments_collection = db['treatments']
    treatments_collection.insert_many(data)


@flow(name="nectar_treatments_reverse_etl")
def flow_treatments():
    e = extract()
    t = transform(e)
    l = load(t)

if __name__ == "main":
    flow_treatments()