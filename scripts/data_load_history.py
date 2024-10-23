import os
from dotenv import load_dotenv
import json
import pprint as pp
from google.cloud import bigquery
import looker_sdk
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

# BigQuery setup
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../bq-service-account.json'
client = bigquery.Client()
project_id = os.environ['project_id']
dataset_id = "bigquery_velo2"
table_id = "looker_history"
full_table_id = f"{project_id}.{dataset_id}.{table_id}"

# Looker setup
# os.environ['LOOKERSDK_BASE_URL'] = 'LOOKERSDK_BASE_URL'
# os.environ['LOOKERSDK_CLIENT_ID'] = 'LOOKERSDK_CLIENT_ID'
# os.environ['LOOKERSDK_CLIENT_SECRET'] = 'LOOKERSDK_CLIENT_SECRET'
sdk = looker_sdk.init40()

## Fetch data from Looker API
looker_fields = [
    "history.created_time",
    "history.id",
    "history.source",
    "history.issuer_source",
    "history.slug",
    "history.status",
    "history.result_format",
    "history.render_key",
    "history.real_dash_id",
    "history.message",
    "query.view",
    "query.dynamic_fields",
    "query.formatted_fields",
    # "query.fields_used",
    "query.fill_fields",
    "query.filters",
    "query.formatted_pivots",
    "query.hash",
    "query.id",
    "query.link",
    "query.model",
    "user.email",
    "user.id",
    "user.name",
    "dashboard.description",
    "dashboard.id",
    "dashboard.link",
    "dashboard.lookml_link_id",
    "dashboard.title"
]
# for daily load
date_range = "yesterday"
# for history data
date_range = "before yesterday"

def fetch_looker_data(date_range="yesterday", looker_fields=looker_fields):
    body = {
        "model": "system__activity",
        "view": "history",
        "fields": looker_fields,
        "filters": {"history.created_time": date_range},
        "query_timezone": "UTC",
        "limit": "1"
    }

    try:
        results = sdk.run_inline_query(result_format='json', body=body)
        return json.loads(results)
    except:
        raise Exception(f"Failed to fetch Looker data.")

# Insert data into BigQuery
def insert_into_bigquery(date_range="yesterday", looker_fields=looker_fields):
    looker_data = fetch_looker_data(date_range)

    prepared_data = []

    for record in looker_data:
        prepared_record = {}
        for field in looker_fields:
            # Handle missing or invalid fields by inserting None (null in BigQuery)
            bq_field = field.replace('.', '__')
            prepared_record[bq_field] = record.get(field, None)
            print(field)
        
        prepared_data.append(prepared_record)

    # Insert data into BigQuery
    pp.pprint(prepared_data)
    errors = client.insert_rows_json(full_table_id, prepared_data)
    
    if errors:
        print(f"Encountered errors: {errors}")
        return f"Encountered errors: {errors}", 500
    
    return "Data inserted successfully.", 200

# Run the insertion
insert_into_bigquery(date_range="before yesterday")
