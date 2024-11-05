import os
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta
from google.cloud import bigquery
import looker_sdk
import urllib3

# Disable insecure request warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load environment variables
load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../bq-service-account.json'

# Initialize clients
client = bigquery.Client()
sdk = looker_sdk.init40()

# Constants
project_id = os.environ['project_id']
dataset_id = "bigquery_velo2"
table_id = "looker_history"
full_table_id = f"{project_id}.{dataset_id}.{table_id}"

looker_fields = [
    "history.created_time", "history.id", "history.source", "history.issuer_source",
    "history.slug", "history.status", "history.result_format", "history.render_key",
    "history.real_dash_id", "history.message", "query.view", "query.dynamic_fields",
    "query.formatted_fields", "query.fill_fields", "query.filters", "query.formatted_pivots",
    "query.hash", "query.id", "query.link", "query.model", "user.email", "user.id",
    "user.name", "dashboard.description", "dashboard.id", "dashboard.link",
    "dashboard.lookml_link_id", "dashboard.title"
]

def fetch_looker_data(date_range="yesterday"):
    """Fetch data from Looker API based on the specified date range."""
    body = {
        "model": "system__activity",
        "view": "history",
        "fields": looker_fields,
        "filters": {"history.created_time": date_range},
        "query_timezone": "UTC",
        "limit": "1000"
    }
    try:
        results = sdk.run_inline_query(result_format='json', body=body)
        return json.loads(results)
    except Exception as e:
        print(f"Failed to fetch Looker data: {e}")
        return []

def prepare_record_for_bigquery(record):
    """Prepare a single record for insertion into BigQuery, matching the required schema."""
    prepared_record = {}
    for field in looker_fields:
        # Convert field names to match BigQuery schema
        bq_field = field.replace(".", "__")
        value = record.get(field)
        
        # Handle type conversions
        if bq_field == 'history__real_dash_id' and value is not None:
            try:
                prepared_record[bq_field] = int(value)
            except (ValueError, TypeError):
                prepared_record[bq_field] = None
        else:
            prepared_record[bq_field] = value if value is not None else None
    print(prepared_record)
    return prepared_record

def insert_into_bigquery(looker_data):
    """Insert data into BigQuery with error handling."""
    prepared_data = [prepare_record_for_bigquery(record) for record in looker_data]
    if prepared_data:
        errors = client.insert_rows_json(full_table_id, prepared_data)
        if errors:
            print(f"Encountered errors while inserting data: {errors}")
        else:
            print("Data inserted successfully.")
    else:
        print("No data to insert.")

def run_load(start_date, end_date):
    """Run the data load process for each day in the specified date range."""
    try:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD.")
        return

    for single_date in (start_date + timedelta(days=n) for n in range((end_date - start_date).days + 1)):
        day_str = single_date.strftime("%Y-%m-%d")
        print(f"Processing data for {day_str}")
        
        looker_data = fetch_looker_data(date_range=day_str)
        if looker_data:
            insert_into_bigquery(looker_data)
        else:
            print(f"No data fetched for {day_str}.")

# Run the load
start_date = "2023-01-01"
end_date = "2024-11-04"
run_load(start_date, end_date)
