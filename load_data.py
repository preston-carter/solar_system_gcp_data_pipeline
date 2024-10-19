from google.cloud import bigquery
import os
import json

# Initialize BigQuery client
client = bigquery.Client()

# Define your BigQuery dataset and table names
dataset_id = 'solar-system-prod.Staging'
planets_table_id = f'{dataset_id}.RawPlanet'
moons_table_id = f'{dataset_id}.RawMoon'

# Load planets data
with open('raw_planets.json', 'r') as file:
    planets_data = json.load(file)

# Load moons data
with open('raw_moons.json', 'r') as file:
    moons_data = json.load(file)

# Define job config with automatic schema detection
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    autodetect=True
)

# Load planets data into BigQuery
planets_job = client.load_table_from_json(planets_data, planets_table_id, job_config=job_config)
planets_job.result()  # Wait for the job to complete

# Load moons data into BigQuery
moons_job = client.load_table_from_json(moons_data, moons_table_id, job_config=job_config)
moons_job.result()  # Wait for the job to complete

print(f"Loaded data into {planets_table_id} and {moons_table_id}.")
