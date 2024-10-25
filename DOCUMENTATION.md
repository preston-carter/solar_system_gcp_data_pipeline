# Solar System Data Pipeline Documentation

## Overview
This document outlines the end-to-end data pipeline that extracts, loads, and transforms data from the Solar System OpenData API into BigQuery for analytics and reporting purposes. The pipeline provides detailed information about celestial bodies in our solar system, making it accessible for business intelligence and scientific analysis.

## Pipeline Architecture
```mermaid
graph LR
    A [Solar System API] -->|Python Script| B[Raw Layer]
    B -->|Dataform| C[Transformed Layer]
    C -->|Dataform| D[Data Quality]
    D -->|BI Tool| E[Dashboards]
```

## Authentication & Security
- Uses Service Account JSON key authentication
- Required GitHub Secrets:
  - `GCP_PROJECT_ID`: Project identifier
  - `GCP_SERVICE_ACCOUNT`: Service account JSON key

## Project Structure
```
solar-system-prod (BigQuery)
??? Staging
?   ??? RawPlanet
?   ??? RawMoon
??? SolarSystemBody
?   ??? Planet
?   ??? Moon
??? DataQuality
    ??? PlanetValidation
    ??? MoonValidation

definitions (Dataform)
??? Staging
?   ??? raw_planet.sqlx
?   ??? raw_moon.sqlx
??? SolarSystemBody
?   ??? Planet.sqlx
?   ??? Moon.sqlx
??? DataQuality
    ??? PlanetValidation.sqlx
    ??? MoonValidation.sqlx
```

## Data Sources
- **Source**: Le Système Solaire OpenData API
- **Endpoint**: https://api.le-systeme-solaire.net/rest/bodies/
- **Update Frequency**: Daily
- **Data Format**: JSON

## Extract & Load Process

### Data Extraction (fetch_data.py)
```python
import requests
import json

# Define the base API URL
url = "https://api.le-systeme-solaire.net/rest/bodies/"

# Make a request to fetch all bodies
try:
    response = requests.get(url)
    
    if response.status_code == 200:
        all_bodies = response.json()['bodies']
        
        # Separate lists for planets and moons
        planets = []
        moons = []
        
        # Loop through each body and categorize as planet or moon
        for body in all_bodies:
            if body.get('isPlanet'):
                planets.append(body)
            elif body.get('aroundPlanet') is not None:
                moons.append(body)
        
        # Save planets data to JSON file
        with open('raw_planets.json', 'w') as planets_file:
            json.dump(planets, planets_file, indent=4)
        
        # Save moons data to JSON file
        with open('raw_moons.json', 'w') as moons_file:
            json.dump(moons, moons_file, indent=4)

        print("Data extraction complete. Check 'raw_planets.json' and 'raw_moons.json' files.")

    else:
        print(f"Failed to fetch data. Status code: {response.status_code}, {response.text}")

except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")
```

### BigQuery Load Process (load_data.py)
```python
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
```
## Data Model

### Raw Layer
**Dataset**: `solar-system-prod.Staging`

**Table**: `RawPlanet`
- Contains raw planet data from API
- Schema auto-detected from JSON
- Refreshed daily via Python ETL

**Table**: `RawMoon`
- Contains raw moon data from API
- Schema auto-detected from JSON
- Refreshed daily via Python ETL

### Transformed Layer
**Dataset**: `solar-system-prod.SolarSystemBody`

#### Planet Table Transformation (Planet.sqlx)
```sql
config {
  type: "table",
  tags: ["SolarSystemBody"],
  schema: "SolarSystemBody", 
  description: "This table stores descriptive dimensional data about planets in the solar system",
  uniqueKey: ["PlanetId"],
  assertions: {
    uniqueKey: true,
    nonNull: ["PlanetId", "PlanetName"]
  }
}

select
  -- Use perihelion value for each planet in window func to generate a unique PlanetId
  row_number() over (order by perihelion) as PlanetId
  , englishName as PlanetName
  , gravity as Gravity
  , mass.MassValue as Mass
  , vol.volValue as Volume
  , density as Density
  , avgTemp as AverageSurfaceTemperature
  , axialTilt as AxialTilt
  , eccentricity as Eccentricity
  , perihelion as Perihelion 
  , aphelion as Aphelion
from ${ref("RawPlanet")}
```

#### Moon Table Transformation (Moon.sqlx)
```sql
config {
  type: "table",
  tags: ["SolarSystemBody"],
  schema: "SolarSystemBody", 
  description: "This table stores descriptive dimensional data about planetary satellites (moons) in the solar system",
  uniqueKey: ["MoonId"],
  assertions: {
    uniqueKey: true,
    nonNull: ["MoonId", "MoonName", "PlanetId"]
  }
}

with PreparePlanetId as 
(
  select
    englishName as MoonName
    , aroundPlanet.planet as OrbitingBody
    -- Assign PlanetId for moons that orbit solar system planets
    -- The data come are sourced from a French API, so spellings are slightly different
    , case
        when aroundPlanet.Planet = 'terre'
          then 3
        when aroundPlanet.Planet = 'mars'
          then 4
        when aroundPlanet.Planet = 'jupiter'
          then 5
        when aroundPlanet.Planet = 'saturne'
          then 6
        when aroundPlanet.Planet = 'uranus'
          then 7
        when aroundPlanet.Planet = 'neptune'
          then 8
      else null 
    end as PlanetId
    , gravity as Gravity
    , mass.MassValue as Mass
    , vol.volValue as Volume
    , density as Density
    , avgTemp as AverageSurfaceTemperature
    , axialTilt as AxialTilt
    , eccentricity as Eccentricity
    , perihelion as Perihelion 
    , aphelion as Aphelion
  from ${ref("RawMoon")}
)

, PrepareMoonId as 
(
  select
    concat(coalesce(PlanetId,9),row_number() over (partition by PlanetId order by MoonName)) as MoonId
    , MoonName
    , OrbitingBody
    , PlanetId
    , Gravity
    , Mass
    , Volume
    , Density
    , AverageSurfaceTemperature
    , AxialTilt
    , Eccentricity
    , Perihelion 
    , Aphelion
  from PreparePlanetId
)

select
  case
    when length(MoonId) = 2
      then cast(concat(substr(MoonId,1,1), '00', substr(MoonId,2)) as int64)
    when length(MoonId) = 3
      then cast(concat(substr(MoonId,1,1), '0', substr(MoonId,2)) as int64)
    else cast(MoonId as int64)
  end as MoonId
  , MoonName
  , initcap(OrbitingBody) as OrbitingBody
  , PlanetId
  , Gravity
  , Mass
  , Volume
  , Density
  , AverageSurfaceTemperature
  , AxialTilt
  , Eccentricity
  , Perihelion 
  , Aphelion
from PrepareMoonId
```

### Data Quality Layer
**Dataset**: `solar-system-prod.DataQuality`

#### Planet Validation (planet_assertions.sqlx)
```sql
config {
  type: "assertion",
  tags: ["DataQuality"],
  schema: "DataQuality",
  description: "Data quality checks for Planet table"
}

with PlanetValidation as
(
  select 
    PlanetId
    , PlanetName
    , Gravity
    , Mass
    , Volume
    , Density
    , AverageSurfaceTemperature
    , AxialTilt
    , Eccentricity
    , Perihelion 
    , Aphelion
    , case 
      when Mass <= 0 then 'Invalid Mass'
      when Volume <= 0 then 'Invalid Volume'
      when Density <= 0 then 'Invalid Density'
      when Gravity <= 0 then 'Invalid Gravity'
      when AverageSurfaceTemperature < 0 then 'Invalid Temperature (below absolute zero)'
      when Perihelion >= Aphelion then 'Invalid Orbit (perihelion >= aphelion)'
      when Eccentricity < 0 or Eccentricity >= 1 then 'Invalid Eccentricity'
      when PlanetName is null then 'Missing PlanetName'
    end as ValidationError
  from ${ref("Planet")}
)

select 
  PlanetId
  , PlanetName
  , Gravity
  , Mass
  , Volume
  , Density
  , AverageSurfaceTemperature
  , AxialTilt
  , Eccentricity
  , Perihelion 
  , Aphelion
  , ValidationError
from PlanetValidation
where ValidationError is not null
```

#### Moon Validation (moon_assertions.sqlx)
```sql
config {
  type: "assertion",
  tags: ["DataQuality"],
  schema: "DataQuality",
  description: "Data quality checks for Moon table"
}

with MoonValidation as
(
  select 
    MoonId
    , MoonName
    , OrbitingBody
    , Gravity
    , Mass
    , Volume
    , Density
    , AverageSurfaceTemperature
    , AxialTilt
    , Eccentricity
    , Perihelion 
    , Aphelion
    , case 
      when Mass <= 0 then 'Invalid Mass'
      when Volume <= 0 then 'Invalid Volume'
      when Density <= 0 then 'Invalid Density'
      when Gravity <= 0 then 'Invalid Gravity'
      when AverageSurfaceTemperature < 0 then 'Invalid Temperature (below absolute zero)'
      when Perihelion >= Aphelion then 'Invalid Orbit (perihelion >= aphelion)'
      when Eccentricity < 0 or Eccentricity >= 1 then 'Invalid Eccentricity'
      when OrbitingBody is null then 'Missing orbital body'
      when MoonName is null then 'Missing MoonName'
    end as ValidationError
  from ${ref("Moon")}
)

select 
  MoonId
  , MoonName
  , OrbitingBody
  , Gravity
  , Mass
  , Volume
  , Density
  , AverageSurfaceTemperature
  , AxialTilt
  , Eccentricity
  , Perihelion 
  , Aphelion
  , ValidationError
from MoonValidation
where ValidationError is not null
```

## Scheduled Execution
### GitHub Actions Workflow
```yaml
name: Solar System Data Pipeline
on:
  workflow_dispatch:  # Allows manual triggering
  schedule:
    # Runs at 8am Central Time (13:00 UTC)
    - cron: '0 13 * * *'
jobs:
  extract-and-load:
    runs-on: ubuntu-latest
    permissions:
      contents: 'read'
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.9'

      - name: Authenticate using service account key
        uses: google-github-actions/auth@v2
        with:
          credentials_json: ${{ secrets.GCP_SERVICE_ACCOUNT }}

      - name: Set up Cloud SDK
        uses: google-github-actions/setup-gcloud@v2
        with:
          project_id: ${{ secrets.GCP_PROJECT_ID }}
          export_default_credentials: true

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install requests google-cloud-bigquery

      - name: Extract data from API
        id: extract
        run: |
          python fetch_data.py
          if [ ! -f raw_planets.json ] || [ ! -f raw_moons.json ]; then
            echo "Error: Data extraction failed - JSON files not created"
            exit 1
          fi

      - name: Load data to BigQuery
        id: load
        if: steps.extract.outcome == 'success'
        run: |
          python load_data.py

      - name: Clean up temporary files
        if: always()
        run: |
          rm -f raw_planets.json raw_moons.json
```

## Future Improvements
1. Create metrics view for reporting
2. Add incremental processing capability
3. Add monitoring and alerting
4. Create detailed moon characteristics analysis
