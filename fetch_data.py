import requests
import pandas as pd
import json
from datetime import datetime
from config import NASA_API_KEY

def fetch_planets():
    url = "https://ssd-api.jpl.nasa.gov/api/bodies.api"
    params = {
        "api_key": NASA_API_KEY,
        "body-type": "planet",
        "order": "body"
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data['data'], columns=data['fields'])
    else:
        print(f"Failed to fetch planets. Status code: {response.status_code}")
        return None

def fetch_moons():
    url = "https://ssd-api.jpl.nasa.gov/api/satellites.api"
    params = {
        "api_key": NASA_API_KEY,
        "order": "planet"
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data['data'], columns=data['fields'])
    else:
        print(f"Failed to fetch moons. Status code: {response.status_code}")
        return None

def save_to_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data.to_dict(orient='records'), f, indent=2)

def main():
    planets_df = fetch_planets()
    moons_df = fetch_moons()

    if planets_df is not None:
        save_to_json(planets_df, 'raw_planets.json')
        print("Planets data saved to raw_planets.json")
    
    if moons_df is not None:
        save_to_json(moons_df, 'raw_moons.json')
        print("Moons data saved to raw_moons.json")

if __name__ == "__main__":
    main()