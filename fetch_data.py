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
