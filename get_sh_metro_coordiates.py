import requests
import sqlite3

# Define the API endpoint URL
url = "https://map.amap.com/service/subway?_1568752521231&srhdata=3100_drw_shanghai.json"

# Send a GET request to the API endpoint
response = requests.get(url)

# Parse the JSON response
data = response.json()

# Extract the station names and coordinates
stations = []
for line in data['l']:
    for station in line['st']:
        name = station['n']
        lng, lat = station['sl'].split(',')
        lines = []
        for ln in data['l']:
            if name in [s['n'] for s in ln['st']]:
                lines.append(ln['ln'])
        stations.append({'name': name, 'lng': float(lng), 'lat': float(lat), 'lines': list(set(lines))})

# Connect to the database

conn = sqlite3.connect('./db/house_rent_lianjia.db')

# Create a table to store the station information
conn.execute('''CREATE TABLE IF NOT EXISTS sh_metro_stations
             (id INTEGER PRIMARY KEY,
             name TEXT NOT NULL,
             lng REAL NOT NULL,
             lat REAL NOT NULL,
             lines TEXT NOT NULL);''')

# Insert each station into the database
for station in stations:
    name = station['name']
    lng = station['lng']
    lat = station['lat']
    lines = ', '.join(station['lines'])
    conn.execute("INSERT INTO sh_metro_stations (name, lng, lat, lines) VALUES (?, ?, ?, ?)", (name, lng, lat, lines))

# Commit the changes and close the database connection
conn.commit()
conn.close()
