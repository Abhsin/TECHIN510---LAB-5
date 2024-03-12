import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import psycopg2
import os
from dotenv import load_dotenv


load_dotenv()

db_user = os.getenv('DB_USER')
db_pw = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')
conn_str = f'postgresql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_name}'

def get_lat_lon(location_name, city='Seattle', state='WA', country='USA'):
    base_url = "https://nominatim.openstreetmap.org/search.php"
    query_params = {
        "q": f"{location_name}, {city}, {state}, {country}",  # More specific query
        "format": "jsonv2"
    }
    response = requests.get(base_url, params=query_params)
    if response.status_code == 200 and len(response.json()) > 0:
        data = response.json()[0]
        lat, lon = float(data.get('lat')), float(data.get('lon'))
        # Validate coordinates (example range for Seattle)
        if 47.4 <= lat <= 47.8 and -122.5 <= lon <= -122.2:
            return lat, lon
        else:
            return None, None  # Coordinates are outside the expected range
    else:
        return None, None  # API request failed or returned no data

def get_weather_forecast(lat, lon, event_date):
    try:
        url_weather = f"https://api.weather.gov/points/{lat},{lon}"
        response = requests.get(url_weather)
        if response.status_code == 200:
            # Extract the forecast URL from the response
            forecast_url = response.json()['properties']['forecast']
            forecast_response = requests.get(forecast_url)
            if forecast_response.status_code == 200:
                forecast_data = forecast_response.json()['properties']['periods']
                for period in forecast_data:
                    forecast_date = datetime.strptime(period['startTime'], '%Y-%m-%dT%H:%M:%S%z').date()
                    if forecast_date == event_date and 'daytime' in period['name'].lower():
                        weather = period['shortForecast']
                        temperature = period['temperature']
                        wind_speed = period['windSpeed']
                        wind_direction = period['windDirection']
                        return weather, temperature, wind_speed, wind_direction
        return None, None, None, None
    except Exception as e:
        print(f"Error fetching weather data for lat: {lat}, lon: {lon}, date: {event_date}. Error: {e}")
        return None, None, None, None

# Function to get the latest weather forecast
def get_latest_weather_forecast(lat, lon):
    try:
        url_weather = f"https://api.weather.gov/points/{lat},{lon}"
        response = requests.get(url_weather)
        if response.status_code == 200:
            forecast_url = response.json()['properties']['forecast']
            forecast_response = requests.get(forecast_url)
            if forecast_response.status_code == 200:
                forecast_data = forecast_response.json()['properties']['periods'][0] # Get the latest forecast
                return forecast_data['shortForecast'], forecast_data['temperature'], forecast_data['windSpeed'], forecast_data['windDirection']
        return 'Not available', 'Not available', 'Not available', 'Not available'
    except Exception as e:
        print(f"Error fetching latest weather data for lat: {lat}, lon: {lon}. Error: {e}")
        return 'Not available', 'Not available', 'Not available', 'Not available'

def get_seattle_weather_forecast():
    seattle_lat = '47.6062'
    seattle_lon = '-122.3321'
    return get_latest_weather_forecast(seattle_lat, seattle_lon)

# Function to establish PostgreSQL connection
def connect_to_postgres():
    try:
        conn = psycopg2.connect(conn_str)
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

# Function to insert data into PostgreSQL
# Function to insert data into PostgreSQL
def insert_data_to_postgres(conn, df):
    try:
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                name TEXT,
                date_time TIMESTAMP,
                location TEXT,
                event_type TEXT,
                region TEXT,
                latitude FLOAT,
                longitude FLOAT,
                weather TEXT,
                temperature FLOAT,
                wind_speed TEXT,
                wind_direction TEXT
            )
        """)
        
        # Insert data into the table
        for index, row in df.iterrows():
            try:
                # Attempt to insert coordinates
                lat, lon = get_lat_lon(row['Location'])
                if lat is None or lon is None:
                    # If coordinates are not found or invalid, set them to None
                    lat, lon = None, None
                    print(f"Coordinates not found or invalid for location: {row['Location']}")
                
                # Attempt to parse date_time
                try:
                    date_time = datetime.strptime(row['Date & Time'], '%m/%d/%Y')
                except ValueError:
                    # If the timestamp format is invalid, set it to None or a default value
                    date_time = None
                    print(f"Invalid timestamp format for location: {row['Location']}")
                
                cursor.execute(
                    "INSERT INTO events (name, date_time, location, event_type, region, latitude, longitude, weather, temperature, wind_speed, wind_direction) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        row['Name'],
                        date_time,
                        row['Location'],
                        row['Type'],
                        row['Region'],
                        lat,
                        lon,
                        row['weather'],
                        row['temperature'],
                        row['wind_speed'],
                        row['wind_direction']
                    )
                )
            except Exception as e:
                print(f"Error inserting data for location: {row['Location']}. Error: {e}")
        
        conn.commit()
        cursor.close()
        print("Data inserted successfully into PostgreSQL")
    except Exception as e:
        print(f"Error inserting data into PostgreSQL: {e}")
        conn.rollback()


# Establish PostgreSQL connection
conn = connect_to_postgres()
if conn is not None:
    print("Successfully connected to PostgreSQL.")
    try:
        # Integrate both codes here
        base_url = "https://visitseattle.org/events/page/"
        num_pages = 40
        events = []
        for page in range(1, num_pages + 1):
            url = base_url + str(page)
            res = requests.get(url)
            soup = BeautifulSoup(res.text, 'html.parser')
            selector = 'div.search-result-preview > div > h3 > a'
            a_eles = soup.select(selector)
            events += [x['href'] for x in a_eles]

        eventdata = []

        for event in events:
            res = requests.get(event)

            if res.status_code == 200:
                soup = BeautifulSoup(res.content, 'html.parser')

                name = soup.select_one('div.medium-6.columns.event-top > h1')
                date_time = soup.select_one('div.medium-6.columns.event-top > h4 > span:nth-child(1)')
                location = soup.select_one('div.medium-6.columns.event-top > h4 > span:nth-child(2)')
                event_type = soup.select_one('div.medium-6.columns.event-top > a:nth-child(3)')
                region = soup.select_one('div.medium-6.columns.event-top > a:nth-child(4)')

                eventdata.append({
                    "Name": name.get_text(strip=True) if name else "Not found",
                    "Date & Time": date_time.get_text(strip=True) if date_time else "Not found",
                    "Location": location.get_text(strip=True) if location else "Not found",
                    "Type": event_type.get_text(strip=True) if event_type else "Not found",
                    "Region": region.get_text(strip=True) if region else "Not found"
                })

        df = pd.DataFrame(eventdata)

        # Adding new columns for latitude and longitude
        df['Latitude'] = None
        df['Longitude'] = None

        for index, row in df.iterrows():
            lat, lon = get_lat_lon(row['Location'])
            if lat is not None and lon is not None:
                df.at[index, 'Latitude'] = lat
                df.at[index, 'Longitude'] = lon
            else:
                # Handle cases where coordinates are not found or invalid
                print(f"Coordinates not found or invalid for location: {row['Location']}")

        # Adding new columns for weather details
        df['weather'] = None
        df['temperature'] = None
        df['wind_speed'] = None
        df['wind_direction'] = None

        for index, row in df.iterrows():
            lat = row.get('Latitude')
            lon = row.get('Longitude')
            date_str = row['Date & Time'].split(' ')[0]

            if pd.notna(lat) and pd.notna(lon):
                try:
                    if date_str.lower() == 'now' or date_str.lower() == 'ongoing':
                        weather_info = get_latest_weather_forecast(lat, lon)
                    else:
                        event_date = datetime.strptime(date_str, '%m/%d/%Y').date()
                        weather_info = get_weather_forecast(lat, lon, event_date)

                    # Check if weather info is not returned
                    if not all(weather_info):
                        weather_info = get_seattle_weather_forecast()  # Default to Seattle weather
                except Exception:
                    weather_info = get_seattle_weather_forecast()  # Default to Seattle weather
            else:
                weather_info = get_seattle_weather_forecast()  # Default to Seattle weather

            # Update the DataFrame with the weather information
            df.at[index, 'weather'], df.at[index, 'temperature'], df.at[index, 'wind_speed'], df.at[index, 'wind_direction'] = weather_info

        # Insert data into PostgreSQL
        insert_data_to_postgres(conn, df)
    finally:
        # Close the PostgreSQL connection
        conn.close()
        print("PostgreSQL connection is closed.")
else:
    print("Failed to connect to PostgreSQL.")
# adding comment to make changes!
    
