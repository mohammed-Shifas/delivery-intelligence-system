import requests
import pandas as pd
import time

def get_restaurants(city_name, lat, lon, radius_km=5):
    
    print(f"Fetching restaurants in {city_name}...")
    
    overpass_url = "https://overpass-api.de/api/interpreter"
    
    query = f"""
    [out:json][timeout:60];
    (
      node["amenity"="restaurant"](around:{radius_km * 1000},{lat},{lon});
      node["amenity"="fast_food"](around:{radius_km * 1000},{lat},{lon});
      node["amenity"="cafe"](around:{radius_km * 1000},{lat},{lon});
    );
    out body;
    """
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "User-Agent": "DeliveryIntelligenceProject/1.0"
    }
    
    data = None
    
    for attempt in range(3):
        print(f"  Attempt {attempt + 1} of 3...")
        time.sleep(5)
        
        response = requests.post(
            overpass_url,
            data=f"data={requests.utils.quote(query)}",
            headers=headers,
            timeout=60
        )
        
        print(f"  Status code: {response.status_code}")
        print(f"  Response preview: {response.text[:200]}")
        
        if response.status_code == 200 and len(response.text) > 10:
            data = response.json()
            print(f"  Success!")
            break
        else:
            print(f"  Waiting 15 seconds...")
            time.sleep(15)
    
    if data is None:
        print("Failed to get data after 3 attempts")
        return []
    
    restaurants = []
    for element in data['elements']:
        tags = element.get('tags', {})
        restaurant = {
            'restaurant_id': str(element['id']),
            'name': tags.get('name', 'Unknown'),
            'cuisine': tags.get('cuisine', 'Unknown'),
            'latitude': element.get('lat'),
            'longitude': element.get('lon'),
            'address': tags.get('addr:street', 'Unknown'),
            'opening_hours': tags.get('opening_hours', 'Unknown'),
            'delivery': tags.get('delivery', 'Unknown'),
            'takeaway': tags.get('takeaway', 'Unknown'),
        }
        restaurants.append(restaurant)
    
    print(f"Found {len(restaurants)} restaurants in {city_name}")
    return restaurants

restaurants = get_restaurants(
    city_name="London City Centre",
    lat=51.5074,
    lon=-0.1278,
    radius_km=3
)

if restaurants:
    df = pd.DataFrame(restaurants)
    df.to_csv('data/restaurants_raw.csv', index=False)
    print(f"\nSaved {len(df)} restaurants to data/restaurants_raw.csv")
    print(df.head())
else:
    print("No data to save")