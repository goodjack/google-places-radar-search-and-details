import googlemaps
import os

from os.path import join, dirname
from dotenv import load_dotenv, find_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

GOOGLE_PLACES_API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY")

# Client setting
gmaps = googlemaps.Client(key=GOOGLE_PLACES_API_KEY)

# Radar search
location = (25.017156, 121.506359)
radius = 500
place_type = 'restaurant'
places_radar_result = gmaps.places_radar(location, radius, type=place_type)

# print(places_radar_result['results'][0]['place_id'])


def get_place_details(place_id):
    return gmaps.place(place_id)

for value in places_radar_result['results']:
    place_id = value['place_id']
    # print(get_place_details(place_id))
