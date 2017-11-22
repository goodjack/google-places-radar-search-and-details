import argparse
import googlemaps
import json
import os
import pymysql.cursors

from datetime import datetime
from dotenv import load_dotenv, find_dotenv
from os.path import join, dirname

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path, override=True)

GOOGLE_PLACES_API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY")

# Client setting
gmaps = googlemaps.Client(key=GOOGLE_PLACES_API_KEY)


def frange(start, stop, step):
    x = start
    while x < stop:
        yield x
        x += step


def get_connection():
    MYSQL_HOST = os.environ.get("MYSQL_HOST")
    MYSQL_USER = os.environ.get("MYSQL_USER")
    MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD")
    MYSQL_DB = os.environ.get("MYSQL_DB")
    MYSQL_CHARSET = os.environ.get("MYSQL_CHARSET")

    connection = pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DB,
        charset=MYSQL_CHARSET,
        cursorclass=pymysql.cursors.DictCursor)

    return connection


def get_place_details(gmaps, place_id):
    return gmaps.place(place_id)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--lat1',
        help='The starting point of latitude measurement',
        default='-6.3425357')
    parser.add_argument(
        '--lng1',
        help='The starting point of longitude measurement',
        default='106.6790033')
    parser.add_argument(
        '--lat2',
        help='The end point of latitude measurement',
        default='-6.0130311')
    parser.add_argument(
        '--lng2',
        help='The end point of longitude measurement',
        default='107.005707')
    parser.add_argument('-r', help='radius', default='125')
    parser.add_argument('-t', help='type', default='restaurant')
    args = parser.parse_args()

    lat_start = float(args.lat1)
    lng_start = float(args.lng1)
    lat_end = float(args.lat2)
    lng_end = float(args.lng2)
    radius = float(args.r)
    place_type = args.t

    for lat in frange(lat_start, lat_end, 0.00025):
        for lng in frange(lng_start, lng_end, 0.00025):
            connection = get_connection()

            # === Radar search === #
            # lat = 25.017156
            # lng = 121.506359
            # radar_searchs(lat, lng)
            location = (lat, lng)
            places_radar_result = gmaps.places_radar(
                location, radius, type=place_type)

            try:
                with connection.cursor() as cursor:
                    # Create a new record
                    sql = "INSERT INTO `radar_searchs` (`location`, `radius`, `type`, `results`, `created_at`, `updated_at`) VALUES (%s, %s, %s, %s, %s, %s)"
                    cursor.execute(sql, (str(location), radius, place_type,
                                        json.dumps(places_radar_result['results']),
                                        datetime.now(), datetime.now()))

                connection.commit()
            finally:
                connection.close()

            # print(places_radar_result['results'][0]['place_id'])
            for value in places_radar_result['results']:
                place_id = value['place_id']
                # print(get_place_details(place_id))
                details = get_place_details(gmaps, place_id)
                #TODO: Save details to database


if __name__ == "__main__":
    main()