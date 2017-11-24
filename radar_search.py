import argparse
import googlemaps
import json
import os
import pymysql.cursors
import random

from datetime import datetime
from dotenv import load_dotenv, find_dotenv
from os.path import join, dirname
from time import sleep

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path, override=True)

GOOGLE_PLACES_API_KEYS = os.environ.get("GOOGLE_PLACES_API_KEYS").split(",")
PLACE_TYPES = os.environ.get("PLACE_TYPES").split(",")

query_times = 0


def frange(start, stop, step):
    x = start
    while x < stop:
        yield x
        x += step


def check_query_times():
    global query_times
    query_times += 1

    if query_times >= 40:
        print("Sleep a second.\n")
        sleep(1)
        query_times = 0


def get_gmaps():
    gmaps = googlemaps.Client(key=random.choice(GOOGLE_PLACES_API_KEYS))
    return gmaps


def radar_search(lat, lng, radius):
    location = (lat, lng)
    for place_type in PLACE_TYPES:
        places_radar_result = get_radar_result(location, radius, place_type)

        # If the query fails
        if places_radar_result['status'] != 'OK' and places_radar_result['status'] != 'ZERO_RESULTS':
            # retry
            places_radar_result = get_radar_result(location, radius,
                                                   place_type)

        # If it still fails
        if places_radar_result['status'] != 'OK' and places_radar_result['status'] != 'ZERO_RESULTS':
            insert_radar_result_failed(location, radius, place_type,
                                       places_radar_result)
        else:
            insert_radar_result(location, radius, place_type, places_radar_result)


def get_radar_result(location, radius, place_type):
    check_query_times()
    gmaps = get_gmaps()
    print("Get Radar Result: " + str(location) + " " + str(radius) + " " +
          str(place_type) + " " + str(datetime.now()))
    places_radar_result = gmaps.places_radar(location, radius, type=place_type)
    print("Got it.")

    return places_radar_result


def get_mysql_connection():
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


def insert_radar_result(location, radius, place_type, places_radar_result):
    connection = get_mysql_connection()
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO `radar_searchs` (`location`, `radius`, `type`, `results`, `created_at`, `updated_at`) VALUES (%s, %s, %s, %s, %s, %s)"
            cursor.execute(sql,
                           (str(location), radius, place_type,
                            json.dumps(places_radar_result), datetime.now(),
                            datetime.now()))
        connection.commit()
    finally:
        connection.close()


def insert_radar_result_failed(location, radius, place_type,
                               places_radar_result):
    connection = get_mysql_connection()
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO `radar_searchs_failed` (`location`, `radius`, `type`, `results`, `created_at`, `updated_at`) VALUES (%s, %s, %s, %s, %s, %s)"
            cursor.execute(sql,
                           (str(location), radius, place_type,
                            json.dumps(places_radar_result), datetime.now(),
                            datetime.now()))
        connection.commit()
    finally:
        connection.close()


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
    parser.add_argument(
        '--lastlat',
        help='Last stop point of latitude measurement',
        default=None)
    parser.add_argument(
        '--lastlng',
        help='Last stop point of longitude measurement',
        default=None)
    parser.add_argument('-r', help='radius', default='125')
    parser.add_argument('-t', help='type', default='restaurant')
    args = parser.parse_args()

    lat_start = float(args.lat1)
    lng_start = float(args.lng1)
    lat_end = float(args.lat2)
    lng_end = float(args.lng2)
    radius = int(args.r)
    step = radius * 0.00002
    place_type = args.t

    if (args.lastlat is not None) and (args.lastlng is not None):
        lat_last = float(args.lastlat)
        lng_last = float(args.lastlng)

        for lng in frange(lng_last, lng_end, step):
            radar_search(lat_last, lng, radius)

        lat_start = lat_last + step

    for lat in frange(lat_start, lat_end, step):
        for lng in frange(lng_start, lng_end, step):
            radar_search(lat, lng, radius)


if __name__ == "__main__":
    main()