import googlemaps
import json
import pymysql.cursors
import random
import os
import sys

from datetime import datetime
from dotenv import load_dotenv, find_dotenv
from os.path import join, dirname
from time import sleep

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path, override=True)

GOOGLE_PLACES_API_KEYS = os.environ.get("GOOGLE_PLACES_API_KEYS").split(",")
PLACE_DETAILS_LANG = os.environ.get("PLACE_DETAILS_LANG")
PLACE_DETAILS_TABLE = os.environ.get("PLACE_DETAILS_TABLE")
PLACE_DETAILS_FAILED_TABLE = os.environ.get("PLACE_DETAILS_FAILED_TABLE")


def get_gmaps():
    gmaps = googlemaps.Client(key=random.choice(GOOGLE_PLACES_API_KEYS))
    return gmaps


def request_place_details(place_id, language):
    place_details_result = get_place_details_result(place_id, language)

    if type(place_details_result) is dict:
        # If the query fails
        if place_details_result['status'] != 'OK' and place_details_result['status'] != 'ZERO_RESULTS':
            # retry
            place_details_result = get_place_details_result(place_id, language)

        # If it still fails
        if place_details_result['status'] != 'OK' and place_details_result['status'] != 'ZERO_RESULTS':
            insert_place_details_result_failed(place_id, language,
                                               place_details_result)
        else:
            insert_place_details_result(place_id, language,
                                        place_details_result)
    else:
        print("Failed.")
        insert_place_details_result_failed(place_id, language,
                                           place_details_result)


def get_place_details_result(place_id, language):
    place_details_result = None

    for attempt in range(5):
        sleep(0.05)
        gmaps = get_gmaps()

        print("Get place details result:", place_id, datetime.now())

        try:
            # raise googlemaps.exceptions.TransportError()
            place_details_result = gmaps.place(place_id, language=language)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            place_details_result = "Unexpected error:" + str(sys.exc_info()[0])
            print("Sleep 10 seconds...")
            sleep(10)
        else:
            print("Got it.")
            break

    return place_details_result


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


def insert_place_details_result(place_id, language, place_details_result):
    connection = get_mysql_connection()
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO `" + PLACE_DETAILS_TABLE + "` (`place_id`, `language`, `results`, `created_at`, `updated_at`) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(
                sql, (place_id, language, json.dumps(place_details_result),
                      datetime.now(), datetime.now()))
        connection.commit()
    finally:
        connection.close()


def insert_place_details_result_failed(place_id, language,
                                       place_details_result):
    connection = get_mysql_connection()
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO `" + PLACE_DETAILS_FAILED_TABLE + "` (`place_id`, `language`, `results`, `created_at`, `updated_at`) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(
                sql, (place_id, language, json.dumps(place_details_result),
                      datetime.now(), datetime.now()))
        connection.commit()
    finally:
        connection.close()


def main():
    with open('place_id_sample_data.txt') as input_file:
        for line in input_file:
            place_id = line.strip()
            request_place_details(place_id, PLACE_DETAILS_LANG)


if __name__ == "__main__":
    main()
