from datetime import datetime
import random
import os
from os.path import join, dirname
import sys
from time import sleep

from dotenv import load_dotenv
import pymysql.cursors
import requests

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path, override=True)

GOOGLE_PLACES_API_KEYS = os.environ.get("GOOGLE_PLACES_API_KEYS").split(",")
PLACE_PHOTOS_TABLE = os.environ.get("PLACE_PHOTOS_TABLE")
PLACE_PHOTOS_FILE = os.environ.get("PLACE_PHOTOS_FILE")


def get_random_key():
    return random.choice(GOOGLE_PLACES_API_KEYS)


def request_place_photos(photo_reference):
    place_photos_result = get_place_photos_result(photo_reference)
    insert_place_photos_result(photo_reference, place_photos_result)


def get_place_photos_result(photo_reference):
    place_photos_result = None

    for attempt in range(10):
        sleep(0.05)
        key = get_random_key()

        print("Get place details result:", photo_reference, datetime.now())

        try:
            payload = {
                'key': key,
                'photoreference': photo_reference,
                'maxwidth': '800'
            }
            place_photos_result = requests.get(
                "https://maps.googleapis.com/maps/api/place/photo",
                params=payload).url
        except:
            print("Unexpected error:", sys.exc_info()[0])
            place_photos_result = "Unexpected error:" + str(sys.exc_info())
            print("Sleep a second...")
            sleep(1)
        else:
            print("Got it.")
            break

    return place_photos_result


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


def insert_place_photos_result(photo_reference, place_photos_result):
    connection = get_mysql_connection()
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO `" + PLACE_PHOTOS_TABLE + "` (`photo_reference`, `results`, `created_at`, `updated_at`) VALUES (%s, %s, %s, %s)"
            cursor.execute(sql, (photo_reference, place_photos_result,
                                 datetime.now(), datetime.now()))
        connection.commit()
    finally:
        connection.close()


def main():
    with open(PLACE_PHOTOS_FILE) as input_file:
        for line in input_file:
            photo_reference = line.strip()
            request_place_photos(photo_reference)


if __name__ == "__main__":
    main()
