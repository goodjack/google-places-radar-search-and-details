import argparse
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
PLACE_DETAILS_LANG = os.environ.get("PLACE_DETAILS_LANG")
PLACE_DETAILS_TABLE = os.environ.get("PLACE_DETAILS_TABLE")
PLACE_PHOTOS_TABLE = os.environ.get("PLACE_PHOTOS_TABLE")
PLACE_PHOTOS_FAILED_TABLE = os.environ.get("PLACE_PHOTOS_FAILED_TABLE")
"""
CREATE TABLE `place_photos` (
  `photo_reference` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `results` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`photo_reference`),
  KEY `created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


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


def select_all(limit, offset):
    connection = get_mysql_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT `place_id`, `language` FROM " + PLACE_DETAILS_TABLE + " WHERE `results` LIKE '%photo_reference%' ORDER BY `created_at`"

            if (limit is not None) and (offset is not None):
                sql += " LIMIT " + str(limit) + ", " + str(offset)
            cursor.execute(sql)
            place_details_keys = cursor.fetchall()
    finally:
        connection.close()

    return place_details_keys


def select_place_details_result(place_id, language):
    print("=== Place Details: ", place_id, language, "===")

    connection = get_mysql_connection()

    try:
        with connection.cursor() as cursor:
            sql = "SELECT `results` FROM " + PLACE_DETAILS_TABLE + " WHERE `place_id`=%s AND `language`=%s"
            cursor.execute(sql, (place_id, language))
            json_results = cursor.fetchone()
    finally:
        connection.close()

    return json_results


def get_photo_reference_list(json_results):
    # TODO: NOT FINISHED YET
    # results_row_data = json.loads(json_results['results'])['results']

    # place_ids = []
    # for place in results_row_data:
    #     place_ids.append(place['place_id'])

    # return place_ids


def insert_place_photos_result(photo_reference, place_photos_result):
    # TODO: NOT FINISHED YET
    # connection = get_mysql_connection()
    # try:
    #     with connection.cursor() as cursor:
    #         # Create a new record
    #         sql = "INSERT INTO `" + PLACE_PHOTOS_TABLE + "` (`photo_reference`, `results`) VALUES (%s, %s)"
    #         cursor.execute(sql, (photo_reference, place_photos_result))
    #     connection.commit()
    # finally:
    #     connection.close()


def insert_place_photos_result_failed(photo_reference, place_photos_result):
    # TODO: NOT FINISHED YET

    # connection = get_mysql_connection()
    # try:
    #     with connection.cursor() as cursor:
    #         # Create a new record
    #         sql = "INSERT INTO `" + PLACE_PHOTOS_FAILED_TABLE + "` (`photo_reference`, `results`) VALUES (%s, %s)"
    #         cursor.execute(sql, (photo_reference, place_photos_result))
    #     connection.commit()
    # finally:
    #     connection.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', help='The limit order by created_at')
    parser.add_argument('-o', help='The offset order by created_at')
    args = parser.parse_args()

    if args.l is not None:
        limit = int(args.l)
    else:
        limit = None

    if args.o is not None:
        offset = int(args.o)
    else:
        offset = None

    place_details_keys = select_all(limit, offset)

    for place_details_key in place_details_keys:
        json_results = select_place_details_result(
            place_details_key['place_id'], place_details_key['language'])

        photo_references = get_photo_reference_list(json_results)
        for photo_reference in photo_references:
            # TODO: NOT FINISHED YET
            # request_place_details(place_id, PLACE_DETAILS_LANG)


if __name__ == "__main__":
    main()
