import argparse
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
RADAR_SEARCHS_TABLE = os.environ.get("RADAR_SEARCHS_TABLE")
PLACE_DETAILS_LANG = os.environ.get("PLACE_DETAILS_LANG")
PLACE_DETAILS_TABLE = os.environ.get("PLACE_DETAILS_TABLE")
PLACE_DETAILS_FAILED_TABLE = os.environ.get("PLACE_DETAILS_FAILED_TABLE")
"""
CREATE TABLE `place_details` (
  `place_id` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `language` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `results` json DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`place_id`,`language`),
  KEY `created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


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

    for attempt in range(10):
        sleep(0.05)
        gmaps = get_gmaps()

        print("Get place details result:", place_id, datetime.now())

        try:
            # raise googlemaps.exceptions.TransportError()
            place_details_result = gmaps.place(place_id, language=language)
        except:
            print("Unexpected error:", sys.exc_info())
            place_details_result = "Unexpected error:" + str(sys.exc_info())
            print("Sleep a second...")
            sleep(1)
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


def select_all(id_start, id_end):
    connection = get_mysql_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT `id` FROM " + RADAR_SEARCHS_TABLE + " WHERE `results` NOT LIKE '%ZERO_RESULTS%'"

            if id_start > 0:
                sql += " AND `id` >= " + str(id_start)

            if id_end > 0:
                sql += " AND `id` <= " + str(id_end)

            cursor.execute(sql)
            radar_searchs_ids = cursor.fetchall()
    finally:
        connection.close()

    return radar_searchs_ids


def select_radar_searchs_result(id):
    print("=== Radar Search ID:", id, "===")

    connection = get_mysql_connection()

    try:
        with connection.cursor() as cursor:
            sql = "SELECT `results` FROM " + RADAR_SEARCHS_TABLE + " WHERE `id`=%s"
            cursor.execute(sql, (id, ))
            json_results = cursor.fetchone()
    finally:
        connection.close()

    return json_results


def get_place_id_list(json_results):
    results_row_data = json.loads(json_results['results'])['results']

    place_ids = []
    for place in results_row_data:
        place_ids.append(place['place_id'])

    return place_ids


def insert_place_details_result(place_id, language, place_details_result):
    connection = get_mysql_connection()
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO `" + PLACE_DETAILS_TABLE + "` (`place_id`, `language`, `results`) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE `results` = %s;"
            cursor.execute(
                sql, (place_id, language, json.dumps(place_details_result),
                      json.dumps(place_details_result)))
        connection.commit()
    finally:
        connection.close()


def insert_place_details_result_failed(place_id, language,
                                       place_details_result):
    connection = get_mysql_connection()
    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO `" + PLACE_DETAILS_FAILED_TABLE + "` (`place_id`, `language`, `results`) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE `results` = %s;"
            cursor.execute(
                sql, (place_id, language, json.dumps(place_details_result),
                      json.dumps(place_details_result)))
        connection.commit()
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-s', help='The starting ID of radar_searchs', default='0')
    parser.add_argument('-e', help='The end ID of radar_searchs', default='0')
    args = parser.parse_args()

    id_start = float(args.s)
    id_end = float(args.e)

    radar_searchs_ids = select_all(id_start, id_end)

    for radar_searchs_id in radar_searchs_ids:
        json_results = select_radar_searchs_result(radar_searchs_id['id'])

        place_ids = get_place_id_list(json_results)
        for place_id in place_ids:
            request_place_details(place_id, PLACE_DETAILS_LANG)


if __name__ == "__main__":
    main()
