import os
import csv
import time
import shutil
import logging
from datetime import datetime

import mysql.connector
from mysql.connector import Error


INPUT_DIR = "/app/input"
PROCESSED_DIR = "/app/processed"
FILE_NAME = "inventory.csv"

MYSQL_HOST = "mysql"
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_DATABASE = "noah_webstore"

POLL_INTERVAL = 10

# Rule OUTLIERS của nhóm 
MIN_VALID_QUANTITY = 0
MAX_VALID_QUANTITY = 1000


logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s - %(message)s"
)


def retry_connection(max_retries=20, delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            conn = mysql.connector.connect(
                host=MYSQL_HOST,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE
            )

            if conn.is_connected():
                logging.info("Connected to MySQL.")
                return conn

        except Error as e:
            logging.warning(f"Retry {attempt}: MySQL not ready ({e})")
            time.sleep(delay)

    raise Exception("Cannot connect to MySQL")


def validate_row(row):
    try:
        product_id = int(row["product_id"])
        quantity = int(row["quantity"])
    except:
        return None, None, False, "invalid format"

    if quantity < MIN_VALID_QUANTITY:
        return product_id, quantity, False, "negative"

    if quantity > MAX_VALID_QUANTITY:
        return product_id, quantity, False, "outlier"

    return product_id, quantity, True, "valid"


def process_file(file_path):
    conn = retry_connection()
    cursor = conn.cursor()

    processed = 0
    skipped = 0

    with open(file_path, "r") as file:
        reader = csv.DictReader(file)

        for row in reader:
            product_id, quantity, valid, reason = validate_row(row)

            if not valid:
                skipped += 1
                logging.warning(f"Skipped row {row} ({reason})")
                continue

            try:
                cursor.execute(
                    "UPDATE products SET stock=%s WHERE id=%s",
                    (quantity, product_id)
                )
                processed += 1

            except Exception as e:
                skipped += 1
                logging.warning(f"DB error {row}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    logging.info(f"Processed: {processed}, Skipped: {skipped}")


def move_file(file_path):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_name = f"inventory_{timestamp}.csv"
    dest = os.path.join(PROCESSED_DIR, new_name)

    shutil.move(file_path, dest)
    logging.info(f"Moved to {dest}")


def main():
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    logging.info("Legacy Adapter started...")

    while True:
        file_path = os.path.join(INPUT_DIR, FILE_NAME)

        if os.path.exists(file_path):
            logging.info("Found inventory.csv")
            process_file(file_path)
            move_file(file_path)
        else:
            logging.info("Waiting for file...")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()