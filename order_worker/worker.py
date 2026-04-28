import json
import os
import time

import pika
import psycopg2
import pymysql

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "0905632186aA")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "init")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "noah_finance")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "123456")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")


def retry(operation, attempts=30, delay=2):
    last_error = None
    for _ in range(attempts):
        try:
            return operation()
        except Exception as exc:
            last_error = exc
            time.sleep(delay)
    raise last_error


def get_mysql_connection():
    return retry(
        lambda: pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            cursorclass=pymysql.cursors.DictCursor,
        )
    )


def get_postgres_connection():
    return retry(
        lambda: psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DATABASE,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
    )


def process_order(ch, method, properties, body):
    print("Received message:", body)

    data = json.loads(body)

    order_id = data["order_id"]
    user_id = data["user_id"]
    product_id = data["product_id"]
    quantity = data["quantity"]
    total_price = data["total_price"]

    time.sleep(2)

    pg_conn = get_postgres_connection()
    pg_cursor = pg_conn.cursor()

    pg_cursor.execute(
        """
        INSERT INTO public.transactions
        (order_id, user_id, product_id, quantity, total_price, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (order_id, user_id, product_id, quantity, total_price, "PAID"),
    )

    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()

    my_conn = get_mysql_connection()
    my_cursor = my_conn.cursor()

    my_cursor.execute(
        "UPDATE orders SET status = %s WHERE id = %s",
        ("COMPLETED", order_id),
    )

    my_conn.commit()
    my_cursor.close()
    my_conn.close()

    print(f"Order {order_id} processed successfully")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = retry(
        lambda: pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST)
        )
    )
    channel = connection.channel()

    channel.queue_declare(queue="order_queue", durable=True)
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(
        queue="order_queue",
        on_message_callback=process_order,
    )

    print("Worker is waiting for messages...")
    channel.start_consuming()


if __name__ == "__main__":
    main()
