import pika
import json
import time
import pymysql
import psycopg2

# ===== MYSQL CONFIG =====
MYSQL_HOST = "127.0.0.1"
MYSQL_USER = "root"
MYSQL_PASSWORD = "0905632186aA"
MYSQL_DATABASE = "init"
 
# ===== POSTGRES CONFIG =====
POSTGRES_HOST = "127.0.0.1"
POSTGRES_DATABASE = "noah_finance"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "123456"


def get_mysql_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        cursorclass=pymysql.cursors.DictCursor
    )


def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DATABASE,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
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

    # Insert into PostgreSQL
    pg_conn = get_postgres_connection()
    pg_cursor = pg_conn.cursor()

    pg_cursor.execute(
        """
        INSERT INTO public.transactions
        (order_id, user_id, product_id, quantity, total_price, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (order_id, user_id, product_id, quantity, total_price, "PAID")
    )

    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()

    # Update MySQL order status
    my_conn = get_mysql_connection()
    my_cursor = my_conn.cursor()

    my_cursor.execute(
        "UPDATE orders SET status = %s WHERE id = %s",
        ("COMPLETED", order_id)
    )

    my_conn.commit()
    my_cursor.close()
    my_conn.close()

    print(f"Order {order_id} processed successfully")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
    )
    channel = connection.channel()

    channel.queue_declare(queue="order_queue", durable=True)
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(
        queue="order_queue",
        on_message_callback=process_order
    )

    print("Worker is waiting for messages...")
    channel.start_consuming()


if __name__ == "__main__":
    main()