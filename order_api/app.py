from flask import Flask, request, jsonify
import json
import os
import time

import pika
import pymysql

app = Flask(__name__)

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "0905632186aA")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "init")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
PORT = int(os.getenv("PORT", "5000"))


def retry(operation, attempts=20, delay=2):
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


def get_rabbitmq_channel():
    connection = retry(
        lambda: pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST)
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue="order_queue", durable=True)
    return connection, channel


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "order-api"})


@app.route("/orders", methods=["GET"])
def list_orders():
    conn = get_mysql_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT id, user_id, product_id, quantity, total_price, status, created_at
        FROM orders
        ORDER BY id DESC
        LIMIT 20
        """
    )
    orders = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify({"orders": orders})


@app.route("/orders", methods=["POST"])
@app.route("/api/orders", methods=["POST"])
def create_order():
    data = request.get_json(silent=True) or {}

    user_id = data.get("user_id")
    product_id = data.get("product_id")
    quantity = data.get("quantity")

    if user_id is None or product_id is None or quantity is None:
        return jsonify({"error": "Missing required fields"}), 400

    if quantity <= 0:
        return jsonify({"error": "Quantity must be greater than 0"}), 400

    conn = get_mysql_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT price FROM products WHERE id = %s",
        (product_id,),
    )
    product = cursor.fetchone()

    if product is None:
        cursor.close()
        conn.close()
        return jsonify({"error": "Product not found"}), 404

    total_price = product["price"] * quantity

    cursor.execute(
        """
        INSERT INTO orders (user_id, product_id, quantity, total_price, status)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (user_id, product_id, quantity, total_price, "PENDING"),
    )

    conn.commit()
    order_id = cursor.lastrowid

    message = {
        "order_id": order_id,
        "user_id": user_id,
        "product_id": product_id,
        "quantity": quantity,
        "total_price": float(total_price),
    }

    rabbit_conn, channel = get_rabbitmq_channel()
    channel.basic_publish(
        exchange="",
        routing_key="order_queue",
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    rabbit_conn.close()

    cursor.close()
    conn.close()

    return jsonify(
        {
            "message": "Order received",
            "order_id": order_id,
            "status": "PENDING",
        }
    ), 202


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
