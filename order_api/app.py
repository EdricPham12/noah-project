from flask import Flask, request, jsonify
import pymysql
import pika
import json

app = Flask(__name__)

# ===== MYSQL CONFIG =====
MYSQL_HOST = "127.0.0.1"
MYSQL_USER = "root"
MYSQL_PASSWORD = "0905632186aA"
MYSQL_DATABASE = "init"


def get_mysql_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        cursorclass=pymysql.cursors.DictCursor
    )


def get_rabbitmq_channel():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
    )
    channel = connection.channel()
    channel.queue_declare(queue="order_queue", durable=True)
    return connection, channel


@app.route("/api/orders", methods=["POST"])
def create_order():
    data = request.get_json()

    user_id = data.get("user_id")
    product_id = data.get("product_id")
    quantity = data.get("quantity")

    if not user_id or not product_id or not quantity:
        return jsonify({"error": "Missing required fields"}), 400

    if quantity <= 0:
        return jsonify({"error": "Quantity must be greater than 0"}), 400

    conn = get_mysql_connection()
    cursor = conn.cursor()

    # lấy giá sản phẩm
    cursor.execute(
        "SELECT price FROM products WHERE id = %s",
        (product_id,)
    )
    product = cursor.fetchone()

    if product is None:
        cursor.close()
        conn.close()
        return jsonify({"error": "Product not found"}), 404

    total_price = product["price"] * quantity

    # insert đơn hàng vào MySQL
    cursor.execute(
        """
        INSERT INTO orders (user_id, product_id, quantity, total_price, status)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (user_id, product_id, quantity, total_price, "PENDING")
    )

    conn.commit()
    order_id = cursor.lastrowid

    message = {
        "order_id": order_id,
        "user_id": user_id,
        "product_id": product_id,
        "quantity": quantity,
        "total_price": float(total_price)
    }

    # gửi message vào RabbitMQ
    rabbit_conn, channel = get_rabbitmq_channel()
    channel.basic_publish(
        exchange="",
        routing_key="order_queue",
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    rabbit_conn.close()

    cursor.close()
    conn.close()

    return jsonify({
        "message": "Order received",
        "order_id": order_id,
        "status": "PENDING"
    }), 202


if __name__ == "__main__":
    app.run(debug=True, port=5000)