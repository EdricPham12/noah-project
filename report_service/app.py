import os
import time

import psycopg2
from flask import Flask, jsonify

app = Flask(__name__)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "noah_finance")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "123456")
PORT = int(os.getenv("PORT", "5001"))


def retry(operation, attempts=20, delay=2):
    last_error = None
    for _ in range(attempts):
        try:
            return operation()
        except Exception as exc:
            last_error = exc
            time.sleep(delay)
    raise last_error


def get_postgres_connection():
    return retry(
        lambda: psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DATABASE,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
    )


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "report-service"})


@app.route("/report", methods=["GET"])
def report():
    conn = get_postgres_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            COUNT(*) AS total_transactions,
            COALESCE(SUM(total_price), 0) AS total_revenue
        FROM public.transactions
        """
    )
    total_transactions, total_revenue = cursor.fetchone()

    cursor.close()
    conn.close()

    return jsonify(
        {
            "total_transactions": total_transactions,
            "total_revenue": float(total_revenue),
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
