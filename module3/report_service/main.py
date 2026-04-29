from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
from sqlalchemy import text

from database import mysql_engine, postgres_engine

app = FastAPI(
    title="NOAH Retail Report Service",
    description="Module 3 - Data Stitching từ MySQL và PostgreSQL",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def clean_dataframe_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Xử lý NaN, Infinity và datetime để FastAPI trả JSON không bị lỗi 500.
    """
    if df.empty:
        return df

    df = df.copy()

    df = df.replace([np.inf, -np.inf], 0)
    df = df.fillna(0)

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)

    return df


def ensure_postgres_tables():
    sql = """
    CREATE TABLE IF NOT EXISTS finance_transactions (
        id SERIAL PRIMARY KEY,
        order_id INT NOT NULL,
        user_id INT NOT NULL,
        amount NUMERIC(12,2) NOT NULL,
        payment_status VARCHAR(50) DEFAULT 'PAID',
        paid_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    with postgres_engine.begin() as conn:
        conn.execute(text(sql))


@app.on_event("startup")
def startup():
    ensure_postgres_tables()
    print("[INFO] Report Service started")


@app.get("/")
def root():
    return {
        "service": "NOAH Retail Report Service",
        "module": "Module 3",
        "endpoints": [
            "/api/report",
            "/api/summary",
            "/api/revenue-by-user",
            "/api/top-products"
        ]
    }


@app.get("/api/report")
def get_report(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=500)
):
    offset = (page - 1) * limit

    orders_query = """
        SELECT 
            o.id AS order_id,
            o.user_id,
            o.product_id,
            p.name AS product_name,
            o.quantity,
            o.total_price,
            o.status,
            o.created_at
        FROM orders o
        LEFT JOIN products p ON o.product_id = p.id
        ORDER BY o.id DESC
        LIMIT %(limit)s OFFSET %(offset)s
    """

    payments_query = """
        SELECT 
            order_id,
            user_id AS payment_user_id,
            amount,
            payment_status,
            paid_at
        FROM finance_transactions
        ORDER BY order_id DESC
        LIMIT %(limit)s OFFSET %(offset)s
    """

    count_query = "SELECT COUNT(*) AS total_rows FROM orders"

    orders_df = pd.read_sql(
        orders_query,
        mysql_engine,
        params={"limit": limit, "offset": offset}
    )

    payments_df = pd.read_sql(
        payments_query,
        postgres_engine,
        params={"limit": limit, "offset": offset}
    )

    total_rows_df = pd.read_sql(count_query, mysql_engine)
    total_rows = int(total_rows_df.iloc[0]["total_rows"])

    if orders_df.empty:
        return {
            "page": page,
            "limit": limit,
            "total_rows": total_rows,
            "total_revenue": 0,
            "synced_orders": 0,
            "pending_orders": 0,
            "revenue_by_user": [],
            "orders": []
        }

    # Data Stitching: ghép MySQL orders với PostgreSQL payments theo order_id
    merged_df = pd.merge(
        orders_df,
        payments_df,
        on="order_id",
        how="left"
    )

    merged_df["amount"] = merged_df["amount"].fillna(0)
    merged_df["payment_status"] = merged_df["payment_status"].fillna("UNPAID")

    total_revenue = float(merged_df["amount"].sum())

    synced_orders = int(
        merged_df[merged_df["payment_status"] == "PAID"]["order_id"].count()
    )

    pending_orders = int(
        merged_df[merged_df["payment_status"] != "PAID"]["order_id"].count()
    )

    revenue_by_user_df = (
        merged_df
        .groupby("user_id")
        .agg(
            total_orders=("order_id", "count"),
            total_quantity=("quantity", "sum"),
            total_revenue=("amount", "sum")
        )
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )

    merged_df = clean_dataframe_for_json(merged_df)
    revenue_by_user_df = clean_dataframe_for_json(revenue_by_user_df)

    return {
        "page": page,
        "limit": limit,
        "total_rows": total_rows,
        "total_revenue": total_revenue,
        "synced_orders": synced_orders,
        "pending_orders": pending_orders,
        "revenue_by_user": revenue_by_user_df.to_dict(orient="records"),
        "orders": merged_df.to_dict(orient="records")
    }


@app.get("/api/summary")
def get_summary():
    mysql_summary_query = """
        SELECT
            COUNT(*) AS total_orders,
            COALESCE(SUM(total_price), 0) AS mysql_order_value,
            SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) AS pending_orders,
            SUM(CASE WHEN status IN ('COMPLETED', 'SYNCED') THEN 1 ELSE 0 END) AS completed_orders
        FROM orders
    """

    pg_summary_query = """
        SELECT
            COUNT(*) AS total_payments,
            COALESCE(SUM(amount), 0) AS postgres_revenue
        FROM finance_transactions
    """

    mysql_df = pd.read_sql(mysql_summary_query, mysql_engine)
    pg_df = pd.read_sql(pg_summary_query, postgres_engine)

    return {
        "total_orders": int(mysql_df.iloc[0]["total_orders"] or 0),
        "mysql_order_value": float(mysql_df.iloc[0]["mysql_order_value"] or 0),
        "pending_orders": int(mysql_df.iloc[0]["pending_orders"] or 0),
        "completed_orders": int(mysql_df.iloc[0]["completed_orders"] or 0),
        "total_payments": int(pg_df.iloc[0]["total_payments"] or 0),
        "postgres_revenue": float(pg_df.iloc[0]["postgres_revenue"] or 0)
    }


@app.get("/api/revenue-by-user")
def revenue_by_user(limit: int = Query(10, ge=1, le=100)):
    query = """
        SELECT
            user_id,
            COUNT(order_id) AS total_paid_orders,
            COALESCE(SUM(amount), 0) AS total_revenue
        FROM finance_transactions
        WHERE payment_status = 'PAID'
        GROUP BY user_id
        ORDER BY total_revenue DESC
        LIMIT %(limit)s
    """

    df = pd.read_sql(query, postgres_engine, params={"limit": limit})
    df = clean_dataframe_for_json(df)
    return df.to_dict(orient="records")


@app.get("/api/top-products")
def top_products(limit: int = Query(10, ge=1, le=100)):
    query = """
        SELECT
            p.id AS product_id,
            p.name AS product_name,
            COALESCE(SUM(o.quantity), 0) AS total_sold,
            COALESCE(SUM(o.total_price), 0) AS total_value
        FROM orders o
        JOIN products p ON o.product_id = p.id
        GROUP BY p.id, p.name
        ORDER BY total_value DESC
        LIMIT %(limit)s
    """

    df = pd.read_sql(query, mysql_engine, params={"limit": limit})
    df = clean_dataframe_for_json(df)
    return df.to_dict(orient="records")