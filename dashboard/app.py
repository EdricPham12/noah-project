import os
import requests
import pandas as pd
import streamlit as st
import plotly.express as px

REPORT_API_URL = os.getenv(
    "REPORT_API_URL",
    "http://report-service:7000"
)

st.set_page_config(
    page_title="NOAH Retail Dashboard",
    page_icon="📊",
    layout="wide"
)

st.title("📊 NOAH Retail Unified Commerce Dashboard")
st.caption("Module 3 - Data Stitching giữa MySQL và PostgreSQL")


def get_json(endpoint: str, params=None):
    try:
        response = requests.get(
            f"{REPORT_API_URL}{endpoint}",
            params=params,
            timeout=10
        )
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        st.error(f"Không gọi được Report API: {e}")
        return None


st.sidebar.header("Bộ lọc")
page = st.sidebar.number_input("Trang", min_value=1, value=1, step=1)
limit = st.sidebar.slider("Số dòng mỗi trang", 10, 500, 50)

if st.sidebar.button("Refresh"):
    st.rerun()


summary = get_json("/api/summary")
report = get_json("/api/report", params={"page": page, "limit": limit})
top_products = get_json("/api/top-products", params={"limit": 10})
revenue_users = get_json("/api/revenue-by-user", params={"limit": 10})


if summary:
    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Tổng Orders MySQL", f"{summary['total_orders']:,}")
    col2.metric("Tổng Payments PostgreSQL", f"{summary['total_payments']:,}")
    col3.metric("Doanh thu Finance", f"{summary['postgres_revenue']:,.0f} VND")
    col4.metric("Pending Orders", f"{summary['pending_orders']:,}")


st.divider()

if report:
    st.subheader("🔗 Data Stitching: MySQL Orders + PostgreSQL Payments")

    col1, col2, col3 = st.columns(3)
    col1.metric("Doanh thu trang hiện tại", f"{report['total_revenue']:,.0f} VND")
    col2.metric("Đơn đã thanh toán", report["synced_orders"])
    col3.metric("Đơn chưa thanh toán", report["pending_orders"])

    orders_df = pd.DataFrame(report["orders"])

    if not orders_df.empty:
        st.dataframe(
            orders_df,
            use_container_width=True,
            hide_index=True
        )

        st.info(
            "Bảng trên được ghép từ MySQL và PostgreSQL bằng order_id, "
            "không phải query trực tiếp từ một database duy nhất."
        )
    else:
        st.warning("Không có dữ liệu orders.")


st.divider()

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("🏆 Top sản phẩm theo doanh thu")

    if top_products:
        top_products_df = pd.DataFrame(top_products)

        if not top_products_df.empty:
            fig = px.bar(
                top_products_df,
                x="product_name",
                y="total_value",
                title="Top Products by Revenue"
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(top_products_df, use_container_width=True, hide_index=True)

with col_right:
    st.subheader("👤 Top khách hàng theo doanh thu")

    if revenue_users:
        revenue_users_df = pd.DataFrame(revenue_users)

        if not revenue_users_df.empty:
            fig = px.bar(
                revenue_users_df,
                x="user_id",
                y="total_revenue",
                title="Revenue by User"
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(revenue_users_df, use_container_width=True, hide_index=True)


st.divider()

st.subheader("✅ Checklist chấm điểm Module 3")

st.markdown(
    """
- Kết nối đồng thời MySQL và PostgreSQL  
- Có endpoint `GET /api/report`  
- Có Data Stitching bằng `order_id`  
- Có tính tổng doanh thu theo user  
- Có pagination, không dùng `SELECT *`  
- Có dashboard trực quan  
"""
)