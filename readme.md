# NOAH PROJECT – RUN GUIDE

## 1. Clone project
git clone <link_repo>
cd GR_PJ

## 2. Cài thư viện Python
pip install flask pymysql pika psycopg2-binary cryptography

## 3. Chạy RabbitMQ (Docker)
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

RabbitMQ UI:
http://localhost:15672
user: guest
pass: guest

## 4. Chạy PostgreSQL (Docker)
docker run -d --name postgres -e POSTGRES_PASSWORD=123456 -p 5432:5432 postgres

## 5. Setup PostgreSQL
docker exec -it postgres psql -U postgres

CREATE DATABASE noah_finance;
\c noah_finance

CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    order_id INT,
    user_id INT,
    product_id INT,
    quantity INT,
    total_price DECIMAL(10,2),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

\q

## 6. Setup MySQL
Mở MySQL Workbench → chạy file init.sql

## 7. Chạy API
py order_api/app.py

## 8. Chạy Worker
py order_worker/worker.py

## 9. Test API (Postman)
POST http://localhost:5000/api/orders

Body:
{
  "user_id": 2,
  "product_id": 102,
  "quantity": 1
}

## 10. Kiểm tra kết quả

MySQL:
SELECT * FROM orders;

→ status = COMPLETED

PostgreSQL:
SELECT * FROM transactions;

→ có dữ liệu mới

## DONE
API → RabbitMQ → Worker → DB hoạt động OK

## NOTE
- MySQL database: init
- PostgreSQL database: noah_finance
- Không đổi password nếu không sửa code