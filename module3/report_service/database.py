import os
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError


MYSQL_URL = os.getenv(
    "MYSQL_URL",
    "mysql+pymysql://root:root@mysql:3306/noah_store"
)

POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql+psycopg2://postgres:postgres@postgres:5432/noah_finance"
)


def create_db_engine(db_url: str, name: str, retries: int = 10, delay: int = 5):
    """
    Retry connection vì DB có thể khởi động chậm 10-20s.
    Đây là yêu cầu độ bền vững của project.
    """
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            engine = create_engine(db_url, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(f"[INFO] Connected to {name}")
            return engine

        except OperationalError as e:
            last_error = e
            print(f"[WARN] Cannot connect to {name}. Attempt {attempt}/{retries}")
            time.sleep(delay)

    raise RuntimeError(f"Cannot connect to {name}: {last_error}")


mysql_engine = create_db_engine(MYSQL_URL, "MySQL")
postgres_engine = create_db_engine(POSTGRES_URL, "PostgreSQL")