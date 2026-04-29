FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY legacy_adapter.py .

CMD ["python", "legacy_adapter.py"]