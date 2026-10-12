FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY migrate_mongo_to_postgres.py .

CMD ["python", "migrate_mongo_to_postgres.py"]