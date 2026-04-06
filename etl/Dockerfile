FROM python:3.12-slim
RUN pip install --no-cache-dir pandas openpyxl sqlalchemy psycopg2-binary
WORKDIR /etl
COPY daily_sync.py .
CMD ["python3", "daily_sync.py"]
