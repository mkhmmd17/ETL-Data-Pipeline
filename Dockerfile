FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
COPY etl_pipeline.py .

RUN pip install --no-cache-dir -r requirements.txt


CMD ["python", "etl_pipeline.py"]