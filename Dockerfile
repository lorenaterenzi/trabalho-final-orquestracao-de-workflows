FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    prefect \
    requests \
    pandas \
    psycopg2-binary \
    sqlalchemy

COPY . /app/

CMD ["python", "pipeline.py"]