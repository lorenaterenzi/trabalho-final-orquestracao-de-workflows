FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "pydantic>=2.7.0,<2.9.0" \
    "pydantic-core<2.24.0" \
    prefect==3.0.0 \
    requests \
    pandas \
    psycopg2-binary \
    sqlalchemy

COPY . /app/

CMD ["python", "pipeline.py"]