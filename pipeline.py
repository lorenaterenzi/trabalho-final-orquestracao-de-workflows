import os
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from prefect import flow, task

DB_URL = os.getenv("DB_URL", "postgresql://admin:adminpassword@postgres:5432/finance_db")

@task(retries=3, retry_delay_seconds=5)
def extract_crypto_data():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,solana,cardano",
        "vs_currencies": "usd",
        "include_24hr_vol": "true",
        "include_24hr_change": "true"
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    print(f"[EXTRACT] Cotações obtidas para {len(data)} ativos.")
    return data

@task
def transform_crypto_data(raw_data):
    rows = []
    timestamp_atual = datetime.utcnow()
    
    for coin, metrics in raw_data.items():
        rows.append({
            "coin_id": coin,
            "price_usd": float(metrics["usd"]),
            "volume_24h": float(metrics["usd_24h_vol"]),
            "change_24h_pct": float(metrics["usd_24h_change"]),
            "extracted_at": timestamp_atual
        })
    
    df = pd.DataFrame(rows)
    print(f"[TRANSFORM] Dados processados com sucesso.")
    return df

@task
def load_to_postgres(df):
    engine = create_engine(DB_URL)
    df.to_sql("fact_crypto_prices", engine, if_exists="append", index=False)
    df.to_sql("latest_crypto_prices", engine, if_exists="replace", index=False)
    print("[LOAD] Dados salvos no PostgreSQL.")

@flow(name="pipeline-financeiro-cripto")
def crypto_pipeline_flow():
    raw_data = extract_crypto_data()
    transformed_df = transform_crypto_data(raw_data)
    load_to_postgres(transformed_df)

if __name__ == "__main__":
    crypto_pipeline_flow()