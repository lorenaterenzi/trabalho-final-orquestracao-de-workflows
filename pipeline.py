import os
import time
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from prefect import flow, task

DB_URL = os.getenv("DB_URL", "postgresql://admin:adminpassword@postgres:5432/finance_db")
PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://prefect-server:4200/api")

def wait_for_prefect():
    """Aguarda o servidor do Prefect responder antes de iniciar."""
    health_url = PREFECT_API_URL.replace("/api", "/api/health")
    print(f"[WAIT] Aguardando API do Prefect em {health_url}...")
    for _ in range(30):
        try:
            r = requests.get(health_url, timeout=2)
            if r.status_code == 200:
                print("[WAIT] Servidor Prefect pronto!")
                return
        except Exception:
            pass
        time.sleep(2)
    print("[WAIT] Aviso: Timeout aguardando Prefect, tentando executar...")

# ── TASK 1: Ingestão de Dados ────────────────────────────────────────────────
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
    print(f"[EXTRACT] Cotações obtidas com sucesso para {len(data)} ativos.")
    return data

# ── TASK 2: Transformação ─────────────────────────────────────────────────────
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
    print(f"[TRANSFORM] Dados estruturados com sucesso.")
    return df

# ── TASK 3: Carga Idempotente ────────────────────────────────────────────────
@task
def load_to_postgres(df):
    engine = create_engine(DB_URL)
    df.to_sql("fact_crypto_prices", engine, if_exists="append", index=False)
    df.to_sql("latest_crypto_prices", engine, if_exists="replace", index=False)
    print("[LOAD] Dados persistidos com sucesso no banco PostgreSQL ('finance_db').")

# ── FLOW PRINCIPAL ───────────────────────────────────────────────────────────
@flow(name="pipeline-financeiro-cripto")
def crypto_pipeline_flow():
    raw_data = extract_crypto_data()
    transformed_df = transform_crypto_data(raw_data)
    load_to_postgres(transformed_df)

if __name__ == "__main__":
    wait_for_prefect()
    crypto_pipeline_flow()