import os
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

def detect_anomalies():
    """Detecta transacciones anómalas mediante reglas heurísticas simples."""
    anomalies = []

    # Conexión Snowflake
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"]
    )

    # Consulta transacciones recientes (últimos 30 días)
    query = """
        SELECT OBSERVED_WALLET, TRANSACTION_ID, TO_ADDRESS, AMOUNT, TIMESTAMP_UTC
        FROM TRANSACTIONS
        WHERE TIMESTAMP_UTC >= DATEADD(day, -30, CURRENT_TIMESTAMP())
    """
    df = conn.cursor().execute(query).fetch_pandas_all()

    # --- Regla 1: Large Transaction
    # --- Marca transacciones cuyo monto es significativamente mayor al patrón histórico de su misma wallet
    for wallet, group in df.groupby("OBSERVED_WALLET"):
        median = group["AMOUNT"].median()
        std = group["AMOUNT"].std() or 0
        threshold = median + 3 * std

        for _, row in group.iterrows():
            if row["AMOUNT"] > threshold:
                anomalies.append({
                    "wallet": wallet,
                    "transaction_id": row["TRANSACTION_ID"],
                    "anomaly_type": "large_transaction",
                    "amount": float(row["AMOUNT"]),
                    "reason": f"El monto {row['AMOUNT']:.2f} supera 3 desviaciones estándar sobre la mediana ({median:.2f})"
                })

    # --- Regla 2: Burst Activity (incremento de frecuencia)
    # --- Detecta aumentos repentinos en la frecuencia de transacciones
    df["DATE"] = pd.to_datetime(df["TIMESTAMP_UTC"]).dt.date
    freq_wallet_day = df.groupby(["OBSERVED_WALLET", "DATE"]).size().reset_index(name="TX_COUNT")

    for wallet, group in freq_wallet_day.groupby("OBSERVED_WALLET"):
        if len(group) >= 7:
            avg_week = group.tail(7)["TX_COUNT"].mean()
            today_tx = group["TX_COUNT"].iloc[-1]
            if today_tx > 2 * avg_week:
                anomalies.append({
                    "wallet": wallet,
                    "transaction_id": None,
                    "anomaly_type": "burst_activity",
                    "amount": None,
                    "reason": f"Frecuencia diaria ({today_tx}) es > 2x del promedio semanal ({avg_week:.1f})"
                })

    # --- Regla 3: New Counterparty
    # --- Detecta transacciones hacia nuevas wallets no vistas antes
    seen_counterparties = set()
    for _, row in df.iterrows():
        pair = (row["OBSERVED_WALLET"], row["TO_ADDRESS"])
        if pair not in seen_counterparties:
            if len([p for p in seen_counterparties if p[0] == row["OBSERVED_WALLET"]]) > 0:
                anomalies.append({
                    "wallet": row["OBSERVED_WALLET"],
                    "transaction_id": row["TRANSACTION_ID"],
                    "anomaly_type": "new_counterparty",
                    "amount": float(row["AMOUNT"]),
                    "reason": f"Transferencia a nueva wallet destino: {row['TO_ADDRESS']}"
                })
            seen_counterparties.add(pair)

    conn.close()

    # Guardar resultados
    if anomalies:
        os.makedirs("out/anomalies", exist_ok=True)
        filename = f"out/anomalies/anomalies_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(anomalies, f, indent=2, ensure_ascii=False)
        print(f"{len(anomalies)} anomalías detectadas y guardadas en {filename}")
    else:
        print("No se detectaron anomalías recientes.")
