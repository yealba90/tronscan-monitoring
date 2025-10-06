import os
import json
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector

def export_alerts_to_json():
    """Consulta las alertas recientes en Snowflake y las guarda en un archivo JSON."""
    load_dotenv()

    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"]
    )

    query = """
        SELECT WALLET, RULE_NAME AS RULE, METRIC_VALUE AS VALUE,
               THRESHOLD, ALERT_TIME AS TIMESTAMP
        FROM ALERT_RULES_LOG
        WHERE ALERT_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
          AND STATUS = 'ALERT'
        ORDER BY ALERT_TIME DESC;
    """

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        columns = [col[0] for col in cur.description]

    conn.close()

    # Convertir resultados a lista de dicts
    alerts = [dict(zip(columns, row)) for row in rows]

    if not alerts:
        print("No se encontraron alertas recientes.")
        return None

    # Crear carpeta si no existe
    os.makedirs("out/alerts", exist_ok=True)
    filename = f"out/alerts/alerts_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"

    # Guardar archivo JSON
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(alerts, f, indent=4, default=str)

    print(f"🚨 Alertas exportadas a {filename}")
    return filename
