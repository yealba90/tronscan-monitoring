import os
import json
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector

def export_alerts_to_json():
    """Consulta las alertas recientes en Snowflake y las guarda en un archivo JSON."""
    load_dotenv()

    # Establecer conexión a Snowflake
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    )

    try:
        with conn.cursor() as cur:
            # Fijar contexto explícito (previene errores de compilación SQL)
            cur.execute(f"USE DATABASE {os.environ['SNOWFLAKE_DATABASE']}")
            cur.execute(f"USE SCHEMA {os.environ['SNOWFLAKE_SCHEMA']}")

            # Consulta calificada con nombre completo
            query = f"""
                SELECT WALLET,
                    RULE_NAME AS RULE,
                    METRIC_VALUE AS VALUE,
                    THRESHOLD,
                    ALERT_TIME AS TIMESTAMP
                FROM {os.environ['SNOWFLAKE_DATABASE']}.{os.environ['SNOWFLAKE_SCHEMA']}.ALERT_RULES_LOG
                WHERE ALERT_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
                AND STATUS = 'ALERT'
                ORDER BY ALERT_TIME DESC;
            """

            cur.execute(query)
            rows = cur.fetchall()
            columns = [col[0] for col in cur.description]

    except Exception as e:
        print(f"Error al consultar alertas: {e}")
        conn.close()
        return None

    conn.close()

    # Convertir resultados a lista de diccionarios
    alerts = [dict(zip(columns, row)) for row in rows]

    if not alerts:
        print("No se encontraron alertas recientes.")
        return None

    # Crear carpeta de salida
    os.makedirs("out/alerts", exist_ok=True)
    filename = f"out/alerts/alerts_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"

    # Guardar archivo JSON
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(alerts, f, indent=4, default=str)

    print(f"Alertas exportadas correctamente a {filename}")
    return filename
