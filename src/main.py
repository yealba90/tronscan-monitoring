import os
import time
import logging
import schedule
from dotenv import load_dotenv
from datetime import datetime
from src.alerts.alert_exporter import export_alerts_to_json
from src.extract.tronscan_client import TronScanClient
from src.config.config_loader import load_wallets
from src.extract.raw_saver import save_raw
from src.transform.transformer import parse_transaction
from src.transform.save_structured import save_structured
from src.load.snowflake_loader import SnowflakeLoader
from src.anomaly.anomaly_detector import detect_anomalies

# ========================================
# CONFIGURACIÓN GLOBAL DE LOGS
# ========================================

LOG_FILE = "logs/etl_tronscan.log"
os.makedirs("logs", exist_ok=True)

# Crear logger principal
logger = logging.getLogger("tron_monitoring")
logger.setLevel(logging.INFO)
logger.propagate = False  # evita duplicados en consola

# Formato de salida
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

# Handler para archivo
file_handler = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Handler para consola
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# ========================================
# FUNCIÓN PRINCIPAL DEL ETL
# ========================================

def etl_job():
    """Ejecuta el ciclo completo ETL: extracción, transformación y carga."""
    logging.info("=== Iniciando ciclo ETL TronScan ===")
    start_time = datetime.utcnow()

    try:
        client = TronScanClient()
        loader = SnowflakeLoader()
        wallets = load_wallets()

        for wallet in wallets:
            logger.info(f"Iniciando extracción de transacciones para wallet {wallet}")
            txs = client.get_transactions(wallet)

            if not txs:
                logging.warning(f"No se encontraron transacciones para {wallet}")
                continue

            # Guardar datos crudos
            raw_path = save_raw(wallet, txs)
            logger.info(f"Datos crudos guardados en: {raw_path}")

            # Transformar datos
            structured = []
            for tx in txs:
                try:
                    structured.append(parse_transaction(tx, wallet))
                except Exception as e:
                    logger.error(f"Error al parsear transacción en {wallet}: {e}")

            # Guardar estructurados
            structured_path = save_structured(wallet, structured)
            logger.info(f"Datos estructurados guardados en: {structured_path}")

            # Cargar en Snowflake
            loader.insert_transactions(structured)
            logger.info(f"Datos cargados exitosamente en Snowflake para {wallet}")

        # Ejecutar task en Snowflake al final del ciclo
        loader.run_task("REFRESH_WALLET_METRICS")

        try:
            detect_anomalies()
            logger.info("Detección de anomalías completada.")
            export_alerts_to_json()
            logger.info("Alertas exportadas correctamente a archivo JSON.")
            detect_anomalies()
            logger.info("Detección de anomalías completada.")
        except Exception as e:
            logger.error(f"Error al exportar alertas a JSON: {e}")

        elapsed = (datetime.utcnow() - start_time).total_seconds()
        logger.info(f"ETL completado correctamente en {elapsed:.2f} segundos.\n")

        loader.close()

    except Exception as e:
        logger.error(f"Error general en el ETL: {e}")
        logger.info("Reintentando en el próximo ciclo...\n")


# ========================================
# PROGRAMADOR DE TAREAS (schedule)
# ========================================

def main():
    """Ejecuta el ETL cada 15 minutos de forma continua."""
    logger.info("Servicio ETL TronScan iniciado (ejecución cada 15 min)...")

    # Ejecutar la primera vez al inicio
    etl_job()

    # Programar el ETL cada 10 minutos
    schedule.every(10).minutes.do(etl_job)

    while True:
        schedule.run_pending()
        time.sleep(10)


if __name__ == "__main__":
    main()


