from src.extract.tronscan_client import TronScanClient
from src.config.config_loader import load_wallets
from src.extract.raw_saver import save_raw

from src.transform.transformer import parse_transaction
from src.transform.models import Transaction
from src.transform.save_structured import save_structured

from src.load.snowflake_loader import SnowflakeLoader

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

def main():
    wallets = load_wallets()
    client = TronScanClient()
    loader = SnowflakeLoader()

    for wallet in wallets:
        log.info(f"Iniciando extracción de transacciones para wallet {wallet}")
        txs = client.fetch_all_transactions(wallet, max_pages=1, limit=10)
        log.info(f"Recuperadas {len(txs)} transacciones del wallet {wallet}")
        if txs:
            save_raw(wallet, txs)
            structured_transactions = [parse_transaction(tx, wallet) for tx in txs]
            save_structured(wallet, structured_transactions)
            loader.insert_transactions(structured_transactions)
            log.info(f"Insertadas {len(structured_transactions)} transacciones en Snowflake")

            # save_raw(wallet, txs)
            # log.info(f"Datos crudos guardados para wallet {wallet}")

            # structured_transactions = [parse_transaction(tx, wallet) for tx in txs]
            # log.info(f"Transacción estructurada:\n{structured_transactions[0].dict()}") 

            # save_structured(wallet, structured_transactions)
            # log.info(f"Datos estructurados guardados para wallet {wallet}")



    client.close()
    log.info("Proceso de carga en Snowflake finalizado")

if __name__ == "__main__":
    main()
