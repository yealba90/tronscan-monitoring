from src.extract.tronscan_client import TronScanClient
from src.config.config_loader import load_wallets
from src.extract.raw_saver import save_raw
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

    for wallet in wallets:
        log.info(f"Iniciando extracción de transacciones para wallet {wallet}")
        txs = client.fetch_all_transactions(wallet, max_pages=1, limit=10)
        log.info(f"Recuperadas {len(txs)} transacciones del wallet {wallet}")
        if txs:
            save_raw(wallet, txs)
            log.info(f"Datos crudos guardados para wallet {wallet}")
    client.close()
    log.info("Proceso de extracción finalizado")

if __name__ == "__main__":
    main()
