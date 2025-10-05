import httpx
from typing import List, Dict, Any
from tenacity import retry, wait_fixed, stop_after_attempt, wait_exponential

class TronScanClient:
    BASE_URL = "https://apilist.tronscan.org/api"

    def __init__(self, timeout: int = 10.0):
        self.client = httpx.Client(timeout=timeout)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10)) # Reintenta hasta 3 veces con espera exponencial
    def get_transactions(self, wallet: str, limit: int = 50, start: int = 0) -> List[Dict[str, Any]]:
        """
        Se obtienen las transacciones de una billetera TRON.
        """
        url = f"{self.BASE_URL}/transaction"
        params = {
            "address": wallet,
            "limit": limit,
            "start": start
        }
        response = self.client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get("data", [])

    def fetch_all_transactions(self, wallet: str, max_pages: int = 5, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Itera sobre las páginas para obtener todas las transacciones de una billetera.
        """
        all_transactions = []
        start = 0
        for _ in range(max_pages):
            transactions = self.get_transactions(wallet, limit=limit, start=start)
            if not transactions:
                break
            all_transactions.extend(transactions)
            start += limit
        return all_transactions
    
    def close(self):
        self.client.close()