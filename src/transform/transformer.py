from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict
from .models import Transaction

def parse_transaction(raw_tx: Dict, observed_wallet: str) -> Transaction:
    """
    Convierte una transacción cruda de TronScan a Transaction (pydantic)
    """
    transaction_id = raw_tx.get("hash")
    timestamp = datetime.fromtimestamp(raw_tx.get("timestamp")/1000, tz=timezone.utc)
    from_address = raw_tx.get("ownerAddress")
    to_address = raw_tx.get("toAddress")
    token = raw_tx.get("tokenInfo", {}).get("tokenSymbol")

    decimals = raw_tx.get("tokenInfo", {}).get("tokenDecimal", 6)
    raw_amount = raw_tx.get("amount", 0)

    amount = Decimal(raw_amount) / Decimal(10**int(decimals))



    return Transaction(
        transaction_id=transaction_id,
        timestamp=timestamp,
        from_address=from_address,
        to_address=to_address,
        token=token,
        amount=amount,
        observed_wallet=observed_wallet,
        raw=raw_tx
    )