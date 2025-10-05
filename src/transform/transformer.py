from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict
from .models import Transaction

def parse_transaction(raw_tx: Dict, observed_wallet: str) -> Transaction:
    """
    Convierte una transacción cruda de TronScan a Transaction (pydantic).
    Ajusta correctamente Transaction ID, Timestamp, From, To, Token y Amount
    tanto para TRX nativo como para TRC20.
    """

    # --- Transaction ID ---
    transaction_id = raw_tx.get("hash") or raw_tx.get("transaction_id")

        # --- Timestamp (epoch ms → UTC datetime) ---
    timestamp = None
    if raw_tx.get("timestamp"):
        timestamp = datetime.fromtimestamp(raw_tx["timestamp"] / 1000, tz=timezone.utc)

    # --- From / To addresses ---
    from_address = raw_tx.get("ownerAddress")

    # TronScan puede tener toAddress directo, o dentro de contractData
    to_address = raw_tx.get("toAddress")
    if not to_address and "contractData" in raw_tx:
        to_address = raw_tx["contractData"].get("to_address") or raw_tx["contractData"].get("receiver_address")

    # --- Token / Asset type ---
    token_info = raw_tx.get("tokenInfo") or {}
    token_abbr = token_info.get("tokenAbbr") or token_info.get("tokenName")

    # Para algunos TRC20 viene además en toAddressTag
    if not token_abbr:
        token_abbr = raw_tx.get("toAddressTag")

    # --- Amount ---
    decimals = int(token_info.get("tokenDecimal", 6))

    amount_val = None

    # TRC20 transfer: está en trigger_info.parameter._value
    trigger_info = raw_tx.get("trigger_info") or {}
    parameter = trigger_info.get("parameter") or {}
    if "_value" in parameter:
        try:
            amount_val = Decimal(parameter["_value"]) / (10 ** decimals)
        except Exception:
            amount_val = Decimal(0)
    else:
        # TRX nativo u otros tokens: puede estar en amount o contractData.amount
        raw_amount = raw_tx.get("amount") or raw_tx.get("contractData", {}).get("amount")
        if raw_amount is not None:
            try:
                amount_val = Decimal(raw_amount) / (10 ** decimals)
            except Exception:
                amount_val = Decimal(0)
        else:
            amount_val = Decimal(0)





    return Transaction(
        transaction_id=transaction_id,
        timestamp=timestamp,
        from_address=from_address,
        to_address=to_address,
        token=token_abbr,
        amount=amount_val,
        observed_wallet=observed_wallet,
        raw=raw_tx
    )