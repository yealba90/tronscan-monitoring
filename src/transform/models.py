from decimal import Decimal
from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional

class Transaction(BaseModel):
    transaction_id: str = Field(..., description="Transaction ID")
    timestamp: datetime = Field(..., description="Timestamp")
    from_address: str = Field(..., description="From Address")
    to_address: str = Field(..., description="To Address")
    token: Optional[str] = Field(None, description="Token/Asset")
    amount: Decimal = Field(None, description="Amount")
    observed_wallet: str = Field(..., description="Wallet Monitored")
    raw: dict = Field(..., description="Raw transaction data")