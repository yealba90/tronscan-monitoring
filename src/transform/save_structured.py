# src/transform/save_structured.py
import json
from pathlib import Path
from datetime import datetime
from typing import List
from .models import Transaction

def save_structured(wallet: str, transactions: List[Transaction], out_dir: str = "out/structured"):
    """
    Guarda las transacciones estructuradas en JSON en out/structured.
    """
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = Path(out_dir) / f"{wallet}_{ts}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump([t.dict() for t in transactions], f, indent=2, default=str)
    print(f"Datos estructurados guardados en: {filename}")
