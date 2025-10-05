# src/extract/raw_saver.py
import json
from pathlib import Path
from datetime import datetime

def save_raw(wallet: str, data: list[dict], out_dir: str = "out/raw"):
    """
    Guarda los datos crudos en un archivo JSON dentro de out/raw
    """
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = Path(out_dir) / f"{wallet}_{ts}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"Datos crudos guardados en: {filename}")
