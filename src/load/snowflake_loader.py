import os
import json
from typing import List
from pathlib import Path
from src.transform.models import Transaction
from dotenv import load_dotenv
import snowflake.connector

class SnowflakeLoader:
    def __init__(self):
        load_dotenv()
        self.conn = snowflake.connector.connect(
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        )
        with self.conn.cursor() as cur:
            cur.execute(f"USE DATABASE {os.environ['SNOWFLAKE_DATABASE']}")
            cur.execute(f"USE SCHEMA {os.environ['SNOWFLAKE_SCHEMA']}")
        self._create_table()

    def _create_table(self):
        create_stmt = """
        CREATE TABLE IF NOT EXISTS TRANSACTIONS (
            TX_ID STRING PRIMARY KEY,
            TIMESTAMP_UTC TIMESTAMP_TZ,
            FROM_ADDRESS STRING,
            TO_ADDRESS STRING,
            TOKEN STRING,
            AMOUNT NUMBER(38,8),
            OBSERVED_WALLET STRING,
            RAW VARIANT
        )
        """
        with self.conn.cursor() as cur:
            cur.execute(create_stmt)
        self.conn.commit()

    def insert_transactions(self, transactions: List[Transaction]):
        """
        Inserta transacciones fila por fila usando PARSE_JSON
        (forma estable sin errores de rewrite).
        """
        insert_stmt = """
        INSERT INTO TRANSACTIONS (
            TX_ID, TIMESTAMP_UTC, FROM_ADDRESS, TO_ADDRESS, TOKEN, AMOUNT, OBSERVED_WALLET, RAW
        ) SELECT %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)
        """
        with self.conn.cursor() as cur:
            for t in transactions:
                cur.execute(insert_stmt, (
                    t.transaction_id,
                    t.timestamp.isoformat(),
                    t.from_address,
                    t.to_address,
                    t.token,
                    float(t.amount),
                    t.observed_wallet,
                    json.dumps(t.raw)
                ))
        self.conn.commit()    

    # def insert_transactions(self, transactions: list):
    #     insert_stmt = """
    #         INSERT INTO TRANSACTIONS (
    #             TX_ID, TIMESTAMP_UTC, FROM_ADDRESS, TO_ADDRESS, TOKEN, AMOUNT, OBSERVED_WALLET, RAW
    #         ) SELECT %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)
    #         """
    #     rows = [
    #         (
    #             t.transaction_id,
    #             t.timestamp.isoformat(),
    #             t.from_address,
    #             t.to_address,
    #             t.token,
    #             float(t.amount),
    #             t.observed_wallet,
    #             t.raw
    #         ) for t in transactions
    #     ]
    #     with self.conn.cursor() as cur:
    #         cur.executemany(insert_stmt, rows)
    #     self.conn.commit()
