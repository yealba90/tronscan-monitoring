import os
import logging
from dotenv import load_dotenv
import snowflake.connector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

class SnowflakeLoader:
    """
    Clase que gestiona la conexión, creación de tabla y carga de datos
    hacia Snowflake. Incluye MERGE automático para evitar duplicados
    y ejecución de Tasks (como REFRESH_WALLET_METRICS).
    """   

    def __init__(self):
        load_dotenv()

        try:
            self.conn = snowflake.connector.connect(
                user=os.environ["SNOWFLAKE_USER"],
                password=os.environ["SNOWFLAKE_PASSWORD"],
                account=os.environ["SNOWFLAKE_ACCOUNT"],
                warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
                role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
            )

            with self.conn.cursor() as cur:
                cur.execute(f"USE DATABASE {os.environ['SNOWFLAKE_DATABASE']}")
                cur.execute(f"USE SCHEMA {os.environ['SNOWFLAKE_SCHEMA']}")
                logging.info("Conexión a Snowflake establecida correctamente.")
                print("Conexión a Snowflake establecida correctamente.")

            # Crea la tabla si no existe
            self._create_table()

        except Exception as e:
            logging.error(f"Error al conectar con Snowflake: {e}")
            print(f"Error al conectar con Snowflake: {e}")
            raise e



    def _create_table(self):
        """Crea la tabla TRANSACTIONS si no existe."""
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

    def insert_transactions(self, transactions):
        """
        Inserta o actualiza transacciones en Snowflake sin duplicar.
        Convierte todos los valores a tipos básicos antes de ejecutar.
        """
        if not transactions:
            logging.info("No se recibieron transacciones para insertar.")
            print("No se recibieron transacciones para insertar.")
            return

        merge_stmt = """
        MERGE INTO TRANSACTIONS AS T
        USING (
            SELECT %s AS TX_ID,
                   %s AS TIMESTAMP_UTC,
                   %s AS FROM_ADDRESS,
                   %s AS TO_ADDRESS,
                   %s AS TOKEN,
                   %s AS AMOUNT,
                   %s AS OBSERVED_WALLET
        ) AS S
        ON T.TX_ID = S.TX_ID
           AND T.OBSERVED_WALLET = S.OBSERVED_WALLET
        WHEN MATCHED THEN UPDATE SET
            T.TIMESTAMP_UTC = S.TIMESTAMP_UTC,
            T.FROM_ADDRESS = S.FROM_ADDRESS,
            T.TO_ADDRESS = S.TO_ADDRESS,
            T.TOKEN = S.TOKEN,
            T.AMOUNT = S.AMOUNT
        WHEN NOT MATCHED THEN INSERT (
            TX_ID, TIMESTAMP_UTC, FROM_ADDRESS, TO_ADDRESS, TOKEN, AMOUNT, OBSERVED_WALLET
        ) VALUES (
            S.TX_ID, S.TIMESTAMP_UTC, S.FROM_ADDRESS, S.TO_ADDRESS, S.TOKEN, S.AMOUNT, S.OBSERVED_WALLET
        );
        """

        inserted = 0
        with self.conn.cursor() as cur:
            for tx in transactions:
                try:
                    tx_data = (
                        str(tx.transaction_id),
                        str(getattr(tx, "timestamp", None)),
                        str(tx.from_address) if tx.from_address else None,
                        str(tx.to_address) if tx.to_address else None,
                        str(tx.token) if tx.token else None,
                        float(tx.amount) if tx.amount else None,
                        str(tx.observed_wallet),
                    )
                    cur.execute(merge_stmt, tx_data)
                    inserted += 1

                except Exception as e:
                    logging.error(f"Error al insertar transacción {tx.transaction_id}: {e}")
                    print(f"Error al insertar transacción {tx.transaction_id}: {e}")

        self.conn.commit()
        logging.info(f"{inserted} transacciones procesadas (MERGE completado).")
        print(f"{inserted} transacciones procesadas (MERGE completado).")

    def run_task(self, task_name):
        """
        Ejecuta una Task existente en Snowflake (por ejemplo: REFRESH_WALLET_METRICS).
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"EXECUTE TASK {task_name};")
                logging.info(f"Task {task_name} ejecutada correctamente.")
                print(f"Task {task_name} ejecutada correctamente.")
        except Exception as e:
            logging.error(f"Error al ejecutar Task {task_name}: {e}")
            print(f"Error al ejecutar Task {task_name}: {e}")


    # =====================================================
    # CERRAR CONEXIÓN
    # =====================================================
    def close(self):
        """Cierra la conexión a Snowflake."""
        try:
            self.conn.close()
            logging.info("Conexión Snowflake cerrada correctamente.")
            print("Conexión Snowflake cerrada correctamente.")
        except Exception as e:
            logging.error(f"Error al cerrar conexión: {e}")
            print(f"Error al cerrar conexión: {e}")




