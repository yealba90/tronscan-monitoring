-- ===============================================================
-- SCRIPT DE CREACIÓN PROCEDIMIENTO REFRESH_WALLET_METRICS, 
-- CREACIÓN DE LA TASK REFRESH_WALLET_METRICS
-- Proyecto: TronScan Monitoring Pipeline
-- Autor: Yeison Alvarez Balvin
-- Fecha: 2025-10-06
-- ===============================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_TRON_WH;
USE DATABASE TRON_SCAN_MONITORING;
USE SCHEMA TRANSACTIONS;

-- ===============================================================
-- Crear o reemplazar el procedimiento de agregación de métricas
-- ===============================================================
CREATE OR REPLACE PROCEDURE UPDATE_WALLET_METRICS()
RETURNS STRING
LANGUAGE SQL
AS
$$
    CREATE OR REPLACE TABLE WALLET_METRICS AS
    SELECT
        OBSERVED_WALLET,
        COUNT(*) AS TOTAL_TX,
        COUNT(DISTINCT TOKEN) AS TOKENS_UNICOS,
        MAX(TIMESTAMP_UTC) AS ULTIMA_TX,
        SUM(AMOUNT) AS MONTO_TOTAL
    FROM TRANSACTIONS
    GROUP BY OBSERVED_WALLET;
$$;

-- ===============================================================
-- Crear o reemplazar la Task que ejecuta el procedimiento
-- ===============================================================
CREATE OR REPLACE TASK REFRESH_WALLET_METRICS
  WAREHOUSE = COMPUTE_TRON_WH
  SCHEDULE = '15 MINUTE'
  COMMENT = 'Actualiza la tabla WALLET_METRICS con datos agregados'
AS
  CALL UPDATE_WALLET_METRICS();

-- ===============================================================
-- Activar la Task
-- ===============================================================
ALTER TASK REFRESH_WALLET_METRICS RESUME;

-- ===============================================================
-- Verificar que se haya creado correctamente
-- ===============================================================
SHOW TASKS;
