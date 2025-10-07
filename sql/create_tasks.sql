-- ===============================================================
-- SCRIPT DE CREACIÓN DE LA TASK REFRESH_WALLET_METRICS
-- Proyecto: TronScan Monitoring Pipeline
-- Autor: Yeison Alvarez Balvin
-- Fecha: 2025-10-06
-- ===============================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_TRON_WH;
USE DATABASE TRON_SCAN_MONITORING;
USE SCHEMA TRANSACTIONS;

-- Crear la Task programada
CREATE OR REPLACE TASK REFRESH_WALLET_METRICS
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 * * * * UTC'  -- cada hora
COMMENT = 'Task que inserta snapshot de WALLET_METRICS cada hora'
AS
INSERT INTO WALLET_METRICS_HIST
SELECT 
    *,
    CURRENT_TIMESTAMP() AS SNAPSHOT_TS
FROM WALLET_METRICS;
