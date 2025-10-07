-- ===============================================================
-- SCRIPT DE CREACIÓN DE LA TABLA WALLET_METRICS_HIST
-- Proyecto: TronScan Monitoring Pipeline
-- Autor: Yeison Alvarez Balvin
-- Fecha: 2025-10-06
-- ===============================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_TRON_WH;
USE DATABASE TRON_SCAN_MONITORING;
USE SCHEMA TRANSACTIONS;

CREATE OR REPLACE TABLE WALLET_METRICS_HIST AS
SELECT 
    *,
    CURRENT_TIMESTAMP() AS SNAPSHOT_TS
FROM WALLET_METRICS
WHERE 1 = 0;