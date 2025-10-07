-- ===============================================================
-- SCRIPT DE CREACIÓN DE LA TASK CHECK_WALLET_RULES_TASK
-- Proyecto: TronScan Monitoring Pipeline
-- Autor: Yeison Alvarez Balvin
-- Fecha: 2025-10-06
-- ===============================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_TRON_WH;
USE DATABASE TRON_SCAN_MONITORING;
USE SCHEMA TRANSACTIONS;

CREATE OR REPLACE TASK CHECK_WALLET_RULES_TASK
  WAREHOUSE = COMPUTE_TRON_WH
  SCHEDULE = '15 MINUTE'
AS
  CALL CHECK_WALLET_RULES();
