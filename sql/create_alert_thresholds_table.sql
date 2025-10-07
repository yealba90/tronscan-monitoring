-- ===============================================================
-- SCRIPT DE CREACIÓN DE LA TABLA ALERT_THRESHOLDS
-- Proyecto: TronScan Monitoring Pipeline
-- Autor: Yeison Alvarez Balvin
-- Fecha: 2025-10-06
-- ===============================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_TRON_WH;
USE DATABASE TRON_SCAN_MONITORING;
USE SCHEMA TRANSACTIONS;
CREATE OR REPLACE TABLE ALERT_THRESHOLDS (
    RULE_NAME STRING PRIMARY KEY,
    PERIOD_DAYS INT,
    THRESHOLD FLOAT
);

-- Valores iniciales
INSERT INTO ALERT_THRESHOLDS VALUES
  ('Transaction Count Rule - 7d', 7, 100),
  ('Transaction Count Rule - 30d', 30, 300),
  ('Transaction Volume Rule - 7d', 7, 500000),
  ('Transaction Volume Rule - 30d', 30, 2000000);
