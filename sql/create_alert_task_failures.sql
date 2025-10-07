-- ===============================================================
-- SCRIPT DE CREACIÓN DE LA TASK Y TABLA DE LOGS
-- Proyecto: TronScan Monitoring Pipeline
-- Autor: Yeison Alvarez Balvin
-- Fecha: 2025-10-06
-- ===============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_TRON_WH;
USE DATABASE TRON_SCAN_MONITORING;
USE SCHEMA TRANSACTIONS;

-- ===============================================================
-- 1️⃣ Crear tabla de logs si no existe
-- ===============================================================
CREATE OR REPLACE TABLE ALERT_LOGS (
    LOG_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    TASK_NAME STRING,
    STATE STRING,
    ERROR_CODE STRING,
    ERROR_MESSAGE STRING
);

-- ===============================================================
-- 2️⃣ Crear task que verifica fallos en las últimas 2 horas
-- ===============================================================
CREATE OR REPLACE TASK ALERT_TASK_FAILURES
  WAREHOUSE = COMPUTE_TRON_WH
  SCHEDULE = '1 HOUR'
  COMMENT = 'Registra fallos recientes de tasks en ALERT_LOGS'
AS
INSERT INTO ALERT_LOGS (TASK_NAME, STATE, ERROR_CODE, ERROR_MESSAGE)
SELECT
    NAME,
    STATE,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE STATE = 'FAILED'
  AND SCHEDULED_TIME >= DATEADD('hour', -2, CURRENT_TIMESTAMP());

-- ===============================================================
-- 3️⃣ Activar la task
-- ===============================================================
ALTER TASK ALERT_TASK_FAILURES RESUME;

-- ===============================================================
-- 4️⃣ Verificar
-- ===============================================================
SHOW TASKS;
