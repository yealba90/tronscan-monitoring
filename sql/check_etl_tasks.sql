-- ===============================================================
-- SCRIPT DE VALIDACIÓN COMPLETA DE TASKS Y ETL EN SNOWFLAKE
-- Proyecto: TronScan Monitoring Pipeline
-- Autor: Yeison Alvarez Balvin
-- Fecha: 2025-10-06
-- ===============================================================

-- Establecer el contexto correcto
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_TRON_WH;
USE DATABASE TRON_SCAN_MONITORING;
USE SCHEMA TRANSACTIONS;

-- ===============================================================
-- Mostrar todas las tasks en el esquema actual
-- ===============================================================
SHOW TASKS;

-- ===============================================================
-- Describir la task principal (para ver su configuración)
-- ===============================================================
DESCRIBE TASK REFRESH_WALLET_METRICS;

-- ===============================================================
-- Consultar el historial reciente de ejecuciones de la task
-- ===============================================================
SELECT
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    DATEDIFF('second', SCHEDULED_TIME, COMPLETED_TIME) AS DURATION_SEC,
    RETURN_VALUE,
    QUERY_ID
FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        TASK_NAME => 'REFRESH_WALLET_METRICS',
        RESULT_LIMIT => 10
    )
)
ORDER BY SCHEDULED_TIME DESC;

-- ===============================================================
-- Consultar las últimas queries ejecutadas relacionadas con la task
-- ===============================================================
SELECT
    QUERY_ID,
    USER_NAME,
    WAREHOUSE_NAME,
    START_TIME,
    END_TIME,
    EXECUTION_STATUS,
    ERROR_CODE,
    ERROR_MESSAGE,
    QUERY_TEXT
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_TEXT ILIKE '%REFRESH_WALLET_METRICS%'
  AND START_TIME > DATEADD('hour', -2, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC
LIMIT 10;

-- ===============================================================
-- Verificar el efecto de la task en los datos
-- ===============================================================
SELECT
    COUNT(*) AS TOTAL_TRANSACCIONES
FROM TRANSACTIONS;

