USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_TRON_WH;
USE DATABASE TRON_SCAN_MONITORING;
USE SCHEMA TRANSACTIONS;


CREATE OR REPLACE VIEW WALLET_METRICS AS
SELECT
    OBSERVED_WALLET,

    -- Número de transacciones en últimos 7 días
    COUNT(*) AS TX_COUNT_7D,

    -- Volumen total (suma de montos) en últimos 7 días
    SUM(AMOUNT) AS VOL_7D,

    -- Monto promedio de transacciones
    AVG(AMOUNT) AS AVG_AMOUNT_7D,

    -- Número de tokens diferentes utilizados
    COUNT(DISTINCT TOKEN) AS UNIQUE_TOKENS_7D,

    -- Valor máximo transado
    MAX(AMOUNT) AS MAX_TX_AMOUNT_7D,

    -- Días de inactividad (desde la última transacción)
    DATEDIFF(day, MAX(TIMESTAMP_UTC), CURRENT_TIMESTAMP()) AS INACTIVE_DAYS,

    -- Proporción de transacciones con token principal (ejemplo: USDT)
    CASE
        WHEN COUNT(*) = 0 THEN 0
        ELSE
            SUM(CASE WHEN UPPER(TOKEN) = 'USDT' THEN 1 ELSE 0 END) / COUNT(*)
    END AS MAIN_TOKEN_RATIO,

    -- Fecha de actualización de la vista
    CURRENT_TIMESTAMP() AS LAST_REFRESH

FROM TRANSACTIONS
WHERE TIMESTAMP_UTC >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY OBSERVED_WALLET
ORDER BY VOL_7D DESC;
