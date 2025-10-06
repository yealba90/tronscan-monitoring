# AI USAGE LOG

## Commit 1: Estructura inicial del proyecto.
 Con la ayuda de ChatGPT, se solicito un plan detallado de estructura para iniciar el pipeline. Manualmente se realiza la creación de carpetas y archivos requereridos.

## Commit 2: Extract
Se construyó, con apoyo de herramientas de IA, el módulo de extracción de transacciones de wallets TRON desde la API pública de TronScan.
Diseño del plan:
Utilicé ChatGPT para que me ayudara a desglosar la Fase 1 en pasos claros (analizar endpoint TronScan, crear cliente Python, cargar wallets desde YAML, probar en main.py, añadir logging). Esto me permitió arrancar con una estructura ordenada.
Generación de código base:
Con IA obtuve ejemplos y plantillas iniciales para:
- La clase TronScanClient en src/extract/tronscan_client.py.
- El cargador de wallets load_wallets() en src/config/config_loader.py.
- El módulo raw_saver.py para guardar datos crudos en JSON.
- El script main.py para ejecutar la extracción y ver resultados.

Ajustes y refactors:
A partir de las sugerencias del asistente, revisé y adapté:
- La paginación de la API.
- El uso de httpx para peticiones HTTP.
- La configuración de logging básico para reemplazar print() y dar visibilidad en consola.

Prueba final:
Con los ejemplos sugeridos por IA ejecuté python -m src.main desde la raíz del proyecto. El cliente se conectó a la API de TronScan, recuperó transacciones reales y las guardó en out/raw/ en formato JSON, mostrando mensajes de INFO en consola.

Valor del uso de IA:
Me ayudó a acelerar la creación de la estructura inicial del repositorio, a redactar código base robusto y a tener buenas prácticas (módulos separados, logging, YAML de configuración) desde el primer paso.

## Commit 3: Transform
Se definió un esquema estructurado para las transacciones mediante un modelo Transaction en Pydantic, que garantiza tipos consistentes (timestamp UTC, amount como Decimal, campos normalizados).
Se implementó la función parse_transaction que transforma los datos crudos extraídos en objetos validados.
Se probó la transformación integrándola en main.py, mostrando ejemplos de transacciones ya normalizadas y guardándolas opcionalmente en out/structured/ para depuración.

## Commit 4: Load
Se implementó el módulo de carga de datos transformados hacia Snowflake. Se buscó persistir en una base de datos las transacciones extraídas y normalizadas en las fases anteriores.

Se configuró un entorno Python 3.11 compatible con snowflake-connector-python y se añadieron las credenciales de Snowflake en un archivo .env seguro.

Se desarrolló la clase SnowflakeLoader en src/load/snowflake_loader.py:
Establece la conexión con Snowflake usando las credenciales.
Ejecuta USE DATABASE y USE SCHEMA para fijar el contexto.
Crea automáticamente la tabla TRANSACTIONS si no existe, con las columnas:
* TX_ID (STRING, clave primaria)
* TIMESTAMP_UTC (TIMESTAMP_TZ)
* FROM_ADDRESS (STRING)
* TO_ADDRESS (STRING/NULLABLE)
* TOKEN (STRING)
* AMOUNT (NUMBER(38,8))
* OBSERVED_WALLET (STRING)
* RAW (VARIANT con el JSON original)
Inserta cada transacción fila por fila usando PARSE_JSON para la columna RAW, garantizando compatibilidad y evitando errores de multi-row insert.
Se actualizó main.py para que, después de extraer y transformar con parse_transaction, llame a loader.insert_transactions(structured_transactions) y cargue los datos en Snowflake.
Se ajustó el modelo Transaction para permitir valores NULL en campos opcionales como to_address, evitando fallos de validación.


## Commit 5: Orquestación y Monitoreo en Snowflake
El objetivo principal fue asegurar que las cargas de datos estructurados provenientes del ETL Python se mantengan sincronizadas, auditables y con capacidad de autogestión dentro de Snowflake.

### Automatización del flujo en Snowflake
Se implementó un mecanismo completo de ejecución automática directamente en Snowflake, compuesto por:

Procedimiento almacenado
UPDATE_WALLET_METRICS()
Genera o actualiza la tabla agregada WALLET_METRICS, resumiendo la actividad por cada wallet monitoreada.
Incluye métricas como:
* Total de transacciones (TOTAL_TX)
* Tokens únicos utilizados (TOKENS_UNICOS)
* Última transacción (ULTIMA_TX)
* Monto total transado (MONTO_TOTAL)

Task programada
REFRESH_WALLET_METRICS
Ejecuta el procedimiento automáticamente cada 15 minutos, manteniendo las métricas actualizadas en paralelo al ciclo del ETL.
La task se configura con:
```bash
CREATE OR REPLACE TASK REFRESH_WALLET_METRICS
  WAREHOUSE = COMPUTE_TRON_WH
  SCHEDULE = '15 MINUTE'
  AS CALL UPDATE_WALLET_METRICS();
```
Esta integración garantiza que la base analítica de Snowflake se mantenga sincronizada con los datos cargados desde el pipeline Python.

### Creación de vista de métricas
Se implementó la vista WALLET_METRICS_VIEW, que sirve como punto central para análisis, dashboards o herramientas externas como Power BI, Snowsight o Dataiku.

Campos principales:
* OBSERVED_WALLET – dirección monitoreada
* TOTAL_TX – total de transacciones registradas
* TOKENS_UNICOS – cantidad de tokens distintos
* ULTIMA_TX – fecha/hora de la última transacción
* MONTO_TOTAL – monto agregado
* MINUTOS_DESDE_ULTIMA_TX – tiempo desde la última actividad
Esta vista facilita consultas rápidas sin necesidad de recalcular las métricas cada vez.

### Monitoreo y alertas de fallos
Se añadió una segunda task para monitorear la salud del pipeline Snowflake:

Task: ALERT_TASK_FAILURES
* Corre cada 1 hora.
* Consulta el TASK_HISTORY() de Snowflake.
* Registra en la tabla ALERT_LOGS todas las ejecuciones fallidas durante las últimas 2 horas.

```bash
INSERT INTO ALERT_LOGS (TASK_NAME, STATE, ERROR_CODE, ERROR_MESSAGE)
SELECT NAME, STATE, ERROR_CODE, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE STATE = 'FAILED'
  AND SCHEDULED_TIME >= DATEADD('hour', -2, CURRENT_TIMESTAMP());
```
Esto permite mantener un registro histórico de errores y habilita futuras integraciones con sistemas de notificación o monitoreo externo.

### Validación y supervisión de las tareas
Se creó un script SQL (check_etl_tasks.sql) para monitorear el estado del ecosistema ETL, con consultas a:
* SHOW TASKS → Ver tareas activas y su programación.
* DESCRIBE TASK → Inspeccionar configuración detallada.
* TABLE(INFORMATION_SCHEMA.TASK_HISTORY(...)) → Revisar historial de ejecuciones.
* ACCOUNT_USAGE.QUERY_HISTORY → Analizar queries ejecutadas por cada task.
Estas consultas permiten verificar fácilmente si las tareas se ejecutan correctamente, cuándo fue la última ejecución exitosa y cuánto tardaron.

### Logging y visibilidad
* Los registros del ETL se mantienen en logs/etl_tronscan.log con salida simultánea en consola.
* Cada ejecución muestra el ciclo completo: Extracción → Transformación → Carga → Task Snowflake → Cierre de conexión.
Se validó la consistencia entre los datos locales (out/structured/) y los almacenados en la tabla TRANSACTIONS.

### Flujo completo del pipeline
```bash
    A[ETL Python cada 15 min] -->|Carga| B[Tabla TRANSACTIONS]
    B -->|Ejecución automática| C[Task REFRESH_WALLET_METRICS]
    C -->|Actualiza| D[Tabla WALLET_METRICS]
    D --> E[Vista WALLET_METRICS_VIEW]
    B -->|Monitoreo| F[Task ALERT_TASK_FAILURES]
    F --> G[Tabla ALERT_LOGS]
    G -->|Consulta| H[Snowsight / Dashboard / Alertas]
```
## Commit 5: alert rules (Transaction Count y Volume)
Se implementa la **lógica de detección de comportamientos anómalos** mediante reglas de negocio directamente en **Snowflake**, con el objetivo de generar alertas cuando las wallets superen umbrales definidos de actividad o volumen transaccionado.

---

### Objetivo

Detectar de forma automática patrones de comportamiento inusuales en las wallets monitoreadas:

- **Transaction Count Rule** → cuando una wallet realiza más de *N* transacciones en una ventana de tiempo.  
- **Transaction Volume Rule** → cuando una wallet transfiere más de *X* unidades de valor en una ventana determinada.  

Ambas reglas se ejecutan de forma recurrente mediante **Snowflake Tasks**, generando alertas que posteriormente son exportadas a JSON en la Fase 5.

---

### Estructura implementada

Se añadieron tres componentes principales dentro del esquema `TRANSACTIONS`:

| Componente | Descripción |
|-------------|-------------|
| **`ALERT_THRESHOLDS`** | Tabla de parámetros que define los umbrales por regla y periodo (7 y 30 días). |
| **`CHECK_WALLET_RULES()`** | Procedimiento que evalúa ambas reglas y registra las alertas detectadas. |
| **`CHECK_WALLET_RULES_TASK`** | Task automática que ejecuta el procedimiento cada 15 minutos. |

---

### Tabla de umbrales configurables

Permite ajustar los valores de alerta sin modificar el código del procedimiento:

```sql
CREATE OR REPLACE TABLE ALERT_THRESHOLDS (
    RULE_NAME STRING PRIMARY KEY,
    PERIOD_DAYS INT,
    THRESHOLD FLOAT
);

INSERT INTO ALERT_THRESHOLDS VALUES
  ('Transaction Count Rule - 7d', 7, 100),
  ('Transaction Count Rule - 30d', 30, 300),
  ('Transaction Volume Rule - 7d', 7, 500000),
  ('Transaction Volume Rule - 30d', 30, 2000000);
```

### Procedimiento CHECK_WALLET_RULES()
Evalúa los datos en la tabla TRANSACTIONS y registra las alertas que superen los límites establecidos:
```sql
CREATE OR REPLACE TABLE ALERT_THRESHOLDS (
    RULE_NAME STRING PRIMARY KEY,
    PERIOD_DAYS INT,
    THRESHOLD FLOAT
);

INSERT INTO ALERT_THRESHOLDS VALUES
  ('Transaction Count Rule - 7d', 7, 100),
  ('Transaction Count Rule - 30d', 30, 300),
  ('Transaction Volume Rule - 7d', 7, 500000),
  ('Transaction Volume Rule - 30d', 30, 2000000);
```
### Task de ejecución periódica
Esta tarea se encarga de lanzar las reglas cada 15 minutos:}
```sql
CREATE OR REPLACE TASK CHECK_WALLET_RULES_TASK
  WAREHOUSE = COMPUTE_TRON_WH
  SCHEDULE = '15 MINUTE'
AS
  CALL CHECK_WALLET_RULES();
```

## Commit 6: alert JSON

