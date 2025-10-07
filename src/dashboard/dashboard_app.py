import streamlit as st
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

# ======================================================
# CONFIGURACIÓN DE ENTORNO Y CONEXIÓN
# ======================================================
load_dotenv()

st.set_page_config(
    page_title="TronScan Monitoring Dashboard",
    page_icon="🚀",
    layout="wide"
)

# Crear conexión Snowflake
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    )

conn = init_connection()

# ======================================================
# CONSULTAS SQL (Similares al dashboard Snowsight)
# ======================================================

QUERY_WALLET_ACTIVITY = """
SELECT OBSERVED_WALLET AS WALLET,
       COUNT(*) AS TOTAL_TX,
       SUM(AMOUNT) AS TOTAL_AMOUNT
FROM TRANSACTIONS
WHERE TIMESTAMP_UTC >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY OBSERVED_WALLET
ORDER BY TOTAL_TX DESC;
"""

QUERY_TOP5_VOLUME = """
SELECT OBSERVED_WALLET,
       SUM(AMOUNT) AS VOLUME
FROM TRANSACTIONS
WHERE TIMESTAMP_UTC >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY OBSERVED_WALLET
ORDER BY VOLUME DESC
LIMIT 5;
"""

QUERY_TX_TRENDS = """
SELECT DATE_TRUNC('day', TIMESTAMP_UTC) AS DATE,
       COUNT(*) AS TRANSACTIONS
FROM TRANSACTIONS
WHERE TIMESTAMP_UTC >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1;
"""

QUERY_ALERTS = """
SELECT RULE_NAME, WALLET, METRIC_VALUE, THRESHOLD, STATUS, ALERT_TIME
FROM ALERT_RULES_VIEW
ORDER BY ALERT_TIME DESC
LIMIT 10;
"""

# ======================================================
# LECTURA DE DATOS DESDE SNOWFLAKE
# ======================================================

@st.cache_data(ttl=600)
def run_query(query):
    return conn.cursor().execute(query).fetch_pandas_all()

df_activity = run_query(QUERY_WALLET_ACTIVITY)
df_top5 = run_query(QUERY_TOP5_VOLUME)
df_trends = run_query(QUERY_TX_TRENDS)
df_alerts = run_query(QUERY_ALERTS)

# ======================================================
# LAYOUT DEL DASHBOARD
# ======================================================

st.title("TronScan Monitoring Dashboard")
st.markdown("**Análisis de actividad y alertas de wallets TRON (Snowflake + Streamlit)**")

# --- Sección superior: Métricas clave
col1, col2, col3 = st.columns(3)
col1.metric("Total Wallets Analizadas", len(df_activity))
col2.metric("Total Transacciones (7d)", int(df_activity["TOTAL_TX"].sum()))
col3.metric("Volumen Total (30d)", f"{df_top5['VOLUME'].sum():,.2f}")

st.markdown("---")

# --- Sección de gráficos principales
colA, colB, colC = st.columns((1, 1, 1))

# Wallets Activity (barras)
with colA:
    st.subheader("Wallets Activity (últimos 7 días)")
    st.bar_chart(df_activity.set_index("WALLET")["TOTAL_TX"])

# Top 5 wallets por volumen
with colB:
    st.subheader("Top 5 wallets con mayor volumen (30d)")
    st.bar_chart(df_top5.set_index("OBSERVED_WALLET")["VOLUME"])

# Transaction trends
with colC:
    st.subheader("Tendencia diaria de transacciones (30d)")
    st.line_chart(df_trends.set_index("DATE")["TRANSACTIONS"])

st.markdown("---")

# --- Sección inferior: Tablas detalladas
colT1, colT2 = st.columns(2)

with colT1:
    st.subheader("Wallets Activity (detalle)")
    st.dataframe(df_activity, use_container_width=True)

with colT2:
    st.subheader("Alertas activas")
    if df_alerts.empty:
        st.info("No se detectaron alertas activas en este periodo.")
    else:
        st.dataframe(df_alerts, use_container_width=True)

st.markdown("---")

conn.close()
