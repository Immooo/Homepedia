import os
import sqlite3
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Homepedia — Temps réel (prix)", layout="wide")

DB_PATH = os.getenv("DB_PATH", os.path.join("data", "homepedia.db"))

st.title("⏱️ Temps réel — Prix immobilier (scraping INSEE)")
st.caption(
    "Démonstration : scraping + ingestion automatique toutes les X minutes (polling)."
)


@st.cache_data(ttl=10, show_spinner=False)
def load_latest() -> pd.DataFrame:
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        """
        SELECT metric_uid, metric_name, geo, unit, period, value, scraped_at
        FROM realtime_price_latest
        ORDER BY metric_uid
        """,
        con,
    )
    con.close()
    return df


@st.cache_data(ttl=10, show_spinner=False)
def load_history(metric_uid: str, limit: int) -> pd.DataFrame:
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        """
        SELECT scraped_at, period, value
        FROM realtime_price_history
        WHERE metric_uid = ?
        ORDER BY scraped_at DESC
        LIMIT ?
        """,
        con,
        params=(metric_uid, limit),
    )
    con.close()
    if df.empty:
        return df
    return df.sort_values("scraped_at")


latest = load_latest()

if latest.empty:
    st.warning(
        "Aucune donnée temps réel pour le moment. Lance le worker `realtime-price` ou un `scrape_once`."
    )
    st.stop()

col1, col2, col3 = st.columns(3)
col1.metric("Métriques suivies", str(len(latest)))
col2.metric("Dernier scrape (UTC)", str(latest["scraped_at"].max()))
col3.metric("Période INSEE (dernier trimestre)", str(latest["period"].iloc[0]))

st.subheader("Dernières valeurs (table)")
st.dataframe(latest, use_container_width=True)

st.subheader("Historique (courbe)")
metric_uid = st.selectbox("Choisir une métrique", latest["metric_uid"].tolist())
limit = st.slider(
    "Nombre de points d'historique", min_value=10, max_value=2000, value=200, step=10
)

hist = load_history(metric_uid, limit)

if hist.empty:
    st.info("Pas d'historique pour cette métrique.")
else:
    st.dataframe(hist.tail(30), use_container_width=True)
    st.line_chart(hist.set_index("scraped_at")["value"])
