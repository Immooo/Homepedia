import os
import sqlite3
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Homepedia — Temps réel (prix)", layout="wide")

DB_PATH = os.getenv("DB_PATH", os.path.join("data", "homepedia.db"))
TZ_PARIS = ZoneInfo("Europe/Paris")

COLS_FR_LATEST = {
    "metric_uid": "Identifiant métrique",
    "metric_name": "Nom de la métrique",
    "geo": "Zone",
    "unit": "Unité",
    "period": "Période",
    "value": "Valeur",
    "scraped_at_local": "Collecté le (heure Paris)",
    "scraped_at_utc": "Collecté le (UTC)",
}

COLS_FR_HISTORY = {
    "scraped_at_local": "Collecté le (heure Paris)",
    "scraped_at_utc": "Collecté le (UTC)",
    "period": "Période",
    "value": "Valeur",
}


def _to_dt_utc(series: pd.Series) -> pd.Series:
    """
    Convertit une colonne scraped_at (ISO avec offset, ex: ...+00:00) en datetime UTC.
    """
    # utc=True force un tz-aware en UTC, même si la string contient un offset
    return pd.to_datetime(series, utc=True, errors="coerce")


def _format_dt(series: pd.Series) -> pd.Series:
    """
    Format FR lisible: jj/mm/aaaa HH:MM:SS
    """
    return series.dt.strftime("%d/%m/%Y %H:%M:%S")


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

    if df.empty:
        return df

    dt_utc = _to_dt_utc(df["scraped_at"])
    dt_paris = dt_utc.dt.tz_convert(TZ_PARIS)

    df["scraped_at_utc"] = _format_dt(dt_utc)
    df["scraped_at_local"] = _format_dt(dt_paris)

    # Affichage plus propre
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
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

    dt_utc = _to_dt_utc(df["scraped_at"])
    dt_paris = dt_utc.dt.tz_convert(TZ_PARIS)

    df["scraped_at_utc"] = _format_dt(dt_utc)
    df["scraped_at_local"] = _format_dt(dt_paris)

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    # Pour le graphique : trier du plus ancien au plus récent
    df = df.sort_values("scraped_at_local")
    return df


st.title("⏱️ Temps réel — Prix immobilier (scraping INSEE)")
st.caption(
    "Démonstration : scraping + ingestion automatique toutes les X minutes (polling)."
)

latest = load_latest()
if latest.empty:
    st.warning(
        "Aucune donnée temps réel pour le moment. Lance le worker `realtime-price` ou `rt-scrape-now`."
    )
    st.stop()

# ---- KPIs ----
col1, col2, col3 = st.columns(3)
col1.metric("Métriques suivies", str(len(latest)))

last_paris = latest["scraped_at_local"].max()
last_utc = latest["scraped_at_utc"].max()
col2.metric("Dernier scrape (heure Paris)", str(last_paris))
col3.metric("Dernier scrape (UTC)", str(last_utc))

# ---- Table latest ----
st.subheader("Dernières valeurs (table)")

show_utc = st.toggle("Afficher aussi la colonne UTC", value=False)

latest_display = latest.copy()

# Garder les colonnes utiles + ordre
cols = [
    "metric_uid",
    "metric_name",
    "geo",
    "unit",
    "period",
    "value",
    "scraped_at_local",
]
if show_utc:
    cols.append("scraped_at_utc")

latest_display = latest_display[cols].rename(columns=COLS_FR_LATEST)

st.dataframe(latest_display, use_container_width=True)

# ---- Historique ----
st.subheader("Historique (courbe)")

# selectbox lisible : afficher metric_name (uid en petit)
options = (
    latest[["metric_uid", "metric_name"]]
    .drop_duplicates()
    .sort_values(["metric_name", "metric_uid"])
    .to_dict("records")
)

selected = st.selectbox(
    "Choisir une métrique",
    options,
    format_func=lambda x: f"{x['metric_name']}  —  {x['metric_uid']}",
)

metric_uid = selected["metric_uid"]

limit = st.slider(
    "Nombre de points d'historique", min_value=10, max_value=5000, value=200, step=10
)

hist = load_history(metric_uid, limit)

if hist.empty:
    st.info("Pas d'historique pour cette métrique.")
else:
    hist_display = hist.copy()
    cols_h = ["scraped_at_local", "period", "value"]
    if show_utc:
        cols_h.insert(1, "scraped_at_utc")

    st.dataframe(
        hist_display[cols_h].rename(columns=COLS_FR_HISTORY).tail(30),
        use_container_width=True,
    )

    # Graph : axe temps en heure Paris
    chart_df = hist[["scraped_at_local", "value"]].copy()
    # Reconvertir "scraped_at_local" string -> datetime pour un axe propre
    chart_df["scraped_at_local"] = pd.to_datetime(
        chart_df["scraped_at_local"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
    )
    chart_df = chart_df.dropna(subset=["scraped_at_local"]).set_index(
        "scraped_at_local"
    )

    st.line_chart(chart_df["value"])
