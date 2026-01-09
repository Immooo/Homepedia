import os
import sqlite3
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


st.set_page_config(page_title="Homepedia — Temps réel (prix)", layout="wide")

DB_PATH = os.getenv("DB_PATH", os.path.join("data", "homepedia.db"))
TZ_PARIS = ZoneInfo("Europe/Paris")


GEO_LABELS = {
    "IDF": "Île-de-France",
    "PROVINCE": "Province",
    "FR": "France",
}

UNIT_LABELS = {
    "index_base": "Indice (base 100)",
    "pct": "Variation annuelle (%)",
}

SERIES_LABELS = {
    "insee_indices": "INSEE indices",
    "insee_yoy": "INSEE yoy",
}


def _to_dt_paris(series: pd.Series) -> pd.Series:
    """Convertit une colonne ISO timestamp en datetime timezone Europe/Paris."""
    dt_utc = pd.to_datetime(series, utc=True, errors="coerce")
    return dt_utc.dt.tz_convert(TZ_PARIS)


def _fmt_fr(series_dt: pd.Series) -> pd.Series:
    """Format FR lisible: jj/mm/aaaa HH:MM:SS"""
    return series_dt.dt.strftime("%d/%m/%Y %H:%M:%S")


def _parse_metric_uid(metric_uid: str) -> tuple[str, str, str]:
    """
    Retourne (series_key, geo_key, segment_key) à partir d'un uid type:
    - insee_indices:ile_de_france_appartements
    - insee_yoy:france_maisons
    """
    # series_key = avant ":" (insee_indices / insee_yoy)
    if ":" in metric_uid:
        series_key, rest = metric_uid.split(":", 1)
    else:
        series_key, rest = metric_uid, ""

    rest = rest.strip()

    # segment (appartements/maisons/total)
    segment = "total"
    if rest.endswith("_appartements"):
        segment = "appartements"
        rest = rest[: -len("_appartements")]
    elif rest.endswith("_maisons"):
        segment = "maisons"
        rest = rest[: -len("_maisons")]

    # geo_key
    geo_key = rest or "france"
    geo_key_norm = geo_key.lower()

    if geo_key_norm in ("ile_de_france", "idf"):
        geo = "IDF"
    elif geo_key_norm in ("province",):
        geo = "PROVINCE"
    elif geo_key_norm in ("france", "fr"):
        geo = "FR"
    else:
        # fallback : on garde la valeur brute
        geo = geo_key.upper()

    return series_key, geo, segment


def _pretty_metric_label(series_key: str, geo: str, segment: str) -> tuple[str, str]:
    """
    Retourne:
    - serie_label: "INSEE indices" / "INSEE yoy"
    - metric_label: "Île-de-France - appartements" / "France" / etc.
    """
    serie_label = SERIES_LABELS.get(series_key, series_key)

    geo_label = GEO_LABELS.get(geo, geo)

    if geo == "FR" and segment == "total":
        metric_label = "France"
    elif segment == "total":
        metric_label = f"{geo_label}"
    else:
        metric_label = f"{geo_label} - {segment}"

    return serie_label, metric_label


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

    dt_paris = _to_dt_paris(df["scraped_at"])
    df["scraped_at_paris_dt"] = dt_paris
    df["collecte_le"] = _fmt_fr(dt_paris)

    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Normalisation + labels propres
    rows = []
    for _, r in df.iterrows():
        series_key, geo_from_uid, segment = _parse_metric_uid(str(r["metric_uid"]))
        serie_label, metric_label = _pretty_metric_label(
            series_key, geo_from_uid, segment
        )

        unit_label = UNIT_LABELS.get(str(r["unit"]), str(r["unit"]))

        rows.append(
            {
                "Série": serie_label,
                "Métrique": metric_label,
                "Valeur": r["value"],
                "Unité": unit_label,
                "Collecté le (heure Paris)": r["collecte_le"],
                "_metric_uid": r["metric_uid"],
                "_series_key": series_key,
                "_unit": r["unit"],
                "_geo": geo_from_uid,
                "_segment": segment,
                "_collecte_dt": r["scraped_at_paris_dt"],
            }
        )

    out = pd.DataFrame(rows)

    # Format valeur plus joli (sans forcer l’unité dans la string)
    # Indices -> 1 décimale, % -> 1 décimale
    out["Valeur"] = out["Valeur"].round(1)

    return out


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

    dt_paris = _to_dt_paris(df["scraped_at"])
    df["collecte_dt"] = dt_paris
    df["collecte_le"] = _fmt_fr(dt_paris)
    df["value"] = pd.to_numeric(df["value"], errors="coerce").round(1)

    # ordre chronologique pour courbe
    df = df.sort_values("collecte_dt")
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

# KPI dernier scrape (Paris)
last_dt = latest["_collecte_dt"].max()
st.metric(
    "Dernier scrape (heure Paris)",
    last_dt.strftime("%d/%m/%Y %H:%M:%S") if pd.notna(last_dt) else "—",
)

st.subheader("Dernières valeurs (table)")
table_cols = ["Série", "Métrique", "Valeur", "Unité", "Collecté le (heure Paris)"]
st.dataframe(latest[table_cols], use_container_width=True)

st.subheader("Historique (courbe)")

# Select: même logique que ta liste attendue, propre et stable
options = (
    latest[["Série", "Métrique", "_metric_uid", "Unité"]]
    .drop_duplicates()
    .sort_values(["Série", "Métrique"])
    .to_dict("records")
)

selected = st.selectbox(
    "Choisir une métrique",
    options,
    format_func=lambda x: f"{x['Série']} — {x['Métrique']}",
)

metric_uid = selected["_metric_uid"]
unit_label = selected["Unité"]

limit = st.slider(
    "Nombre de points d'historique", min_value=10, max_value=5000, value=200, step=10
)

hist = load_history(metric_uid, limit)

if hist.empty:
    st.info("Pas d'historique pour cette métrique.")
else:
    st.caption(f"Unité : {unit_label}")

    st.dataframe(
        hist[["collecte_le", "value"]]
        .rename(
            columns={
                "collecte_le": "Collecté le (heure Paris)",
                "value": "Valeur",
            }
        )
        .tail(30),
        use_container_width=True,
    )

    # Graph matplotlib (axe temps FR)
    fig, ax = plt.subplots()
    ax.plot(hist["collecte_dt"], hist["value"])
    ax.set_xlabel("Date / heure (Paris)")
    ax.set_ylabel(f"Valeur — {unit_label}")

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m %H:%M"))
    fig.autofmt_xdate(rotation=45)

    st.pyplot(fig, use_container_width=True)
