import os
import sqlite3
import hashlib
import math
from dataclasses import dataclass
from datetime import datetime, UTC
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

try:
    import altair as alt
except Exception:
    alt = None

try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None


PARIS_TZ = ZoneInfo("Europe/Paris")

DB_PATH = os.getenv("DB_PATH", "/app/data/homepedia.db")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))

# UI refresh (par défaut = fréquence de collecte)
UI_REFRESH_SECONDS = int(
    os.getenv("REALTIME_UI_REFRESH_SECONDS", str(POLL_INTERVAL_SECONDS))
)
DEFAULT_HISTORY_LIMIT = int(os.getenv("REALTIME_UI_HISTORY_LIMIT", "300"))

# Mock UI (démo): fait "bouger" la courbe sans modifier les données stockées
DEFAULT_UI_MOCK = os.getenv("REALTIME_UI_MOCK", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)

# Par défaut on met plus fort pour que ce soit VISIBLE
MOCK_JITTER_INDEX_DEFAULT = float(
    os.getenv("REALTIME_UI_MOCK_JITTER_INDEX", "3.0")
)  # indices base 100
MOCK_JITTER_YOY_DEFAULT = float(
    os.getenv("REALTIME_UI_MOCK_JITTER_YOY", "1.0")
)  # variation annuelle %


def _parse_iso_to_paris_dt(iso_str: str) -> datetime:
    if not iso_str:
        return datetime.now(PARIS_TZ)
    s = iso_str.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(PARIS_TZ)


def _to_py_dt(x):
    return x.to_pydatetime() if hasattr(x, "to_pydatetime") else x


def _fmt_dt_paris(x) -> str:
    dt = _to_py_dt(x)
    return dt.astimezone(PARIS_TZ).strftime("%d/%m/%Y %H:%M:%S")


def _human_delta(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds} s"
    minutes, s = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes} min {s:02d} s"
    hours, m = divmod(minutes, 60)
    if hours < 24:
        return f"{hours} h {m:02d} min"
    days, h = divmod(hours, 24)
    return f"{days} j {h:02d} h"


@dataclass(frozen=True)
class MetricInfo:
    metric_uid: str
    family: str  # 'indices' | 'yoy' | 'autre'
    zone: str
    type_bien: str  # 'appartements'|'maisons'|'' (ensemble)
    label: str  # label "selectbox"
    unit_text: str  # pour le graphe
    value_header: str  # pour le tableau
    jitter_default: float


def _clean_words(s: str) -> str:
    return s.replace("_", " ").strip()


def _metric_info(metric_uid: str) -> MetricInfo:
    family = "autre"
    zone = ""
    type_bien = ""
    label = metric_uid
    unit_text = "Valeur"
    value_header = "Valeur"
    jitter_default = 0.2

    if ":" in metric_uid:
        prefix, rest = metric_uid.split(":", 1)
    else:
        prefix, rest = metric_uid, ""

    if prefix == "insee_indices":
        family = "indices"
        unit_text = "Indice (base 100)"
        value_header = "Valeur (indice base 100)"
        jitter_default = MOCK_JITTER_INDEX_DEFAULT

        if rest.startswith("ile_de_france_"):
            zone = "Île-de-France"
            type_bien = _clean_words(rest[len("ile_de_france_") :])
        elif rest.startswith("province_"):
            zone = "Province"
            type_bien = _clean_words(rest[len("province_") :])
        else:
            zone = _clean_words(rest)
            type_bien = ""

        label = f"INSEE indices — {zone}" + (f" - {type_bien}" if type_bien else "")

    elif prefix == "insee_yoy":
        family = "yoy"
        unit_text = "Variation annuelle (%)"
        value_header = "Valeur (variation annuelle %)"
        jitter_default = MOCK_JITTER_YOY_DEFAULT

        if rest == "france":
            zone = "France"
            type_bien = ""
        elif rest.startswith("france_"):
            zone = "France"
            type_bien = _clean_words(rest[len("france_") :])
        else:
            zone = _clean_words(rest)
            type_bien = ""

        label = f"INSEE yoy — {zone}" + (f" - {type_bien}" if type_bien else "")

    return MetricInfo(
        metric_uid=metric_uid,
        family=family,
        zone=zone,
        type_bien=type_bien,
        label=label,
        unit_text=unit_text,
        value_header=value_header,
        jitter_default=jitter_default,
    )


def _stable_noise(key: str) -> float:
    # bruit stable dans [-1 ; +1]
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    n = int(h[:8], 16) / 0xFFFFFFFF
    return (n - 0.5) * 2.0


def _read_sqlite(query: str, params: tuple = ()) -> pd.DataFrame:
    con = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(query, con, params=params)
    finally:
        con.close()
    return df


def _load_latest() -> pd.DataFrame:
    df = _read_sqlite(
        """
        SELECT metric_uid, metric_name, geo, unit, period, value, scraped_at
        FROM realtime_price_latest
        """
    )
    if df.empty:
        return df

    df["scraped_at_paris"] = pd.to_datetime(
        df["scraped_at"].apply(lambda s: _parse_iso_to_paris_dt(s).isoformat())
    )

    infos = df["metric_uid"].apply(_metric_info)
    df["famille"] = infos.apply(lambda x: x.family)
    df["zone"] = infos.apply(lambda x: x.zone)
    df["type_bien"] = infos.apply(lambda x: x.type_bien if x.type_bien else "ensemble")
    df["label"] = infos.apply(lambda x: x.label)
    df["unit_text"] = infos.apply(lambda x: x.unit_text)
    df["value_header"] = infos.apply(lambda x: x.value_header)
    df["jitter_default"] = infos.apply(lambda x: x.jitter_default)

    return df


def _load_history(metric_uid: str, limit: int) -> pd.DataFrame:
    df = _read_sqlite(
        """
        SELECT metric_uid, period, value, scraped_at
        FROM realtime_price_history
        WHERE metric_uid = ?
        ORDER BY scraped_at DESC
        LIMIT ?
        """,
        (metric_uid, limit),
    )
    if df.empty:
        return df

    df["scraped_at_paris"] = pd.to_datetime(
        df["scraped_at"].apply(lambda s: _parse_iso_to_paris_dt(s).isoformat())
    )
    return df


# =========================
# UI
# =========================
st.header("📡 Temps réel — Prix immobiliers (INSEE)")
st.caption("Affichage en **heure de Paris** (Europe/Paris).")

if st_autorefresh:
    st_autorefresh(interval=UI_REFRESH_SECONDS * 1000, key="rt_price_refresh")

latest = _load_latest()
if latest.empty:
    st.warning(
        "Aucune donnée trouvée. Lance un run (`make rt-scrape-now`) ou démarre le worker (`make rt-up`)."
    )
    st.stop()

last_dt = latest["scraped_at_paris"].max()
now_dt = datetime.now(PARIS_TZ)

periods = sorted(set(latest["period"].dropna().astype(str).tolist()))
period_txt = periods[-1] if periods else "n/a"

c1, c2, c3, c4 = st.columns(4)
c1.metric("Fréquence de collecte", f"{POLL_INTERVAL_SECONDS} s")
c2.metric("Dernière collecte (Paris)", _fmt_dt_paris(last_dt))
c3.metric(
    "Dernier run",
    f"il y a {_human_delta((now_dt - _to_py_dt(last_dt)).total_seconds())}",
)
c4.metric("Période INSEE (dernière)", period_txt)

st.divider()

st.subheader("Dernières valeurs")

indices_df = latest[latest["famille"] == "indices"].copy()
yoy_df = latest[latest["famille"] == "yoy"].copy()


def _prep_table(df: pd.DataFrame, value_header: str) -> pd.DataFrame:
    out = df[["zone", "type_bien", "value"]].copy()
    out.rename(
        columns={"zone": "Zone", "type_bien": "Type de bien", "value": value_header},
        inplace=True,
    )
    return out.sort_values(["Zone", "Type de bien"])


if not indices_df.empty:
    st.markdown("**INSEE indices** — *Indice (base 100)*")
    st.dataframe(
        _prep_table(indices_df, "Valeur (indice base 100)"),
        hide_index=True,
        use_container_width=True,
    )
else:
    st.info("Aucune donnée 'INSEE indices'.")

if not yoy_df.empty:
    st.markdown("**INSEE yoy** — *Variation annuelle (%)*")
    st.dataframe(
        _prep_table(yoy_df, "Valeur (variation annuelle %)"),
        hide_index=True,
        use_container_width=True,
    )
else:
    st.info("Aucune donnée 'INSEE yoy'.")

st.divider()

st.subheader("Historique (courbe + tableau)")

labels = sorted(latest["label"].unique().tolist())
label_to_uid = {
    _metric_info(uid).label: uid for uid in latest["metric_uid"].unique().tolist()
}

preferred_order = [
    "INSEE indices — Province - appartements",
    "INSEE indices — Île-de-France - appartements",
    "INSEE indices — Province - maisons",
    "INSEE indices — Île-de-France - maisons",
    "INSEE yoy — France",
    "INSEE yoy — France - appartements",
    "INSEE yoy — France - maisons",
]
ordered = [x for x in preferred_order if x in labels] + [
    x for x in labels if x not in preferred_order
]

selected_label = st.selectbox("Choisir une métrique", options=ordered, index=0)
selected_uid = label_to_uid.get(selected_label, latest["metric_uid"].iloc[0])
info = _metric_info(selected_uid)

left, right = st.columns([1, 1])
with left:
    limit = st.slider(
        "Nombre de points affichés",
        min_value=30,
        max_value=600,
        value=min(DEFAULT_HISTORY_LIMIT, 600),
        step=10,
    )
with right:
    ui_mock = st.checkbox(
        "Mode mock : courbe animée (démo)",
        value=DEFAULT_UI_MOCK,
        help="Ajoute une variation simulée visible sur la courbe (sans modifier la DB).",
    )

hist_desc = _load_history(selected_uid, limit=limit)
if hist_desc.empty:
    st.info("Pas encore d'historique pour cette métrique.")
    st.stop()

# Table doit être du plus récent au plus ancien
hist_desc = hist_desc.sort_values("scraped_at_paris", ascending=False).copy()

# Graph doit être chronologique (ancien -> récent)
hist_asc = hist_desc.sort_values("scraped_at_paris", ascending=True).copy()

# Mock visible : jitter + légère oscillation dépendante du temps (pour un rendu vivant)
# L’oscillation est identique pour tout le monde, le jitter dépend du point (scraped_at)
jitter_default = info.jitter_default
if ui_mock:
    max_amp = 10.0 if info.family == "indices" else 3.0
    amp = st.slider(
        "Intensité du mock (plus = plus visible)",
        min_value=0.0,
        max_value=max_amp,
        value=float(jitter_default),
        step=0.1,
    )
else:
    amp = 0.0

now_ts = datetime.now(PARIS_TZ).timestamp()
wave = math.sin(now_ts / 5.0) * (
    amp * 0.25
)  # petit mouvement global pour que ça “vive”


def _mock_value(
    metric_uid: str, scraped_at: str, base: float, amplitude: float
) -> float:
    if amplitude <= 0:
        return base
    n = _stable_noise(f"{metric_uid}|{scraped_at}")
    return base + (n * amplitude) + wave


hist_asc["valeur_affichee"] = hist_asc.apply(
    lambda r: (
        _mock_value(selected_uid, str(r["scraped_at"]), float(r["value"]), amp)
        if ui_mock
        else float(r["value"])
    ),
    axis=1,
)

# --------- Graph (chronologique)
plot_df = hist_asc[["scraped_at_paris", "value", "valeur_affichee"]].copy()
plot_df["Heure (Paris)"] = plot_df["scraped_at_paris"].dt.strftime("%H:%M:%S")
plot_df["Date"] = plot_df["scraped_at_paris"].dt.strftime("%d/%m/%Y")
plot_df.rename(
    columns={"valeur_affichee": "Valeur affichée", "value": "Valeur brute"},
    inplace=True,
)

chart_title = f"{selected_label} — {info.unit_text}"

if alt is not None:
    base = alt.Chart(plot_df).encode(
        x=alt.X(
            "scraped_at_paris:T",
            title="Heure de collecte (Paris)",
            axis=alt.Axis(format="%H:%M:%S"),
        ),
        y=alt.Y("Valeur affichée:Q", title=info.unit_text),
        tooltip=[
            alt.Tooltip("Date:N"),
            alt.Tooltip("Heure (Paris):N"),
            alt.Tooltip("Valeur brute:Q", format=".3f"),
            alt.Tooltip("Valeur affichée:Q", format=".3f"),
        ],
    )
    st.altair_chart(
        (base.mark_line() + base.mark_circle(size=30)).properties(
            title=chart_title, height=320
        ),
        use_container_width=True,
    )
else:
    st.line_chart(plot_df.set_index("scraped_at_paris")[["Valeur affichée"]])

st.caption(
    f"Dernier point : {_fmt_dt_paris(hist_desc['scraped_at_paris'].max())} — heure de Paris."
)

# --------- Tableau (plus récent -> plus ancien)
st.markdown("**Historique (du plus récent au plus ancien)**")

table_df = hist_desc[["scraped_at_paris", "value"]].copy()
table_df["Date/heure (Paris)"] = table_df["scraped_at_paris"].apply(_fmt_dt_paris)
table_df.rename(columns={"value": info.value_header}, inplace=True)
table_df = table_df[["Date/heure (Paris)", info.value_header]]

st.dataframe(table_df, hide_index=True, use_container_width=True)
