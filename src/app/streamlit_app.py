import os
import math
import random
import sqlite3
from typing import Any

import numpy as np
import pandas as pd
import streamlit as st
import geopandas as gpd
import folium
from streamlit_folium import st_folium
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from scipy.stats import linregress
from textblob import TextBlob
from wordcloud import WordCloud
from tinydb import TinyDB

# -----------------------------
# Config générale
# -----------------------------
st.set_page_config(page_title="Homepedia – Analyses Immobilier France", layout="wide")
st.title("🏠 Homepedia – Analyses Immobilier France")

DB_PATH = os.getenv("DB_PATH", "data/homepedia.db")

COLS_NICE = {
    "code": "Département",
    "dept": "Département",
    "code_region": "Région",
    "nb_transactions": "Nombres de transactions",
    "prix_m2_moyen": "Prix moyen €/m²",
    "prix_m2": "Prix €/m²",
    "surface_reelle_bati": "Surface bâtie m²",
    "valeur_fonciere": "Valeur foncière €",
    "population": "Population",
    "income_median": "Revenu médian €",
    "taux_chomage": "Taux chômage %",
    "poverty_rate": "Taux pauvreté %",
    "type_local": "Type de bien",
    "code_postal": "Code postal",
    "nature_mutation": "Nature de la mutation",
    "nombre_pieces_principales": "Nombres de pièces principales",
}

def pretty(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={k: v for k, v in COLS_NICE.items() if k in df.columns})

def show(df: pd.DataFrame, n: int | None = None):
    st.dataframe(pretty(df if n is None else df.head(n)), use_container_width=True)

# -----------------------------
# Connexion SQLite persistante
# -----------------------------
@st.cache_resource
def get_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH, check_same_thread=False)

conn = get_conn()

# -----------------------------
# Choix de la vue
# -----------------------------
view = st.sidebar.radio(
    "Choix de la vue",
    ["Standard", "Spark Analysis", "Text Analysis", "Indicateurs Socio-éco", "Région", "Méthodologie"],
)

# -----------------------------
# VUE STANDARD
# -----------------------------
if view == "Standard":
    st.header("Transactions immobilières (live SQL + Pandas)")

    def sql_scalar(query: str) -> Any:
        return conn.execute(query).fetchone()[0]

    # Période
    st.sidebar.subheader("Filtres Transactions")
    min_date = pd.to_datetime("2024-01-01")
    max_date = pd.to_datetime("2024-12-31")

    raw_dates = st.sidebar.date_input(
        "Période",
        (min_date.date(), max_date.date()),
        min_value=min_date.date(),
        max_value=max_date.date(),
    )

    if isinstance(raw_dates, (list, tuple)) and len(raw_dates) == 2:
        start_date, end_date = map(pd.to_datetime, raw_dates)
    else:
        d = raw_dates[0] if isinstance(raw_dates, (list, tuple)) and raw_dates else raw_dates
        start_date = end_date = pd.to_datetime(d)

    # Types de bien
    types = conn.execute(
        "SELECT DISTINCT type_local FROM transactions WHERE type_local IS NOT NULL ORDER BY 1"
    ).fetchall()
    type_list = ["Tous"] + [r[0] for r in types]
    choix_type = st.sidebar.selectbox("Type de logement", type_list)

    # Prix min/max globaux
    pmin_glob, pmax_glob = conn.execute(
        """
        SELECT
            MIN(valeur_fonciere / NULLIF(surface_reelle_bati,0)),
            MAX(valeur_fonciere / NULLIF(surface_reelle_bati,0))
        FROM transactions
        WHERE valeur_fonciere IS NOT NULL AND surface_reelle_bati > 0
        """
    ).fetchone()

    pmin_glob = int(pmin_glob or 0)
    pmax_glob = int(pmax_glob or 10000)
    price_range = st.sidebar.slider("Prix au m²", pmin_glob, pmax_glob, (pmin_glob, pmax_glob))

    @st.cache_data(show_spinner=False)
    def load_transactions(start, end, type_sel, pmin, pmax) -> pd.DataFrame:
        start_iso = pd.to_datetime(start).strftime("%Y-%m-%d")
        end_iso = pd.to_datetime(end).strftime("%Y-%m-%d")

        query = """
            SELECT *,
                   ROUND(valeur_fonciere / NULLIF(surface_reelle_bati,0), 0) AS prix_m2,
                   SUBSTR(CAST(code_postal AS TEXT),1,2) AS dept
            FROM transactions
            WHERE date_mutation BETWEEN ? AND ?
              AND surface_reelle_bati > 0
              AND valeur_fonciere IS NOT NULL
              AND (valeur_fonciere / NULLIF(surface_reelle_bati,0)) BETWEEN ? AND ?
        """
        params: list[Any] = [start_iso, end_iso, pmin, pmax]
        if type_sel != "Tous":
            query += " AND type_local = ?"
            params.append(type_sel)

        df = pd.read_sql_query(query, conn, params=params, parse_dates=["date_mutation"])
        if "date_mutation" in df.columns:
            df["date_mutation"] = df["date_mutation"].dt.date

        if "valeur_fonciere" in df.columns:
            df["valeur_fonciere"] = df["valeur_fonciere"].round(0).astype(int, errors="ignore")
        if "prix_m2" in df.columns:
            df["prix_m2"] = pd.to_numeric(df["prix_m2"], errors="coerce").astype("Int64")

        if "code_postal" in df.columns:
            df["code_postal"] = (
                df["code_postal"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(5)
            )
        if "dept" not in df.columns and "code_postal" in df.columns:
            df["dept"] = df["code_postal"].str[:2]
        return df

    tx = load_transactions(start_date, end_date, choix_type, price_range[0], price_range[1])

    # KPIs & export
    c1, c2, c3 = st.columns(3)
    c1.metric("Transactions chargées", f"{len(tx):,}")
    c2.metric("Surface médiane (m²)", f"{tx['surface_reelle_bati'].median():.1f}" if len(tx) else "—")
    c3.metric("Prix moyen €/m²", f"{tx['prix_m2'].mean():.2f}" if len(tx) else "—")

    st.download_button(
        "📥 Exporter ces transactions (CSV)",
        tx.to_csv(index=False).encode("utf-8"),
        file_name="transactions_filtrees.csv",
        mime="text/csv",
        disabled=(len(tx) == 0),
    )

    st.subheader("Aperçu des transactions filtrées")
    if len(tx):
        view_tx = (
            tx.drop_duplicates(subset=["id", "valeur_fonciere", "surface_reelle_bati"], keep="first")
            .sample(n=min(10, len(tx)), random_state=42)
            .sort_values("valeur_fonciere", ascending=False)
        )
        show(view_tx)
    else:
        st.info("Aucune transaction ne correspond aux filtres sélectionnés.")

    # Carte choroplèthe
    if len(tx) and st.checkbox("Afficher la carte", value=True):
        prix_dept = (
            tx.dropna(subset=["dept", "prix_m2"])
            .groupby("dept")["prix_m2"]
            .mean()
            .reset_index()
            .rename(columns={"dept": "code", "prix_m2": "prix_m2_moyen"})
        )
        try:
            geo = gpd.read_file("data/raw/geo/departements_simplifie.geojson")[["code", "geometry"]]
            geo = geo.merge(prix_dept, on="code", how="left")
            with st.spinner("Création carte …"):
                m = folium.Map(location=[46.6, 2.4], zoom_start=5)
                folium.Choropleth(
                    geo_data=geo,
                    data=geo,
                    columns=["code", "prix_m2_moyen"],
                    key_on="feature.properties.code",
                    legend_name="Prix moyen (€ / m²)",
                    fill_opacity=0.7,
                    line_opacity=0.2,
                    nan_fill_color="white",
                ).add_to(m)
                st.subheader("Carte du prix moyen au m²")
                st_folium(m, width=800, height=600)
        except Exception as e:
            st.warning(f"GeoJSON manquant ou invalide : {e}")

    # Histogramme prix/m²
    st.subheader("Distribution des prix au m²")
    if len(tx):
        fig1, ax1 = plt.subplots()
        ax1.hist(tx["prix_m2"].dropna(), bins="auto", range=price_range, edgecolor="black")
        ax1.set_xlim(price_range)
        ax1.set_xlabel("Prix (€ / m²)")
        ax1.set_ylabel("Nombre de transactions")
        st.pyplot(fig1, use_container_width=True)
    else:
        st.info("Graphique non disponible : 0 transaction après filtres.")

    # Box-plot par type
    st.subheader("Dispersion prix/m² par type de bien")
    if len(tx):
        fig_box, ax_box = plt.subplots(figsize=(9, 4))
        try:
            tx.boxplot(column="prix_m2", by="type_local", ax=ax_box, showfliers=False)
            ax_box.set_xlabel("")
            ax_box.set_ylabel("€ / m²")
            ax_box.set_title("")
            fig_box.suptitle("")
            ax_box.tick_params(axis="x", labelrotation=45)
            ax_box.set_xticklabels(
                [lab.get_text().replace(" ", "\n", 1) for lab in ax_box.get_xticklabels()],
                ha="right",
                fontsize=8,
            )
            st.pyplot(fig_box)
        except Exception:
            st.info("Impossible de tracer le box-plot (données insuffisantes).")

    # Scatter Population vs Prix
    try:
        pop = pd.read_sql_query("SELECT * FROM population", conn)
        prix_dept = (
            tx.dropna(subset=["dept", "prix_m2"])
            .groupby("dept")["prix_m2"]
            .mean()
            .reset_index()
            .rename(columns={"dept": "code", "prix_m2": "prix_m2_moyen"})
        )
        prix_pop = prix_dept.merge(pop, on="code", how="left")
        st.subheader("Population vs Prix moyen")
        fig2, ax2 = plt.subplots()
        ax2.scatter(prix_pop["population"], prix_pop["prix_m2_moyen"], alpha=0.6)
        ax2.set_xlabel("Population départementale")
        ax2.set_ylabel("Prix moyen (€ / m²)")
        ax2.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f} M"))
        st.pyplot(fig2)
    except Exception:
        st.info("Données population manquantes pour le scatter.")

# -----------------------------
# VUE SPARK ANALYSIS
# -----------------------------
elif view == "Spark Analysis":
    st.header("Vue Spark Analysis (pré-agrégation)")
    try:
        df_spark = pd.read_sql_query(
            "SELECT dept AS code, nb_transactions, prix_m2_moyen FROM spark_dept_analysis",
            conn,
        )
        df_spark["prix_m2_moyen"] = df_spark["prix_m2_moyen"].round(0)
        st.subheader("Résultats Spark par département")
        show(df_spark)
        per_page = st.sidebar.slider("Dépts par page", 5, 50, 10, 5)
        n_pages = max(1, math.ceil(len(df_spark) / per_page))
        page = st.sidebar.number_input("Page", 1, n_pages, 1)
        start = (page - 1) * per_page
        df_page = df_spark.iloc[start : start + per_page]
        st.subheader(f"Page {page}/{n_pages}")
        show(df_page)
        fig3, ax3 = plt.subplots()
        df_page.set_index("code")["prix_m2_moyen"].plot.bar(ax=ax3)
        ax3.set_xlabel("Département")
        ax3.set_ylabel("Prix moyen (€ / m²)")
        ax3.tick_params(axis="x", rotation=45)
        st.pyplot(fig3)
    except Exception:
        st.info("Table spark_dept_analysis introuvable.")

# -----------------------------
# VUE TEXT ANALYSIS
# -----------------------------
elif view == "Text Analysis":
    st.header("Vue Text Analysis (Sentiment & Word Cloud)")
    tdb_path = os.path.join("data", "processed", "comments.json")
    if not os.path.exists(tdb_path):
        st.error(f"Base NoSQL manquante : {tdb_path}")
        st.stop()
    db = TinyDB(tdb_path)
    docs = db.all()
    st.sidebar.markdown(f"**Total commentaires :** {len(docs):,}")
    per_page = st.sidebar.slider("Avis par page", 10, 200, 50, 10)
    n_pages = max(1, math.ceil(len(docs) / per_page))
    page = st.sidebar.number_input("Page", 1, n_pages, 1)
    subset = docs[(page - 1) * per_page : page * per_page]
    df_page = pd.DataFrame(subset)
    st.subheader(f"Commentaires (page {page})")
    show(df_page)
    if "commentaire" in df_page.columns:
        df_page["sentiment"] = df_page["commentaire"].map(
            lambda t: float(TextBlob(t).sentiment.polarity)  # type: ignore[attr-defined]
        )
        st.subheader("Sentiment des avis")
        st.bar_chart(df_page["sentiment"])
        sample_n = st.sidebar.slider("Échantillon Word Cloud", 100, min(5000, len(docs)), min(1000, len(docs)), 100)
        sampled = random.sample(docs, min(sample_n, len(docs)))
        text = " ".join(d.get("commentaire", "") for d in sampled)
        wc = WordCloud(width=800, height=400, background_color="white").generate(text)
        fig_wc, ax_wc = plt.subplots(figsize=(10, 5))
        ax_wc.imshow(wc, interpolation="bilinear")
        ax_wc.axis("off")
        st.subheader(f"Word Cloud (n={len(sampled)})")
        st.pyplot(fig_wc)

# -----------------------------
# VUE SOCIO-ÉCO
# -----------------------------
elif view == "Indicateurs Socio-éco":
    st.header("📊 Indicateurs Socio-économiques (INSEE)")

    unemployment_path = "data/processed/unemployment_dept.parquet"
    income_path = "data/processed/income_dept.parquet"
    population_path = "data/processed/population_dept.parquet"
    poverty_path = "data/processed/poverty_dept.parquet"
    geojson_path = "data/raw/geo/departements_simplifie.geojson"

    for path, name in [
        (unemployment_path, "chômage"),
        (income_path, "revenu médian"),
        (population_path, "population"),
        (poverty_path, "pauvreté"),
    ]:
        if not os.path.exists(path):
            st.error(f"Données {name} manquantes ({path}).")
            st.stop()

    @st.cache_data(show_spinner=False)
    def load_df(path: str) -> pd.DataFrame:
        return pd.read_parquet(path)

    @st.cache_data(show_spinner=False)
    def load_geo(path: str):
        return gpd.read_file(path)[["code", "geometry"]]

    df_chom = load_df(unemployment_path)
    df_inc = load_df(income_path)
    df_pop = load_df(population_path)
    df_pov = load_df(poverty_path)
    geo = load_geo(geojson_path)

    # cast chômage
    if "taux_chomage" in df_chom.columns:
        df_chom["taux_chomage"] = pd.to_numeric(
            df_chom["taux_chomage"].astype(str).str.replace(",", "."), errors="coerce"
        )

    # slider chômage
    min_c = float(df_chom["taux_chomage"].min())
    max_c = float(df_chom["taux_chomage"].max())
    min_c, max_c = st.sidebar.slider("Taux de chômage (%)", min_c, max_c, (min_c, max_c))
    df_chom = df_chom.query("@min_c <= taux_chomage <= @max_c")

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
        ["Chômage", "Revenu médian", "Population", "Pauvreté", "Corrélation", "Matrice corrélations"]
    )

    # Chômage
    with tab1:
        st.subheader("Taux de chômage (T1 2025)")
        show(df_chom)
        geo1 = geo.merge(df_chom, on="code", how="left")
        m1 = folium.Map(location=[46.6, 2.4], zoom_start=5)
        folium.Choropleth(
            geo_data=geo1, data=geo1, columns=["code", "taux_chomage"], key_on="feature.properties.code", legend_name="Taux de chômage (%)"
        ).add_to(m1)
        st_folium(m1, width=800, height=600)

    # Revenu médian
    with tab2:
        st.subheader("Revenu médian (2021)")
        show(df_inc)
        geo2 = geo.merge(df_inc, on="code", how="left")
        m2 = folium.Map(location=[46.6, 2.4], zoom_start=5)
        folium.Choropleth(
            geo_data=geo2, data=geo2, columns=["code", "income_median"], key_on="feature.properties.code", legend_name="Revenu médian (€ / an)"
        ).add_to(m2)
        st_folium(m2, width=800, height=600)

    # Population
    with tab3:
        st.subheader("Population")
        show(df_pop)
        geo3 = geo.merge(df_pop, on="code", how="left")
        m3 = folium.Map(location=[46.6, 2.4], zoom_start=5)
        folium.Choropleth(
            geo_data=geo3, data=geo3, columns=["code", "population"], key_on="feature.properties.code", legend_name="Population"
        ).add_to(m3)
        st_folium(m3, width=800, height=600)
        fig3, ax3 = plt.subplots()
        ax3.hist(df_pop["population"].dropna(), bins=30, edgecolor="black")
        ax3.set_xlabel("Population")
        ax3.set_ylabel("Nombre de départements")
        ax3.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, pos: f"{x/1e6:.1f} M"))
        st.pyplot(fig3)

    # Pauvreté
    with tab4:
        st.subheader("Taux de pauvreté")
        show(df_pov)
        geo4 = geo.merge(df_pov, on="code", how="left")
        m4 = folium.Map(location=[46.6, 2.4], zoom_start=5)
        folium.Choropleth(
            geo_data=geo4, data=geo4, columns=["code", "poverty_rate"], key_on="feature.properties.code", legend_name="Taux de pauvreté (%)"
        ).add_to(m4)
        st_folium(m4, width=800, height=600)
        fig4, ax4 = plt.subplots()
        ax4.hist(df_pov["poverty_rate"].dropna(), bins=30, edgecolor="black")
        ax4.set_xlabel("Taux de pauvreté (%)")
        ax4.set_ylabel("Nombre de départements")
        st.pyplot(fig4)

    # Corrélation
    with tab5:
        st.subheader("Corrélation chômage ↔ revenu")
        df_corr = df_chom.merge(df_inc, on="code", how="inner")
        if not df_corr.empty:
            fig5, ax5 = plt.subplots()
            ax5.scatter(df_corr["income_median"], df_corr["taux_chomage"], alpha=0.7)
            slope, intercept, r, p, se = linregress(df_corr["income_median"], df_corr["taux_chomage"])
            xx = np.linspace(df_corr["income_median"].min(), df_corr["income_median"].max(), 100)
            ax5.plot(xx, intercept + slope * xx, linestyle="--", label=f"R²={r**2:.2f}")
            ax5.set_xlabel("Revenu médian (€ / an)")
            ax5.set_ylabel("Taux de chômage (%)")
            ax5.legend()
            st.pyplot(fig5)
        else:
            st.info("Données insuffisantes pour calculer la corrélation.")

    # Matrice de corrélation
    with tab6:
        st.subheader("Matrice de corrélations multiples")
        df_all = (
            df_chom.merge(df_inc, on="code", how="inner")
            .merge(df_pop, on="code", how="inner")
            .merge(df_pov, on="code", how="inner")
        )
        cols = [c for c in ["taux_chomage", "income_median", "population", "poverty_rate"] if c in df_all.columns]
        if len(cols) > 1:
            corr = df_all[cols].corr()
            fig, ax = plt.subplots()
            cax = ax.imshow(corr, vmin=-1, vmax=1)
            ax.set_xticks(range(len(corr)))
            labels = [COLS_NICE.get(c, c) for c in corr.columns]
            ax.set_xticklabels(labels, rotation=45, ha="right")
            ax.set_yticks(range(len(corr)))
            ax.set_yticklabels(labels)
            for i in range(len(corr)):
                for j in range(len(corr)):
                    val = corr.iat[i, j]
                    color = "white" if abs(val) > 0.5 else "black"
                    ax.text(j, i, f"{val:.2f}", ha="center", va="center", color=color)
            fig.colorbar(cax, ax=ax, fraction=0.046, pad=0.04)
            st.pyplot(fig)
        else:
            st.info("Corrélation impossible : données manquantes.")

# -----------------------------
# VUE RÉGION
# -----------------------------
elif view == "Région":
    st.header("🌍 Indicateurs par Région")

    @st.cache_data(show_spinner=False)
    def load_region_df() -> pd.DataFrame:
        df = pd.read_sql_query("SELECT * FROM region_analysis", conn)
        if "code_region" in df.columns:
            df["code_region"] = df["code_region"].astype(str).str.zfill(2)
        return df

    @st.cache_data(show_spinner=False)
    def load_region_geo(path: str):
        geo = gpd.read_file(path)[["code", "geometry"]]
        geo["geometry"] = geo["geometry"].simplify(tolerance=0.02, preserve_topology=True)
        return geo

    try:
        df_region = load_region_df()
        geo_reg = load_region_geo(os.path.join("data", "raw", "geo", "regions.geojson"))
    except Exception as e:
        st.error(f"Données régionales manquantes : {e}")
        st.stop()

    st.subheader("Aperçu des données régionales")
    show(df_region)

    pop_min = int(df_region["population"].min()) if "population" in df_region.columns else 0
    pop_max = int(df_region["population"].max()) if "population" in df_region.columns else 0
    borne_min = (pop_min // 2_000_000) * 2_000_000
    borne_max = ((max(pop_max, 1) // 2_000_000) + 1) * 2_000_000
    st.sidebar.subheader("Plage de population (pas 2 M)")
    x_range = st.sidebar.slider(
        "Population", min_value=borne_min, max_value=borne_max, value=(borne_min, borne_max), step=2_000_000, format="%d"
    )

    geo_plot = geo_reg.merge(df_region.rename(columns={"code_region": "code"}), on="code", how="left")
    m_reg = folium.Map(location=[46.6, 2.4], zoom_start=5)
    folium.Choropleth(
        geo_data=geo_plot,
        data=geo_plot,
        columns=["code", "prix_m2_moyen"],
        key_on="feature.properties.code",
        legend_name="Prix moyen (€ / m²)",
        fill_opacity=0.7,
        line_opacity=0.2,
        nan_fill_color="white",
    ).add_to(m_reg)
    st.subheader("Carte du prix moyen au m² par région")
    st_folium(m_reg, width=800, height=600)

    fig_r, ax_r = plt.subplots()
    if "prix_m2_moyen" in df_region.columns:
        ax_r.hist(df_region["prix_m2_moyen"].dropna(), bins=20, edgecolor="black")
    ax_r.set_xlabel("Prix moyen (€ / m²)")
    ax_r.set_ylabel("Nombre de régions")
    st.subheader("Distribution du prix moyen par région")
    st.pyplot(fig_r)

    fig_sp, ax_sp = plt.subplots()
    if {"population", "prix_m2_moyen"}.issubset(df_region.columns):
        ax_sp.scatter(df_region["population"], df_region["prix_m2_moyen"], alpha=0.7)
        ax_sp.set_xlim(x_range)
    ax_sp.set_xlabel("Population")
    ax_sp.set_ylabel("Prix moyen (€ / m²)")
    fmt = mticker.FuncFormatter(lambda x, _: f"{x/1_000_000:.1f} M")
    ax_sp.xaxis.set_major_formatter(fmt)
    st.subheader(f"Population vs Prix moyen par région (zoom : {x_range[0]:,} → {x_range[1]:,})")
    st.pyplot(fig_sp)

    st.subheader("Matrice de corrélations régionales")
    features = ["prix_m2_moyen", "population", "income_median", "taux_chomage", "poverty_rate"]
    cols = [c for c in features if c in df_region.columns]
    if len(cols) > 1:
        corr = df_region[cols].corr()
        fig, ax = plt.subplots()
        cax = ax.imshow(corr, vmin=-1, vmax=1)
        ax.set_xticks(range(len(corr)))
        labels_reg = [COLS_NICE.get(c, c) for c in corr.columns]
        ax.set_xticklabels(labels_reg, rotation=45, ha="right")
        ax.set_yticks(range(len(corr)))
        ax.set_yticklabels(labels_reg)
        for i in range(len(corr)):
            for j in range(len(corr)):
                val = corr.iat[i, j]
                ax.text(j, i, f"{val:.2f}", ha="center", va="center", color=("white" if abs(val) > 0.5 else "black"))
        fig.colorbar(cax, ax=ax, fraction=0.046, pad=0.04)
        st.pyplot(fig)
    else:
        st.info("Corrélation impossible : données socio-économiques manquantes.")

# -----------------------------
# VUE MÉTHODOLOGIE
# -----------------------------
elif view == "Méthodologie":
    st.header("📚 Méthodologie & Choix techniques")
    st.markdown(
        """
### Pré-processing des données
- **Transactions DVF 2024** : nettoyage, contrôle outliers, extraction département.
- **Indicateurs INSEE** : filtrage, cast numérique, agrégation départementale.
- Scripts dans `src/backend/ingest_*.py`.

### Choix des métriques
| Métrique | Rôle |
|---|---|
| Prix moyen au m² | Indicateur principal |
| Revenu médian | Pouvoir d’achat |
| Taux de chômage | Dynamique économique |
| Taux de pauvreté | Vulnérabilité socio-éco |
| Population | Taille du marché |

### Architecture
CSV / Scraping → ETL scripts → SQLite (homepedia.db)
│
└─► Streamlit (5 vues) → Docker

### Pistes d’amélioration
- Migrations (Alembic), Postgres si multi-utilisateur
- Tests d’ingestion plus complets
- Authentification Streamlit si déploiement public
        """
)