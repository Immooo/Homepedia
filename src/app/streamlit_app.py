import math
import os
from pathlib import Path
import random
import sqlite3
import sys
from collections.abc import Sequence
from typing import Any

import folium
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns
import streamlit as st
from scipy.stats import linregress
from streamlit_folium import st_folium
from textblob import TextBlob
from tinydb import TinyDB
from wordcloud import WordCloud

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from app.db.mongo_client import get_mongo_collection  # noqa: E402
from app.db.sqlite_reader import connect_readonly  # noqa: E402

COLS_NICE = {
    "code": "Département",
    "dept": "Département",
    "code_region": "Région",
    "id": "ID transaction",
    "date_mutation": "Date mutation",
    "nature_mutation": "Nature de la mutation",
    "valeur_fonciere": "Valeur foncière €",
    "code_postal": "Code postal",
    "commune": "Commune",
    "type_local": "Type de bien",
    "surface_reelle_bati": "Surface bâtie m²",
    "nombre_pieces_principales": "Nb pièces principales",
    "nb_transactions": "Nombres de transactions",
    "prix_m2_moyen": "Prix moyen €/m²",
    "prix_m2": "Prix €/m²",
    "population": "Population",
    "revenu_median": "Revenu médian €",
    "income_median": "Revenu médian €",
    "taux_chomage": "Taux chômage %",
    "taux_pauvrete": "Taux pauvreté %",
    "poverty_rate": "Taux pauvreté %",
    "income": "Revenu médian € ",
    "unemployment": "Taux chômage % ",
    "poverty": "Taux pauvreté % ",
}


def compute_polarity(text: str) -> float:
    blob = TextBlob(str(text))
    sent: Any = blob.sentiment
    return float(sent.polarity)


def pretty(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={k: v for k, v in COLS_NICE.items() if k in df.columns})


def show(df: pd.DataFrame, n: int | None = None):
    displayed = pretty(df if n is None else df.head(n))
    config = {}
    for column in displayed.columns:
        if "Prix" in column or "Valeur foncière" in column or "Revenu" in column:
            config[column] = st.column_config.NumberColumn(format="%.0f €")
        elif "Taux" in column:
            config[column] = st.column_config.NumberColumn(format="%.1f %%")
    st.dataframe(displayed, column_config=config, use_container_width=True)


# 1. Configuration de la page
st.set_page_config(page_title="Homepedia – Analyses Immobilier France", layout="wide")
st.title("🏠 Homepedia – Analyses Immobilier France")

# 2. Choix de la vue/onglet dans la sidebar
view = st.sidebar.radio(
    "Choix de la vue",
    [
        "Standard",
        "Spark Analysis",
        "Text Analysis",
        "Indicateurs Socio-éco",
        "Région",
        "Méthodologie",
    ],
)

# 3. Connexion à la base SQLite
DB_PATH = os.getenv("DB_PATH", os.path.join("data", "homepedia.db"))
try:
    conn = connect_readonly(DB_PATH)
except sqlite3.OperationalError:
    st.error(
        "La base analytique est momentanément indisponible. "
        "Patiente quelques secondes puis recharge la page."
    )
    st.stop()

# === VUE STANDARD ===
if view == "Standard":
    st.header("Transactions immobilières (live SQL + Pandas)")

    # --- Connexions rapides ---
    def sql_scalar(query: str):
        return conn.execute(query).fetchone()[0]

    # --- Mode d'analyse ---
    mode = st.sidebar.radio(
        "Mode d'analyse",
        ["Rapide (échantillon léger)", "Détaillé (toutes les visualisations)"],
    )
    fast_mode = mode.startswith("Rapide")

    # --- Période ---
    st.sidebar.subheader("Filtres Transactions")

    if fast_mode:
        max_rows = st.sidebar.slider(
            "Nombre maximum de lignes à charger",
            min_value=10_000,
            max_value=80_000,
            value=40_000,
            step=10_000,
            help="Mode rapide : petit échantillon pour garder l'interface très fluide.",
        )
    else:
        max_rows = st.sidebar.slider(
            "Nombre maximum de lignes à charger",
            min_value=50_000,
            max_value=200_000,
            value=120_000,
            step=10_000,
            help="Mode détaillé : plus de données, calculs et graphiques plus lourds.",
        )

    min_date = pd.to_datetime("2024-01-01")
    max_date = pd.to_datetime("2024-12-31")
    raw_dates = st.sidebar.date_input(
        "Période",
        [min_date.date(), max_date.date()],
        min_value=min_date.date(),
        max_value=max_date.date(),
    )

    if isinstance(raw_dates, Sequence):
        dates_list = list(raw_dates)
        if len(dates_list) == 0:
            start_date = min_date
            end_date = max_date
        elif len(dates_list) == 1:
            start_date = end_date = pd.to_datetime(dates_list[0])
        else:
            start_date = pd.to_datetime(dates_list[0])
            end_date = pd.to_datetime(dates_list[1])
    else:
        start_date = end_date = pd.to_datetime(raw_dates)

    # --- Type de bien ---
    type_list = ["Tous"] + [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT type_local FROM transactions WHERE type_local IS NOT NULL ORDER BY 1"
        ).fetchall()
    ]
    choix_type = st.sidebar.selectbox("Type de logement", type_list)

    # Bornes métier fixes : évite un scan intégral de 5,8 M de lignes à chaque session.
    pmin_glob, pmax_glob = 0, 20_000

    price_range = st.sidebar.slider(
        "Prix au m²",
        pmin_glob,
        pmax_glob,
        (pmin_glob, pmax_glob),
    )

    # --- Chargement filtré ---
    @st.cache_data(show_spinner=False)
    def load_transactions(start, end, type_sel, pmin, pmax, max_rows: int):
        start_iso = start.strftime("%Y-%m-%d")
        end_iso = end.strftime("%Y-%m-%d")

        query = """
            SELECT * ,
                valeur_fonciere / surface_reelle_bati AS prix_m2,
                substr(code_postal,1,2) AS dept
            FROM   transactions
            WHERE  date_mutation BETWEEN ? AND ?
            AND  surface_reelle_bati > 0
            AND  valeur_fonciere IS NOT NULL
            AND  (valeur_fonciere / surface_reelle_bati) BETWEEN ? AND ?
        """
        params = [start_iso, end_iso, pmin, pmax]

        if type_sel != "Tous":
            query += " AND type_local = ?"
            params.append(type_sel)

        # Limite de sécurité
        query += " LIMIT ?"
        params.append(max_rows)

        df = pd.read_sql_query(
            query,
            conn,
            params=params,
            parse_dates=["date_mutation"],
        )

        if "code_postal" in df.columns:
            df["code_postal"] = (
                df["code_postal"].astype(str).str.replace(r"\.0$", "", regex=True)
            )

        if "dept" not in df.columns and "code_postal" in df.columns:
            df["dept"] = df["code_postal"].str[:2].str.zfill(2)

        return df

    tx = load_transactions(
        start_date, end_date, choix_type, price_range[0], price_range[1], max_rows
    )
    total_transactions = sql_scalar("SELECT COUNT(*) FROM transactions")

    if tx.empty:
        st.warning("Aucune transaction ne correspond à ces filtres.")
        st.stop()

    # --- KPIs & export ---
    col1, col2, col3 = st.columns(3)
    col1.metric("Transactions chargées (échantillon)", f"{len(tx):,}")
    col2.metric("Surface médiane (m²)", f"{tx['surface_reelle_bati'].median():.1f}")
    col3.metric("Prix moyen €/m²", f"{tx['prix_m2'].mean():.2f}")

    if fast_mode:
        st.caption(
            f"Mode rapide – échantillon de {len(tx):,} transactions sur {total_transactions:,} au total "
            "pour garder l'interface très fluide."
        )
    else:
        st.caption(
            f"Mode détaillé – échantillon de {len(tx):,} transactions sur {total_transactions:,} au total "
            "avec toutes les visualisations activées."
        )

    st.download_button(
        "📥 Exporter ces transactions (CSV)",
        tx.to_csv(index=False).encode("utf-8"),
        file_name="transactions_filtrees.csv",
        mime="text/csv",
    )

    st.subheader("Aperçu des transactions filtrées")
    show(tx.drop(columns=["date_mutation"]), 10)

    # --- Carte choroplèthe ---
    prix_dept = (
        tx.groupby("dept")["prix_m2"]
        .mean()
        .reset_index()
        .rename(columns={"dept": "code", "prix_m2": "prix_m2_moyen"})
    )
    geo = gpd.read_file("data/raw/geo/departements_simplifie.geojson")[
        ["code", "geometry"]
    ]
    geo = geo.merge(prix_dept, on="code", how="left")

    if (not fast_mode) and st.checkbox("Afficher la carte", value=True):
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

    # --- Histogramme ---
    st.subheader("Distribution des prix au m²")

    valid_prices = tx["prix_m2"].replace([np.inf, -np.inf], np.nan).dropna()

    if valid_prices.empty:
        st.info("Aucune transaction exploitable pour ces filtres (prix/m² manquant).")
    else:
        q01 = valid_prices.quantile(0.01)
        q99 = valid_prices.quantile(0.99)

        xmin = max(price_range[0], int(q01))
        xmax = min(price_range[1], int(q99))

        fig1, ax1 = plt.subplots()
        ax1.hist(
            valid_prices,
            bins="auto",
            range=(xmin, xmax),
            edgecolor="black",
        )
        ax1.set_xlim(xmin, xmax)
        ax1.set_xlabel("Prix (€ / m²)")
        ax1.set_ylabel("Nombre de transactions")
        st.pyplot(fig1, use_container_width=True)

    # --- Box-plot ---
    if not fast_mode:
        st.subheader("Dispersion prix/m² par type de bien")

        fig_box, ax_box = plt.subplots(figsize=(9, 4))
        tx.boxplot(column="prix_m2", by="type_local", ax=ax_box, showfliers=False)

        fig_box.suptitle("Boxplot du prix au m² par type de bien")
        ax_box.set_title("")

        ax_box.set_xlabel("Type de bien")
        ax_box.set_ylabel("€ / m²")
        ax_box.tick_params(axis="x", labelrotation=45)
        ax_box.set_xticklabels(
            [lab.get_text().replace(" ", "\n", 1) for lab in ax_box.get_xticklabels()],
            ha="right",
            fontsize=8,
        )

        st.pyplot(fig_box)

    # --- Scatter population ---
    if not fast_mode:
        pop = pd.read_sql_query("SELECT * FROM population", conn)
        prix_pop = prix_dept.merge(pop, on="code", how="left")

        st.subheader("Population vs Prix moyen")
        fig2, ax2 = plt.subplots()
        ax2.scatter(prix_pop["population"], prix_pop["prix_m2_moyen"], alpha=0.6)
        ax2.set_xlabel("Population départementale")
        ax2.set_ylabel("Prix moyen (€ / m²)")
        ax2.xaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f} M")
        )
        st.pyplot(fig2)

        st.subheader("Population par département (jointure DVF + INSEE)")
        show(prix_pop[["code", "population", "prix_m2_moyen"]])

# === VUE SPARK ANALYSIS ===
elif view == "Spark Analysis":
    st.header("Vue Spark Analysis (pré-agrégation)")
    df_spark = pd.read_sql_query(
        "SELECT dept AS code, nb_transactions, prix_m2_moyen FROM analyse_departementale",
        conn,
    )
    df_spark["prix_m2_moyen"] = df_spark["prix_m2_moyen"].round(0).astype(int)
    st.subheader("Résultats Spark par département")
    show(df_spark)
    per_page = st.sidebar.slider("Dépts par page", 5, 50, 10, 5)
    n_pages = math.ceil(len(df_spark) / per_page)
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

# === VUE TEXT ANALYSIS ===
elif view == "Text Analysis":
    st.header("Analyse textuelle annexe (sentiment & nuage de mots)")
    st.warning(
        "Cette démonstration NLP utilise le jeu public Hotel Reviews. "
        "Elle illustre le traitement de texte mais n'entre pas dans les indicateurs DVF/INSEE."
    )
    tdb_path = os.path.join("data", "processed", "comments.json")
    if not os.path.exists(tdb_path):
        st.error(f"Base NoSQL manque : {tdb_path}")
        st.stop()
    db = TinyDB(tdb_path)
    docs = db.all()
    st.sidebar.markdown(f"**Total commentaires :** {len(docs):,}")
    per_page = st.sidebar.slider("Avis par page", 10, 200, 50, 10)
    page = st.sidebar.number_input("Page", 1, math.ceil(len(docs) / per_page), 1)
    subset = docs[(page - 1) * per_page : page * per_page]
    df_page = pd.DataFrame(subset)
    st.subheader(f"Avis hôteliers — jeu annexe (page {page})")
    show(df_page)
    df_page["sentiment"] = df_page["commentaire"].map(compute_polarity)
    st.subheader("Sentiment des avis")
    st.bar_chart(df_page["sentiment"])
    sample_n = st.sidebar.slider("Échantillon Word Cloud", 100, 5000, 1000, 100)
    sampled = random.sample(docs, sample_n)
    text = " ".join(d["commentaire"] for d in sampled)
    wc = WordCloud(width=800, height=400, background_color="white").generate(text)
    fig_wc, ax_wc = plt.subplots(figsize=(10, 5))
    ax_wc.imshow(wc, interpolation="bilinear")
    ax_wc.axis("off")
    st.subheader(f"Word Cloud (n={sample_n})")
    st.pyplot(fig_wc)

# === VUE SOCIO-ÉCO ===
elif view == "Indicateurs Socio-éco":
    st.header("📊 Indicateurs Socio-économiques (INSEE)")

    # Filtres Socio-éco
    df_chom_tmp = pd.read_parquet("data/processed/unemployment_dept.parquet")

    df_chom_tmp["taux_chomage"] = pd.to_numeric(
        df_chom_tmp["taux_chomage"].str.replace(",", "."), errors="coerce"
    )
    min_c, max_c = st.sidebar.slider(
        "Taux de chômage (%)",
        float(df_chom_tmp["taux_chomage"].min()),
        float(df_chom_tmp["taux_chomage"].max()),
        (
            float(df_chom_tmp["taux_chomage"].min()),
            float(df_chom_tmp["taux_chomage"].max()),
        ),
    )

    # Chemins
    unemployment_path = "data/processed/unemployment_dept.parquet"
    income_path = "data/processed/income_dept.parquet"
    population_path = "data/processed/population_dept.parquet"
    poverty_path = "data/processed/poverty_dept.parquet"
    geojson_path = "data/raw/geo/departements_simplifie.geojson"

    # Vérifications
    for path, name in [
        (unemployment_path, "chômage"),
        (income_path, "revenu médian"),
        (population_path, "population"),
        (poverty_path, "pauvreté"),
    ]:
        if not os.path.exists(path):
            st.error(f"Données {name} manquantes ({path}).")
            st.stop()

    # Chargement
    @st.cache_data(show_spinner=False)
    def load_df(path: str) -> pd.DataFrame:
        return pd.read_parquet(path)

    @st.cache_data
    def load_geo(path):
        return gpd.read_file(path)[["code", "geometry"]]

    df_chom = load_df(unemployment_path)
    df_inc = load_df(income_path)
    df_pop = load_df(population_path)
    df_pov = load_df(poverty_path)
    geo = load_geo(geojson_path)

    # Conversion numérique
    df_chom["taux_chomage"] = pd.to_numeric(
        df_chom["taux_chomage"].str.replace(",", "."), errors="coerce"
    )
    df_inc["income_median"] = pd.to_numeric(
        df_inc["income_median"].str.replace(",", "."), errors="coerce"
    )
    df_pop["population"] = pd.to_numeric(
        df_pop["population"].str.replace(" ", ""), errors="coerce"
    )
    df_pov["poverty_rate"] = pd.to_numeric(
        df_pov["poverty_rate"].str.replace(",", "."), errors="coerce"
    )

    # Filtre chômage
    socio = (
        df_chom.merge(df_inc, on="code", how="inner")
        .merge(df_pop, on="code", how="inner")
        .merge(df_pov, on="code", how="inner")
    )
    socio = socio.query("@min_c <= taux_chomage <= @max_c").copy()
    selected_codes = set(socio["code"])
    df_chom = df_chom[df_chom["code"].isin(selected_codes)].copy()
    df_inc = df_inc[df_inc["code"].isin(selected_codes)].copy()
    df_pop = df_pop[df_pop["code"].isin(selected_codes)].copy()
    df_pov = df_pov[df_pov["code"].isin(selected_codes)].copy()

    st.sidebar.caption(
        f"Filtre commun appliqué à toutes les vues : {len(selected_codes)} départements."
    )
    if socio.empty:
        st.warning("Aucun département ne correspond à cette plage de chômage.")
        st.stop()

    # Onglets
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
        [
            "Chômage",
            "Revenu médian",
            "Population",
            "Pauvreté",
            "Corrélation",
            "Matrice corrélations",
        ]
    )

    # --- Chômage ---
    with tab1:
        st.subheader("Taux de chômage (T1 2025)")
        show(df_chom)
        geo1 = geo.merge(
            df_chom.rename(columns={"code": "code"}), on="code", how="left"
        )
        m1 = folium.Map(location=[46.6, 2.4], zoom_start=5)
        folium.Choropleth(
            geo_data=geo1,
            data=geo1,
            columns=["code", "taux_chomage"],
            key_on="feature.properties.code",
            legend_name="Taux de chômage (%)",
        ).add_to(m1)
        st_folium(m1, width=800, height=600)

    # --- Revenu médian ---
    with tab2:
        st.subheader("Revenu médian (2021)")
        show(df_inc)
        geo2 = geo.merge(df_inc, on="code", how="left")
        m2 = folium.Map(location=[46.6, 2.4], zoom_start=5)
        folium.Choropleth(
            geo_data=geo2,
            data=geo2,
            columns=["code", "income_median"],
            key_on="feature.properties.code",
            legend_name="Revenu médian (€ / an)",
        ).add_to(m2)
        st_folium(m2, width=800, height=600)

    # --- Population ---
    with tab3:
        st.subheader("Population")
        show(df_pop)
        geo3 = geo.merge(df_pop, on="code", how="left")
        m3 = folium.Map(location=[46.6, 2.4], zoom_start=5)
        folium.Choropleth(
            geo_data=geo3,
            data=geo3,
            columns=["code", "population"],
            key_on="feature.properties.code",
            legend_name="Population",
        ).add_to(m3)
        st_folium(m3, width=800, height=600)
        fig3, ax3 = plt.subplots()
        ax3.hist(df_pop["population"].dropna(), bins=30, edgecolor="black")
        ax3.set_xlabel("Population")
        ax3.set_ylabel("Nombre de départements")
        ax3.xaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, pos: f"{x/1e6:.1f} M")
        )
        plt.xticks(rotation=45)
        st.pyplot(fig3)

    # --- Pauvreté ---
    with tab4:
        st.subheader("Taux de pauvreté")
        show(df_pov)
        geo4 = geo.merge(df_pov, on="code", how="left")
        m4 = folium.Map(location=[46.6, 2.4], zoom_start=5)
        folium.Choropleth(
            geo_data=geo4,
            data=geo4,
            columns=["code", "poverty_rate"],
            key_on="feature.properties.code",
            legend_name="Taux de pauvrété (%)",
        ).add_to(m4)
        st_folium(m4, width=800, height=600)
        fig4, ax4 = plt.subplots()
        ax4.hist(df_pov["poverty_rate"].dropna(), bins=30, edgecolor="black")
        ax4.set_xlabel("Taux de pauvreté (%)")
        ax4.set_ylabel("Nombre de départements")
        st.pyplot(fig4)

    # --- Corrélation ---
    with tab5:
        st.subheader("Corrélation chômage ↔ revenu")
        df_corr = df_chom.merge(df_inc, on="code")
        fig5, ax5 = plt.subplots()
        ax5.scatter(df_corr["income_median"], df_corr["taux_chomage"], alpha=0.7)
        slope, intercept, r, p, se = linregress(
            df_corr["income_median"], df_corr["taux_chomage"]
        )
        xx = np.linspace(
            df_corr["income_median"].min(), df_corr["income_median"].max(), 100
        )
        r_arr = np.asarray(r, dtype="float64")
        r2 = r_arr**2
        ax5.plot(xx, intercept + slope * xx, linestyle="--", label=f"R²={r2:.2f}")
        ax5.set_xlabel("Revenu médian (€ / an)")
        ax5.set_ylabel("Taux de chômage (%)")
        ax5.legend()
        st.pyplot(fig5)

    # --- Matrice corrélation ---
    with tab6:
        st.subheader("Matrice de corrélations multiples")
        st.info(
            "Chaque case est un coefficient indépendant compris entre -1 et +1. "
            "Les coefficients d'une ligne ne sont pas des pourcentages et ne doivent pas totaliser 1."
        )
        corr = socio[
            ["taux_chomage", "income_median", "population", "poverty_rate"]
        ].corr()
        fig6, ax6 = plt.subplots()
        cax = ax6.imshow(corr, vmin=-1, vmax=1)
        ax6.set_xticks(range(len(corr)))
        labels = [COLS_NICE.get(c, c) for c in corr.columns]
        ax6.set_xticklabels(labels, rotation=45, ha="right")
        ax6.set_yticks(range(len(corr)))
        ax6.set_yticklabels(labels)
        for i in range(len(corr)):
            for j in range(len(corr)):
                val = corr.iat[i, j]
                val_f = np.asarray(val, dtype="float64")
                color = "white" if abs(val_f) > 0.5 else "black"
                ax6.text(j, i, f"{val:.2f}", ha="center", va="center", color=color)
        fig6.colorbar(cax, ax=ax6, fraction=0.046, pad=0.04)
        st.pyplot(fig6)

# === VUE RÉGION ===
elif view == "Région":
    st.header("🌍 Indicateurs par Région")

    # 1) Chargement en cache des données régionales
    @st.cache_data
    def load_region_df():
        df = pd.read_sql_query("SELECT * FROM analyse_regionale", conn)
        # zfill sur code_region si nécessaire
        df["code_region"] = df["code_region"].astype(str).str.zfill(2)
        return df

    # 2) Lecture + simplification du GeoJSON en cache
    @st.cache_data
    def load_region_geo(path):
        geo = gpd.read_file(path)[["code", "geometry"]]
        # simplification : tolérance ajustable (en degrés décimaux)
        geo["geometry"] = geo["geometry"].simplify(
            tolerance=0.02, preserve_topology=True
        )
        return geo

    df_region_all = load_region_df()
    geo_reg = load_region_geo(os.path.join("data", "raw", "geo", "regions.geojson"))

    # 3) Slider population (inchangé)
    pop_min = int(df_region_all["population"].min())
    pop_max = int(df_region_all["population"].max())
    borne_min = (pop_min // 500_000) * 500_000
    borne_max = ((pop_max // 500_000) + 1) * 500_000
    st.sidebar.subheader("Filtre régional")
    x_range = st.sidebar.slider(
        "Population",
        min_value=borne_min,
        max_value=borne_max,
        value=(borne_min, borne_max),
        step=500_000,
        format="%d",
    )
    df_region = df_region_all.query("@x_range[0] <= population <= @x_range[1]").copy()
    st.sidebar.caption(f"{len(df_region)} région(s) sélectionnée(s).")
    if df_region.empty:
        st.warning("Aucune région ne correspond à cette plage de population.")
        st.stop()

    st.subheader("Aperçu des données régionales filtrées")
    show(df_region)

    # 4) Merge + carte Folium
    geo_plot = geo_reg.merge(
        df_region.rename(columns={"code_region": "code"}), on="code", how="left"
    )
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

    # 5) Histogramme prix moyen
    fig_r, ax_r = plt.subplots()
    ax_r.hist(df_region["prix_m2_moyen"].dropna(), bins=20, edgecolor="black")
    ax_r.set_xlabel("Prix moyen (€ / m²)")
    ax_r.set_ylabel("Nombre de régions")
    st.subheader("Distribution du prix moyen par région")
    st.pyplot(fig_r)

    # 6) Scatter Population vs Prix (avec zoom slider)
    fig_sp, ax_sp = plt.subplots()
    ax_sp.scatter(df_region["population"], df_region["prix_m2_moyen"], alpha=0.7)
    ax_sp.set_xlim(x_range)
    ax_sp.set_xlabel("Population")
    ax_sp.set_ylabel("Prix moyen (€ / m²)")

    # Formateur de ticks en M
    fmt = mticker.FuncFormatter(lambda x, _: f"{x/1_000_000:.1f} M")
    ax_sp.xaxis.set_major_formatter(fmt)
    plt.xticks(rotation=45)

    st.subheader(
        f"Population vs Prix moyen par région (zoom : {x_range[0]:,} → {x_range[1]:,})"
    )
    st.pyplot(fig_sp)

    st.subheader("Matrice de corrélations régionales")
    st.caption(
        "Coefficients indépendants entre -1 et +1 : ils mesurent la force d'une relation, "
        "ils ne s'additionnent pas pour former 100 %."
    )

    @st.cache_data
    def compute_region_corr(df: pd.DataFrame) -> pd.DataFrame:
        features = [
            "prix_m2_moyen",
            "population",
            "revenu_median",
            "taux_chomage",
            "taux_pauvrete",
        ]
        cols = [c for c in features if c in df.columns]
        return df[cols].corr() if len(cols) > 1 else pd.DataFrame()

    corr_reg = compute_region_corr(df_region)

    if corr_reg.empty:
        st.info("Corrélation impossible : données socio-économiques manquantes.")
    else:
        fig, ax = plt.subplots()
        sns.heatmap(
            corr_reg,
            annot=True,
            fmt=".2f",
            cmap="coolwarm",
            vmin=-1,
            vmax=1,
            square=True,
            cbar_kws={"shrink": 0.75},
            ax=ax,
        )
        # Ajustement dynamique de la couleur des annotations
        for annotation in ax.texts:
            val = float(annotation.get_text())
            annotation.set_color("white" if abs(val) > 0.5 else "black")
        labels_reg = [COLS_NICE.get(c, c) for c in corr_reg.columns]
        ax.set_xticklabels(labels_reg, rotation=45, ha="right")
        ax.set_yticklabels(labels_reg)
        st.pyplot(fig)

# === VUE MÉTHODOLOGIE ===
elif view == "Méthodologie":
    st.header("📚 Méthodologie & Choix techniques")

    total_transactions = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    nb_departements = conn.execute(
        "SELECT COUNT(*) FROM analyse_departementale"
    ).fetchone()[0]
    c1, c2, c3 = st.columns(3)
    c1.metric("Transactions DVF nettoyées", f"{total_transactions:,}")
    c2.metric("Départements agrégés", nb_departements)
    c3.metric("Workers Spark", 2)

    if st.button("🔍 Tester la connexion Mongo"):
        from app.db.mongo_client import get_mongo_collection

        col = get_mongo_collection("transactions")
        doc = col.find_one()
        st.write("Exemple document Mongo :", doc)

    st.markdown(
        """
    ### Prétraitement et qualité des données
    - **Transactions DVF 2024** : normalisation des dates et codes postaux, conversion numérique, suppression des doublons exacts et exclusion des mutations symboliques inférieures à 1 000 €.
    - Pour les indicateurs de prix, seules les surfaces strictement positives et les valeurs comprises entre 0 et 20 000 €/m² sont conservées.
    - **Indicateurs INSEE (revenu, chômage, pauvreté, population)** : filtrage des mesures fiables, conversion numérique, agrégation au niveau départemental.
    - Le contrat partagé par SQLite, MongoDB, Streamlit et Metabase est documenté dans `docs/DATA_CONTRACT.md`.

    ### Choix des métriques
    | Métrique | Rôle dans l’analyse |
    |----------|--------------------|
    | Prix moyen au m² | Indicateur principal du marché immobilier |
    | Revenu médian | Pouvoir d’achat local |
    | Taux de chômage | Dynamique économique |
    | Taux de pauvreté | Vulnérabilité socio-éco |
    | Population | Taille du marché |

    ### Librairies data science mises en œuvre
    - **pandas**, **geopandas** : manipulation tabulaire & géospatiale
    - **matplotlib / seaborn** : visualisation statistique
    - **PySpark** : pré-agrégations DVF distribuées sur deux workers
    - **folium** : cartes choroplèthes interactives

    ### Architecture
    ```text
    DVF / INSEE → ETL + contrôles qualité → SQLite (référence)
                         │                  │
                         │                  ├─► MongoDB homepedia_buffer → Metabase
                         │                  └─► Streamlit (tableaux, cartes, graphiques)
                         └─► Spark master → worker 1 + worker 2
                                      └─► analyse_departementale

    INSEE web → polling 300 s → validation → latest / history / runs
    ```

    ### Stockage et reproductibilité
    - **SQLite** : référentiel relationnel local, adapté à la démonstration analytique.
    - **MongoDB** : miroir non relationnel consommé par Metabase et stockage du flux périodique.
    - **Docker Compose** : application, worker temps réel, MongoDB, Metabase et cluster Spark reproductibles.
    - **Tests automatisés** : contrat des tables, présence, volumes minimaux et absence de valeurs numériques nulles.

    ### Limites assumées
    - Le cluster possède plusieurs nœuds conteneurisés mais fonctionne sur une seule machine physique.
    - Le temps réel est un polling micro-batch, pas un streaming événementiel natif.
    - SQLite convient au projet local, pas à de nombreuses écritures concurrentes en production.
    """
    )


def show_mongo_debug():
    col = get_mongo_collection("properties")
    doc = col.find_one()
    st.write("Exemple document Mongo :", doc)


# Clôture
conn.close()
