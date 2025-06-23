import os
import sqlite3
import math
import random
from scipy.stats import linregress
import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
from streamlit_folium import st_folium
import matplotlib.pyplot as plt
from textblob import TextBlob
from wordcloud import WordCloud
from tinydb import TinyDB
import numpy as np
from typing import Any, List, cast
import plotly.express as px

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
        "Indicateurs Socio-éco"
    ]
)

# 3. Connexion à la base SQLite
DB_PATH = os.path.join("data", "homepedia.db")
conn = sqlite3.connect(DB_PATH)

# --- VUE STANDARD ---
if view == "Standard":
    st.header("Vue Standard (live SQL + Pandas)")

    # Filtres
    st.sidebar.subheader("Filtres Standard")
    min_date = pd.to_datetime("2024-01-01")
    max_date = pd.to_datetime("2024-12-31")
    raw_dates = st.sidebar.date_input(
        "Période",
        [min_date.date(), max_date.date()],
        min_value=min_date.date(),
        max_value=max_date.date()
    )
    start_date = pd.to_datetime(raw_dates[0])
    end_date = pd.to_datetime(raw_dates[1])

    @st.cache_data
    def load_transactions():
        df = pd.read_sql_query("SELECT * FROM transactions", conn, parse_dates=["date_mutation"])
        df["surface_reelle_bati"] = pd.to_numeric(df["surface_reelle_bati"], errors="coerce")
        df = df[df["surface_reelle_bati"] > 0]
        df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]
        df["dept"] = df["code_postal"].astype(str).str[:2]
        return df

    tx = load_transactions()
    st.subheader("Aperçu des transactions brutes")
    st.dataframe(tx.head(10))

    # Filtre Type
    types = ["Tous"] + sorted(tx["type_local"].dropna().unique().tolist())
    choix_type = st.sidebar.selectbox("Type de logement", types)

    # Filtre Prix
    pmin, pmax = tx["prix_m2"].quantile([0.01, 0.99])
    price_range = st.sidebar.slider("Prix au m²", int(pmin), int(pmax), (int(pmin), int(pmax)))

    # Application des filtres
    mask = (tx["date_mutation"] >= start_date) & (tx["date_mutation"] <= end_date)
    if choix_type != "Tous":
        mask &= tx["type_local"] == choix_type
    mask &= tx["prix_m2"].between(price_range[0], price_range[1])
    df_filt = tx[mask]

    st.subheader("Transactions après filtres")
    st.dataframe(df_filt.head(10))

    # Carte choroplèthe
    prix_dept = (
        df_filt.groupby("dept")["prix_m2"]
               .mean()
               .reset_index()
               .rename(columns={"dept":"code","prix_m2":"prix_m2_moyen"})
    )
    geo = gpd.read_file(os.path.join("data","raw","geo","departements_simplifie.geojson"))[["code","geometry"]]
    geo = geo.merge(prix_dept, on="code", how="left")
    m = folium.Map(location=[46.6,2.4], zoom_start=5)
    folium.Choropleth(
        geo_data=geo,
        data=geo,
        columns=["code","prix_m2_moyen"],
        key_on="feature.properties.code",
        legend_name="Prix moyen (€ / m²)",
        fill_opacity=0.7,
        line_opacity=0.2,
        nan_fill_color="white"
    ).add_to(m)
    folium.LayerControl().add_to(m)
    st.subheader("Carte interactive")
    st_folium(m, width=800, height=600)

    # Histogramme
    fig1, ax1 = plt.subplots()
    ax1.hist(df_filt["prix_m2"], bins=50, range=(price_range[0], price_range[1]))
    ax1.set_xlim(price_range)
    ax1.set_xlabel("Prix (€ / m²)")
    ax1.set_ylabel("Nombre de transactions")
    st.pyplot(fig1)

    # Scatter population
    pop = pd.read_sql_query("SELECT * FROM population", conn)
    prix_pop = prix_dept.merge(pop, on="code", how="left")
    fig2, ax2 = plt.subplots()
    ax2.scatter(prix_pop["population"], prix_pop["prix_m2_moyen"], alpha=0.6)
    ax2.set_xlabel("Population départementale")
    ax2.set_ylabel("Prix moyen (€ / m²)")
    st.pyplot(fig2)
    

# --- VUE SPARK ANALYSIS ---
elif view == "Spark Analysis":
    st.header("Vue Spark Analysis (pré-agrégation)")

    df_spark = pd.read_sql_query(
        "SELECT dept AS code, nb_transactions, prix_m2_moyen FROM spark_dept_analysis",
        conn
    )
    st.subheader("Résultats Spark par département")
    st.dataframe(df_spark)

    # Pagination Spark
    per_page = st.sidebar.slider("Dépts par page", min_value=5, max_value=50, value=10, step=5)
    total = len(df_spark)
    n_pages = math.ceil(total / per_page)
    page = st.sidebar.number_input("Page", min_value=1, max_value=n_pages, value=1)
    start = (page - 1) * per_page
    end = start + per_page
    df_page = df_spark.iloc[start:end]

    st.subheader(f"Page {page}/{n_pages}")
    st.dataframe(df_page)

    fig3, ax3 = plt.subplots()
    df_page.set_index("code")["prix_m2_moyen"].plot.bar(ax=ax3)
    ax3.set_xlabel("Département")
    ax3.set_ylabel("Prix moyen (€ / m²)")
    ax3.tick_params(axis='x', rotation=45)
    st.pyplot(fig3)

    st.subheader("Nombre de transactions (Spark)")
    fig4, ax4 = plt.subplots()
    df_page.set_index("code")["nb_transactions"].plot.bar(ax=ax4)
    ax4.set_xlabel("Département")
    ax4.set_ylabel("Nombre de transactions")
    ax4.tick_params(axis='x', rotation=45)
    st.pyplot(fig4)

# --- VUE TEXT ANALYSIS ---
elif view == "Text Analysis":
    st.header("Vue Text Analysis (Sentiment & Word Cloud)")

    tdb_path = os.path.join("data", "processed", "comments.json")
    if not os.path.exists(tdb_path):
        st.error(f"Base NoSQL manque : {tdb_path}")
        st.stop()
    db = TinyDB(tdb_path)
    all_docs = db.all()
    total = len(all_docs)
    st.sidebar.markdown(f"**Total commentaires :** {total:,}")

    # Pagination des commentaires
    per_page = st.sidebar.slider("Avis par page", min_value=10, max_value=200, value=50, step=10)
    n_pages = math.ceil(total / per_page)
    page = st.sidebar.number_input("Page", min_value=1, max_value=n_pages, value=1)
    start = (page - 1) * per_page
    end = start + per_page
    page_docs = all_docs[start:end]
    df_page = pd.DataFrame(page_docs)

    st.subheader(f"Commentaires (page {page}/{n_pages})")
    st.dataframe(df_page)

    # Sentiment analysis sur page
    df_page['sentiment'] = df_page['commentaire'].apply(lambda t: TextBlob(t).sentiment.polarity)
    st.subheader("Sentiment des avis (page)")
    st.bar_chart(df_page['sentiment'])

    # Word Cloud sur échantillon
    sample_size = st.sidebar.slider("Commentaires pour word cloud", min_value=100, max_value=5000, value=1000, step=100)
    sampled = random.sample(all_docs, min(sample_size, total))
    text = " ".join([doc['commentaire'] for doc in sampled])
    wc = WordCloud(width=800, height=400, background_color='white').generate(text)
    fig_wc, ax_wc = plt.subplots(figsize=(10,5))
    ax_wc.imshow(wc, interpolation='bilinear')
    ax_wc.axis('off')
    st.subheader(f"Word Cloud (échantillon {len(sampled)})")
    st.pyplot(fig_wc)

# Filtre uniquement taux de chômage
if view == "Indicateurs Socio-éco":
    # On charge d’abord les données brutes pour en déterminer la plage
    df_chom_tmp = pd.read_csv("data/processed/unemployment_dept.csv", dtype=str, encoding="utf-8-sig")
    df_chom_tmp["taux_chomage"] = pd.to_numeric(df_chom_tmp["taux_chomage"].str.replace(",", "."), errors="coerce")
    min_c, max_c = st.sidebar.slider(
        "Taux de chômage (%)",
        float(df_chom_tmp["taux_chomage"].min()),
        float(df_chom_tmp["taux_chomage"].max()),
        (float(df_chom_tmp["taux_chomage"].min()), float(df_chom_tmp["taux_chomage"].max()))
    )

# --- VUE TRANSACTIONS ---
if view == "Transactions":
    st.header("Transactions immobilières")
    st.write("Cette vue affiche les transactions immobilières (en cours d’implémentation).")

# --- VUE ANALYSES SPARK ---
elif view == "Analyses Spark":
    st.header("Analyses Spark")
    st.write("Cette vue affiche les analyses Spark (en cours d’implémentation).")

# --- VUE TEXT MINING ---
elif view == "Text Mining":
    st.header("Text Mining")
    st.write("Cette vue affiche les résultats de text mining (en cours d’implémentation).")

# --- VUE INDICATEURS SOCIO-ÉCO ---
elif view == "Indicateurs Socio-éco":
    st.header("📊 Indicateurs Socio-économiques (INSEE)")

    # Chemins
    unemployment_path = os.path.join("data", "processed", "unemployment_dept.csv")
    income_path       = os.path.join("data", "processed", "income_dept.csv")
    population_path   = os.path.join("data", "processed", "population_dept.csv")
    geojson_path      = os.path.join("data", "raw", "geo", "departements_simplifie.geojson")

    # Vérifications
    for path, name in [(unemployment_path, "chômage"), (income_path, "revenu médian"), (population_path,   "population")]:
        if not os.path.exists(path):
            st.error(f"Données {name} manquantes. Exécutez ingest_insee_{name.replace(' ', '_')}.py.")
            st.stop()

    @st.cache_data
    def load_df(path: str) -> pd.DataFrame:
        df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
        df.columns = df.columns.str.strip().str.replace("\ufeff", "")
        if "dept" in df.columns:
            df = df.rename(columns={"dept": "code"})
        return df

    @st.cache_data
    def load_geo(path: str) -> gpd.GeoDataFrame:
        return gpd.read_file(path)[["code", "geometry"]]

    # Chargement
    df_chom = load_df(unemployment_path)
    df_inc  = load_df(income_path)
    geo     = load_geo(geojson_path)
    df_pop  = load_df(population_path)

    # Conversion numérique
    df_chom["taux_chomage"] = pd.to_numeric(df_chom["taux_chomage"].str.replace(",", "."), errors="coerce")
    df_inc["income_median"] = pd.to_numeric(df_inc["income_median"].str.replace(",", "."), errors="coerce")
    df_pop["population"] = pd.to_numeric(df_pop["population"].str.replace(" ", ""), errors="coerce")

    # Application du filtre taux de chômage
    df_chom = df_chom.query("@min_c <= taux_chomage <= @max_c")

    # Onglets
    tab1, tab2, tab3, tab4 = st.tabs([ "Chômage", "Revenu médian", "Population", "Corrélation"])

    # --- Onglet 1 : Chômage ---
    with tab1:
        st.subheader("Taux de chômage par département (T1 2025)")
        st.dataframe(df_chom)

        # Carte choroplèthe chômage
        geo1 = geo.merge(df_chom, on="code", how="left")
        m1 = folium.Map(location=[46.6, 2.4], zoom_start=5)
        folium.Choropleth(
            geo_data=geo1,
            data=geo1,
            columns=["code", "taux_chomage"],
            key_on="feature.properties.code",
            legend_name="Taux de chômage (%)",
            fill_opacity=0.7,
            line_opacity=0.2,
            nan_fill_color="white"
        ).add_to(m1)
        folium.LayerControl().add_to(m1)
        st.subheader("Carte du taux de chômage (T1 2025)")
        st_folium(m1, width=800, height=600)

        # Histogramme
        fig, ax = plt.subplots()
        ax.hist(df_chom["taux_chomage"].dropna(), bins=30, edgecolor='black')
        ax.set_xlabel("Taux de chômage (%)")
        ax.set_ylabel("Nombre de départements")
        st.subheader("Distribution des taux de chômage")
        st.pyplot(fig)

    # --- Onglet 2 : Revenu médian ---
    with tab2:
        st.subheader("Revenu médian par département (2021, INSEE)")
        st.dataframe(df_inc)

        # Carte choroplèthe revenu médian
        geo2 = geo.merge(df_inc, on="code", how="left")
        m2 = folium.Map(location=[46.6, 2.4], zoom_start=5)
        folium.Choropleth(
            geo_data=geo2,
            data=geo2,
            columns=["code", "income_median"],
            key_on="feature.properties.code",
            legend_name="Revenu médian (€ / an)",
            fill_opacity=0.7,
            line_opacity=0.2,
            nan_fill_color="white"
        ).add_to(m2)
        folium.LayerControl().add_to(m2)
        st.subheader("Carte du revenu médian (2021)")
        st_folium(m2, width=800, height=600)

        # Histogramme
        fig2, ax2 = plt.subplots()
        ax2.hist(df_inc["income_median"].dropna(), bins=30, edgecolor='black')
        ax2.set_xlabel("Revenu médian (€ / an)")
        ax2.set_ylabel("Nombre de départements")
        st.subheader("Distribution du revenu médian")
        st.pyplot(fig2)

    with tab3:  # était tab3 avant, devient tab3 pour Population
        st.subheader("Population par département (INSEE)")
        st.dataframe(df_pop)

        geo3 = geo.merge(df_pop, on="code", how="left")
        m3 = folium.Map(location=[46.6,2.4], zoom_start=5)
        folium.Choropleth(
            geo_data=geo3,
            data=geo3,
            columns=["code", "population"],
            key_on="feature.properties.code",
            legend_name="Population",
            fill_opacity=0.7,
            line_opacity=0.2,
            nan_fill_color="white"
        ).add_to(m3)
        folium.LayerControl().add_to(m3)
        st.subheader("Carte de la population")
        st_folium(m3, width=800, height=600)

        fig3, ax3 = plt.subplots()
        ax3.hist(df_pop["population"].dropna(), bins=30, edgecolor='black')
        ax3.set_xlabel("Population")
        ax3.set_ylabel("Nombre de départements")
        st.subheader("Distribution de la population")
        st.pyplot(fig3)
        
    # --- Onglet 3 : Corrélation taux de chômage / revenu médian ---
    with tab4:
        st.subheader("Corrélation taux de chômage ↔ revenu médian")
        df_corr = df_chom.merge(df_inc, on="code", how="inner")

        fig3, ax3 = plt.subplots()
        ax3.scatter(df_corr["income_median"], df_corr["taux_chomage"], alpha=0.7)
        slope, intercept, r_value, p_value, std_err = linregress(
            df_corr["income_median"], df_corr["taux_chomage"]
        )
        xx = np.linspace(df_corr["income_median"].min(), df_corr["income_median"].max(), 100)
        ax3.plot(xx, intercept + slope * xx, linestyle='--',
                 label=f"R²={r_value**2:.2f}, p={p_value:.3f}")
        ax3.set_xlabel("Revenu médian (€ / an)")
        ax3.set_ylabel("Taux de chômage (%)")
        ax3.legend()
        st.pyplot(fig3)

        st.info(f"Coefficient de corrélation (Pearson) : {r_value:.3f}")
conn.close()
