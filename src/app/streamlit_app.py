import os
import sqlite3
import math
import random

import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
import matplotlib.pyplot as plt
from streamlit_folium import st_folium
from textblob import TextBlob
from wordcloud import WordCloud
from tinydb import TinyDB

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

# --- VUE INDICATEURS SOCIO-ECO ---
elif view == "Indicateurs Socio-éco":
    st.header("Indicateurs Socio-économiques (INSEE)")

    # Chargement des données de chômage
    unemployment_path = os.path.join("data", "processed", "unemployment_dept.csv")
    if not os.path.exists(unemployment_path):
        st.error("Données chômage manquantes. Exécute ingest_insee_unemployment.py.")
        st.stop()

    df_chomage = pd.read_csv(unemployment_path, dtype={'code': str})

    # Affichage du tableau
    st.subheader("Taux de chômage par département (T1 2025)")
    st.dataframe(df_chomage)

    # Carte choroplèthe
    geo = gpd.read_file(os.path.join("data","raw","geo","departements_simplifie.geojson"))[["code","geometry"]]
    geo = geo.merge(df_chomage, on="code", how="left")
    m = folium.Map(location=[46.6,2.4], zoom_start=5)
    folium.Choropleth(
        geo_data=geo,
        data=geo,
        columns=["code", "taux_chomage"],
        key_on="feature.properties.code",
        legend_name="Taux de chômage (%)",
        fill_opacity=0.7,
        line_opacity=0.2,
        nan_fill_color="white",
    ).add_to(m)
    folium.LayerControl().add_to(m)
    st.subheader("Carte du taux de chômage (T1 2025)")
    st_folium(m, width=800, height=600)

    # Histogramme
    fig, ax = plt.subplots()
    ax.hist(df_chomage["taux_chomage"].dropna(), bins=30, color='tab:blue', edgecolor='black')
    ax.set_xlabel("Taux de chômage (%)")
    ax.set_ylabel("Nombre de départements")
    st.subheader("Distribution des taux de chômage")
    st.pyplot(fig)

    # Corrélation chômage / prix immobilier
    prix_path = os.path.join("data", "homepedia.db")
    # On va chercher les prix moyens par département dans la base (Spark table)
    prix_dept = pd.read_sql_query(
        "SELECT dept AS code, prix_m2_moyen FROM spark_dept_analysis", conn
    )
    df_corr = prix_dept.merge(df_chomage, on="code", how="inner").dropna()

    st.subheader("Corrélation Taux de Chômage / Prix moyen immobilier (département)")
    st.dataframe(df_corr[["code", "libelle", "taux_chomage", "prix_m2_moyen"]])

    fig_corr, ax_corr = plt.subplots()
    ax_corr.scatter(df_corr["taux_chomage"], df_corr["prix_m2_moyen"], alpha=0.7)
    ax_corr.set_xlabel("Taux de chômage (%)")
    ax_corr.set_ylabel("Prix moyen (€ / m²)")
    ax_corr.set_title("Lien entre taux de chômage et prix immobilier par département")
    st.pyplot(fig_corr)
    st.info(
        "Ce graphique permet de visualiser le lien entre la situation économique (taux de chômage) "
        "et le marché immobilier (prix moyen au m²) pour chaque département."
    )

conn.close()
