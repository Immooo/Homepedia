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
from typing import Any, List
import matplotlib.ticker as mticker

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
        "Région"
    ]
)

# 3. Connexion à la base SQLite
DB_PATH = os.path.join("data", "homepedia.db")
conn = sqlite3.connect(DB_PATH)

# === VUE STANDARD ===
if view == "Standard":
    st.header("Transactions immobilières (live SQL + Pandas)")

    # Filtres
    st.sidebar.subheader("Filtres Transactions")
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
        df = pd.read_sql_query(
            "SELECT * FROM transactions", conn, parse_dates=["date_mutation"]
        )
        df["surface_reelle_bati"] = pd.to_numeric(
            df["surface_reelle_bati"], errors="coerce"
        )
        df["valeur_fonciere"] = pd.to_numeric(
            df["valeur_fonciere"], errors="coerce"
        )
        df = df[df["surface_reelle_bati"] > 0]
        df = df[df["valeur_fonciere"].notna()]
        df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]
        df["dept"] = df["code_postal"].astype(str).str[:2].str.zfill(2)
        return df

    tx = load_transactions()
    st.subheader("Aperçu des transactions brutes")
    st.dataframe(tx.head(10))

    # Filtre Type
    types = ["Tous"] + sorted(tx["type_local"].dropna().unique().tolist())
    choix_type = st.sidebar.selectbox("Type de logement", types)

    # Filtre Prix
    pmin, pmax = tx["prix_m2"].quantile([0.01, 0.99])
    price_range = st.sidebar.slider(
        "Prix au m²", int(pmin), int(pmax), (int(pmin), int(pmax))
    )

    # Application des filtres
    mask = (
        (tx["date_mutation"] >= start_date) &
        (tx["date_mutation"] <= end_date)
    )
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
    geo = gpd.read_file(
        os.path.join("data","raw","geo","departements_simplifie.geojson")
    )[["code","geometry"]]
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
    st.subheader("Carte du prix moyen au m²")
    st_folium(m, width=800, height=600)

    # Histogramme
    fig1, ax1 = plt.subplots()
    ax1.hist(
        df_filt["prix_m2"].dropna(), bins=50,
        range=(price_range[0], price_range[1]), edgecolor='black'
    )
    ax1.set_xlabel("Prix (€ / m²)")
    ax1.set_ylabel("Nombre de transactions")
    st.subheader("Distribution des prix au m²")
    st.pyplot(fig1)

    # Scatter population
    pop = pd.read_sql_query("SELECT * FROM population", conn)
    prix_pop = prix_dept.merge(pop, on="code", how="left")
    fig2, ax2 = plt.subplots()
    ax2.scatter(prix_pop["population"], prix_pop["prix_m2_moyen"], alpha=0.6)
    ax2.set_xlabel("Population départementale")
    ax2.set_ylabel("Prix moyen (€ / m²)")
    st.subheader("Population vs Prix moyen")
    st.pyplot(fig2)

# === VUE SPARK ANALYSIS ===
elif view == "Spark Analysis":
    st.header("Vue Spark Analysis (pré-agrégation)")
    df_spark = pd.read_sql_query(
        "SELECT dept AS code, nb_transactions, prix_m2_moyen FROM spark_dept_analysis",
        conn
    )
    st.subheader("Résultats Spark par département")
    st.dataframe(df_spark)
    per_page = st.sidebar.slider("Dépts par page", 5, 50, 10, 5)
    n_pages = math.ceil(len(df_spark) / per_page)
    page = st.sidebar.number_input("Page", 1, n_pages, 1)
    start = (page-1)*per_page
    df_page = df_spark.iloc[start:start+per_page]
    st.subheader(f"Page {page}/{n_pages}")
    st.dataframe(df_page)
    fig3, ax3 = plt.subplots()
    df_page.set_index("code")["prix_m2_moyen"].plot.bar(ax=ax3)
    ax3.set_xlabel("Département")
    ax3.set_ylabel("Prix moyen (€ / m²)")
    ax3.tick_params(axis='x', rotation=45)
    st.pyplot(fig3)

# === VUE TEXT ANALYSIS ===
elif view == "Text Analysis":
    st.header("Vue Text Analysis (Sentiment & Word Cloud)")
    tdb_path = os.path.join("data", "processed", "comments.json")
    if not os.path.exists(tdb_path):
        st.error(f"Base NoSQL manque : {tdb_path}")
        st.stop()
    db = TinyDB(tdb_path)
    docs = db.all()
    st.sidebar.markdown(f"**Total commentaires :** {len(docs):,}")
    per_page = st.sidebar.slider("Avis par page", 10, 200, 50, 10)
    page = st.sidebar.number_input("Page", 1, math.ceil(len(docs)/per_page), 1)
    subset = docs[(page-1)*per_page : page*per_page]
    df_page = pd.DataFrame(subset)
    st.subheader(f"Commentaires (page {page})")
    st.dataframe(df_page)
    df_page['sentiment'] = df_page['commentaire'].map(lambda t: TextBlob(t).sentiment.polarity)
    st.subheader("Sentiment des avis")
    st.bar_chart(df_page['sentiment'])
    sample_n = st.sidebar.slider("Échantillon Word Cloud", 100, 5000, 1000, 100)
    sampled = random.sample(docs, sample_n)
    text = " ".join(d['commentaire'] for d in sampled)
    wc = WordCloud(width=800, height=400, background_color='white').generate(text)
    fig_wc, ax_wc = plt.subplots(figsize=(10,5))
    ax_wc.imshow(wc, interpolation='bilinear')
    ax_wc.axis('off')
    st.subheader(f"Word Cloud (n={sample_n})")
    st.pyplot(fig_wc)

# === VUE SOCIO-ÉCO ===
elif view == "Indicateurs Socio-éco":
    st.header("📊 Indicateurs Socio-économiques (INSEE)")

    # Filtres Socio-éco
    df_chom_tmp = pd.read_csv(
        "data/processed/unemployment_dept.csv",
        dtype=str, encoding="utf-8-sig"
    )
    df_chom_tmp["taux_chomage"] = pd.to_numeric(
        df_chom_tmp["taux_chomage"].str.replace(",", "."), errors="coerce"
    )
    min_c, max_c = st.sidebar.slider(
        "Taux de chômage (%)",
        float(df_chom_tmp["taux_chomage"].min()),
        float(df_chom_tmp["taux_chomage"].max()),
        (float(df_chom_tmp["taux_chomage"].min()), float(df_chom_tmp["taux_chomage"].max()))
    )

    # Chemins
    unemployment_path = os.path.join("data", "processed", "unemployment_dept.csv")
    income_path       = os.path.join("data", "processed", "income_dept.csv")
    population_path   = os.path.join("data", "processed", "population_dept.csv")
    poverty_path      = os.path.join("data", "processed", "poverty_dept.csv")
    geojson_path      = os.path.join("data", "raw", "geo", "departements_simplifie.geojson")

    # Vérifications
    for path, name in [
        (unemployment_path, "chômage"),
        (income_path,       "revenu médian"),
        (population_path,   "population"),
        (poverty_path,      "pauvreté")
    ]:
        if not os.path.exists(path):
            st.error(f"Données {name} manquantes.")
            st.stop()

    # Chargement
    @st.cache_data
    def load_df(path):
        df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
        df.columns = df.columns.str.strip().str.replace("\ufeff", "")
        return df

    @st.cache_data
    def load_geo(path):
        return gpd.read_file(path)[["code","geometry"]]

    df_chom = load_df(unemployment_path)
    df_inc  = load_df(income_path)
    df_pop  = load_df(population_path)
    df_pov  = load_df(poverty_path)
    geo     = load_geo(geojson_path)

    # Conversion numérique
    df_chom["taux_chomage"] = pd.to_numeric(df_chom["taux_chomage"].str.replace(",","."), errors="coerce")
    df_inc["income_median"]   = pd.to_numeric(df_inc["income_median"].str.replace(",","."), errors="coerce")
    df_pop["population"]      = pd.to_numeric(df_pop["population"].str.replace(" ",""), errors="coerce")
    df_pov["poverty_rate"]    = pd.to_numeric(df_pov["poverty_rate"].str.replace(",","."), errors="coerce")

    # Filtre chômage
    df_chom = df_chom.query("@min_c <= taux_chomage <= @max_c")

    # Onglets
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "Chômage",
        "Revenu médian",
        "Population",
        "Pauvreté",
        "Corrélation",
        "Matrice corrélations"
    ])

    # --- Chômage ---
    with tab1:
        st.subheader("Taux de chômage (T1 2025)")
        st.dataframe(df_chom)
        geo1 = geo.merge(df_chom.rename(columns={"code":"code"}), on="code", how="left")
        m1 = folium.Map(location=[46.6,2.4], zoom_start=5)
        folium.Choropleth(
            geo_data=geo1,
            data=geo1,
            columns=["code","taux_chomage"],
            key_on="feature.properties.code",
            legend_name="Taux de chômage (%)"
        ).add_to(m1)
        st_folium(m1, width=800, height=600)

    # --- Revenu médian ---
    with tab2:
        st.subheader("Revenu médian (2021)")
        st.dataframe(df_inc)
        geo2 = geo.merge(df_inc, on="code", how="left")
        m2 = folium.Map(location=[46.6,2.4], zoom_start=5)
        folium.Choropleth(
            geo_data=geo2,
            data=geo2,
            columns=["code","income_median"],
            key_on="feature.properties.code",
            legend_name="Revenu médian (€ / an)"
        ).add_to(m2)
        st_folium(m2, width=800, height=600)

    # --- Population ---
    with tab3:
        st.subheader("Population")
        st.dataframe(df_pop)
        geo3 = geo.merge(df_pop, on="code", how="left")
        m3 = folium.Map(location=[46.6,2.4], zoom_start=5)
        folium.Choropleth(
            geo_data=geo3,
            data=geo3,
            columns=["code","population"],
            key_on="feature.properties.code",
            legend_name="Population" 
        ).add_to(m3)
        st_folium(m3, width=800, height=600)
        fig3, ax3 = plt.subplots()
        ax3.hist(df_pop["population"].dropna(), bins=30, edgecolor='black')
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
        st.dataframe(df_pov)
        geo4 = geo.merge(df_pov, on="code", how="left")
        m4 = folium.Map(location=[46.6,2.4], zoom_start=5)
        folium.Choropleth(
            geo_data=geo4,
            data=geo4,
            columns=["code","poverty_rate"],
            key_on="feature.properties.code",
            legend_name="Taux de pauvrété (%)"
        ).add_to(m4)
        st_folium(m4, width=800, height=600)
        fig4, ax4 = plt.subplots()
        ax4.hist(df_pov["poverty_rate"].dropna(), bins=30, edgecolor='black')
        ax4.set_xlabel("Taux de pauvreté (%)")
        ax4.set_ylabel("Nombre de départements")
        st.pyplot(fig4)

    # --- Corrélation ---
    with tab5:
        st.subheader("Corrélation chômage ↔ revenu")
        df_corr = df_chom.merge(df_inc, on="code")
        fig5, ax5 = plt.subplots()
        ax5.scatter(df_corr["income_median"], df_corr["taux_chomage"], alpha=0.7)
        slope, intercept, r, p, se = linregress(df_corr["income_median"], df_corr["taux_chomage"])
        xx = np.linspace(df_corr["income_median"].min(), df_corr["income_median"].max(), 100)
        ax5.plot(xx, intercept + slope*xx, linestyle='--', label=f"R²={r**2:.2f}")
        ax5.set_xlabel("Revenu médian (€ / an)")
        ax5.set_ylabel("Taux de chômage (%)")
        ax5.legend()
        st.pyplot(fig5)

    # --- Matrice corrélation ---
    with tab6:
        st.subheader("Matrice de corrélations multiples")
        df_all = df_chom.merge(df_inc, on="code").merge(df_pop, on="code").merge(df_pov, on="code")
        corr = df_all[["taux_chomage","income_median","population","poverty_rate"]].corr()
        fig6, ax6 = plt.subplots()
        cax = ax6.imshow(corr, vmin=-1, vmax=1)
        ax6.set_xticks(range(len(corr)))
        ax6.set_xticklabels(corr.columns, rotation=45, ha="right")
        ax6.set_yticks(range(len(corr)))
        ax6.set_yticklabels(corr.index)
        for i in range(len(corr)):
            for j in range(len(corr)):
                val = corr.iat[i,j]
                color = "white" if abs(val)>0.5 else "black"
                ax6.text(j, i, f"{val:.2f}", ha="center", va="center", color=color)
        fig6.colorbar(cax, ax=ax6, fraction=0.046, pad=0.04)
        st.pyplot(fig6)

# === VUE RÉGION ===
elif view == "Région":
    st.header("🌍 Indicateurs par Région")

    # 1) Chargement en cache des données régionales
    @st.cache_data
    def load_region_df():
        df = pd.read_sql_query("SELECT * FROM region_analysis", conn)
        # zfill sur code_region si nécessaire
        df["code_region"] = df["code_region"].astype(str).str.zfill(2)
        return df

    # 2) Lecture + simplification du GeoJSON en cache
    @st.cache_data
    def load_region_geo(path):
        geo = gpd.read_file(path)[["code","geometry"]]
        # simplification : tolérance ajustable (en degrés décimaux)
        geo["geometry"] = geo["geometry"].simplify(tolerance=0.02, preserve_topology=True)
        return geo

    df_region = load_region_df()
    geo_reg   = load_region_geo(os.path.join("data","raw","geo","regions.geojson"))

    st.subheader("Aperçu des données régionales")
    st.dataframe(df_region)

    # 3) Slider population (inchangé)
    pop_min = int(df_region["population"].min())
    pop_max = int(df_region["population"].max())
    borne_min = (pop_min // 2_000_000) * 2_000_000
    borne_max = ((pop_max // 2_000_000) + 1) * 2_000_000
    st.sidebar.subheader("Plage de population (pas 2 M)")
    x_range = st.sidebar.slider(
        "Population", 
        min_value=borne_min,
        max_value=borne_max,
        value=(borne_min, borne_max),
        step=2_000_000,
        format="%d"
    )

    # 4) Merge + carte Folium
    geo_plot = geo_reg.merge(
        df_region.rename(columns={"code_region":"code"}), on="code", how="left"
    )
    m_reg = folium.Map(location=[46.6,2.4], zoom_start=5)
    folium.Choropleth(
        geo_data=geo_plot,
        data=geo_plot,
        columns=["code","prix_m2_moyen"],
        key_on="feature.properties.code",
        legend_name="Prix moyen (€ / m²)",
        fill_opacity=0.7,
        line_opacity=0.2,
        nan_fill_color="white"
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

# 7) Matrice de corrélations régionales (placée à la fin)
st.subheader("Matrice de corrélations régionales")

@st.cache_data
def compute_region_corr(df: pd.DataFrame) -> pd.DataFrame:
    possibles = ["prix_m2_moyen", "population", "income_median", "taux_chomage", "poverty_rate"]
    cols = [col for col in possibles if col in df.columns]
    
    if len(cols) < 2:
        st.warning("Pas assez de colonnes numériques disponibles pour calculer la corrélation.")
        return pd.DataFrame()
    
    return df[cols].corr()

corr_reg = compute_region_corr(df_region)

if not corr_reg.empty:
    fig_corr, ax_corr = plt.subplots()
    cax = ax_corr.imshow(corr_reg, vmin=-1, vmax=1, cmap='coolwarm')
    ax_corr.set_xticks(range(len(corr_reg)))
    ax_corr.set_xticklabels(corr_reg.columns, rotation=45, ha="right")
    ax_corr.set_yticks(range(len(corr_reg)))
    ax_corr.set_yticklabels(corr_reg.index)
    
    # annotations
    for i in range(len(corr_reg)):
        for j in range(len(corr_reg)):
            val = corr_reg.iat[i, j]
            color = "white" if abs(val) > 0.5 else "black"
            ax_corr.text(j, i, f"{val:.2f}", ha="center", va="center", color=color)
    
    fig_corr.colorbar(cax, ax=ax_corr, fraction=0.046, pad=0.04)
    st.pyplot(fig_corr)
else:
    st.info("Corrélation impossible : données socio-économiques manquantes.")

# Clôture
conn.close()