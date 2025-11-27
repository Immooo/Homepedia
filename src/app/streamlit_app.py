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
import seaborn as sns
from app.db.mongo_client import get_mongo_collection

COLS_NICE = {
    "code": "Département", "dept": "Département", "code_region": "Région",
    "nb_transactions": "Nombres de transactions", "prix_m2_moyen": "Prix moyen €/m²",
    "prix_m2": "Prix €/m²", "surface_reelle_bati": "Surface bâtie m²",
    "valeur_fonciere": "Valeur foncière €",
    "population": "Population", "income_median": "Revenu médian €",
    "taux_chomage": "Taux chômage %", "poverty_rate": "Taux pauvreté %",
    "income"      : "Revenu médian € ",
    "unemployment": "Taux chômage % ",
    "poverty"     : "Taux pauvreté % "
}

def pretty(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={k: v for k, v in COLS_NICE.items() if k in df.columns})

def show(df: pd.DataFrame, n: int | None = None):
    st.dataframe(pretty(df if n is None else df.head(n)))

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
        "Méthodologie"
    ]
)

# 3. Connexion à la base SQLite
DB_PATH = os.path.join("data", "homepedia.db")
conn = sqlite3.connect(DB_PATH)

# === VUE STANDARD ===
if view == "Standard":
    st.header("Transactions immobilières (live SQL + Pandas)")

    # --- Connexions rapides ---
    def sql_scalar(query: str):
        return conn.execute(query).fetchone()[0]

    # --- Période ---
    st.sidebar.subheader("Filtres Transactions")
    min_date = pd.to_datetime("2024-01-01")
    max_date = pd.to_datetime("2024-12-31")
    raw_dates = st.sidebar.date_input(
        "Période",
        [min_date.date(), max_date.date()],
        min_value=min_date.date(),
        max_value=max_date.date()
    )
    
    if isinstance(raw_dates, tuple):
        start_date = pd.to_datetime(raw_dates[0])
        end_date   = pd.to_datetime(raw_dates[1] if len(raw_dates) > 1 else raw_dates[0])
    else:  
        start_date = end_date = pd.to_datetime(raw_dates)

    # --- Type de bien (liste depuis la base) ---
    type_list = ["Tous"] + [
        r[0] for r in conn.execute(
            "SELECT DISTINCT type_local FROM transactions WHERE type_local IS NOT NULL ORDER BY 1"
        ).fetchall()
    ]
    choix_type = st.sidebar.selectbox("Type de logement", type_list)

    # --- Min / Max prix_m2 globaux (pour le slider) ---
    pmin_glob, pmax_glob = conn.execute("""
        SELECT
            MIN(valeur_fonciere / surface_reelle_bati),
            MAX(valeur_fonciere / surface_reelle_bati)
        FROM transactions
        WHERE surface_reelle_bati > 0
          AND valeur_fonciere IS NOT NULL
    """).fetchone()

    price_range = st.sidebar.slider(
        "Prix au m²",
        int(pmin_glob), int(pmax_glob),
        (int(pmin_glob), int(pmax_glob))
    )

    # --- Chargement filtré ---
    @st.cache_data(show_spinner=False)
    def load_transactions(start, end, type_sel, pmin, pmax):
        start_iso = start.strftime("%Y-%m-%d")
        end_iso   = end.strftime("%Y-%m-%d")

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

        # Filtre sur le type de bien (optionnel)
        if type_sel != "Tous":
            query += " AND type_local = ?"
            params.append(type_sel)

        # 🔹 Création du DataFrame
        df = pd.read_sql_query(
            query,
            conn,
            params=params,
            parse_dates=["date_mutation"]
        )

        # 🔹 Nettoyage du code postal (si la colonne existe)
        if "code_postal" in df.columns:
            df["code_postal"] = (
                df["code_postal"]
                .astype(str)
                .str.replace(r"\.0$", "", regex=True)
            )

        # 🔹 Sécurisation du dept si nécessaire
        if "dept" not in df.columns and "code_postal" in df.columns:
            df["dept"] = df["code_postal"].str[:2].str.zfill(2)

        return df

    tx = load_transactions(start_date, end_date, choix_type, price_range[0], price_range[1])

    # --- KPIs & export ---
    col1, col2, col3 = st.columns(3)
    col1.metric("Transactions chargées", f"{len(tx):,}")
    col2.metric("Surface médiane (m²)", f"{tx['surface_reelle_bati'].median():.1f}")
    col3.metric("Prix moyen €/m²", f"{tx['prix_m2'].mean():.2f}")

    st.download_button(
        "📥 Exporter ces transactions (CSV)",
        tx.to_csv(index=False).encode("utf-8"),
        file_name="transactions_filtrees.csv",
        mime="text/csv"
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
    geo = gpd.read_file("data/raw/geo/departements_simplifie.geojson")[["code", "geometry"]]
    geo = geo.merge(prix_dept, on="code", how="left")

    if st.checkbox("Afficher la carte", value=True):
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
                nan_fill_color="white"
            ).add_to(m)
            st.subheader("Carte du prix moyen au m²")
            st_folium(m, width=800, height=600)

    # --- Histogramme ---
    st.subheader("Distribution des prix au m²")
    fig1, ax1 = plt.subplots()
    ax1.hist(tx["prix_m2"], bins="auto", range=price_range, edgecolor="black")
    ax1.set_xlim(price_range)             
    ax1.set_xlabel("Prix (€ / m²)")
    ax1.set_ylabel("Nombre de transactions")
    st.pyplot(fig1, use_container_width=True)

    # --- Box-plot ---
    st.subheader("Dispersion prix/m² par type de bien")
    fig_box, ax_box = plt.subplots(figsize=(9, 4))
    tx.boxplot(column="prix_m2", by="type_local", ax=ax_box, showfliers=False)
    ax_box.set_xlabel("")
    ax_box.set_ylabel("€ / m²")
    ax_box.set_title("")
    ax_box.tick_params(axis="x", labelrotation=45)
    ax_box.set_xticklabels(
        [lab.get_text().replace(" ", "\n", 1) for lab in ax_box.get_xticklabels()],
        ha="right", fontsize=8
    )
    st.pyplot(fig_box)

    # --- Scatter population ---
    pop = pd.read_sql_query("SELECT * FROM population", conn)
    prix_pop = prix_dept.merge(pop, on="code", how="left")

    st.subheader("Population vs Prix moyen")
    fig2, ax2 = plt.subplots()
    ax2.scatter(prix_pop["population"], prix_pop["prix_m2_moyen"], alpha=0.6)
    ax2.set_xlabel("Population départementale")
    ax2.set_ylabel("Prix moyen (€ / m²)")
    import matplotlib.ticker as mticker
    ax2.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f} M"))
    st.pyplot(fig2)

# === VUE SPARK ANALYSIS ===
elif view == "Spark Analysis":
    st.header("Vue Spark Analysis (pré-agrégation)")
    df_spark = pd.read_sql_query(
        "SELECT dept AS code, nb_transactions, prix_m2_moyen FROM spark_dept_analysis",
        conn
    )
    df_spark["prix_m2_moyen"] = df_spark["prix_m2_moyen"].round(0).astype(int)
    st.subheader("Résultats Spark par département")
    show(df_spark)
    per_page = st.sidebar.slider("Dépts par page", 5, 50, 10, 5)
    n_pages = math.ceil(len(df_spark) / per_page)
    page = st.sidebar.number_input("Page", 1, n_pages, 1)
    start = (page-1)*per_page
    df_page = df_spark.iloc[start:start+per_page]
    st.subheader(f"Page {page}/{n_pages}")
    show(df_page)
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
    show(df_page)
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
    df_chom_tmp = pd.read_parquet("data/processed/unemployment_dept.parquet")
                                  
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
    unemployment_path = "data/processed/unemployment_dept.parquet"
    income_path       = "data/processed/income_dept.parquet"
    population_path   = "data/processed/population_dept.parquet"
    poverty_path      = "data/processed/poverty_dept.parquet"
    geojson_path      = "data/raw/geo/departements_simplifie.geojson"

    # Vérifications
    for path, name in [
        (unemployment_path, "chômage"),
        (income_path,       "revenu médian"),
        (population_path,   "population"),
        (poverty_path,      "pauvreté")
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
        show(df_chom)
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
        show(df_inc)
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
        show(df_pop)
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
        show(df_pov)
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
        labels = [COLS_NICE.get(c, c) for c in corr.columns]
        ax6.set_xticklabels(labels, rotation=45, ha="right")
        ax6.set_yticks(range(len(corr)))
        ax6.set_yticklabels(labels)
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
    show(df_region)

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

    st.subheader("Matrice de corrélations régionales")

    @st.cache_data
    def compute_region_corr(df: pd.DataFrame) -> pd.DataFrame:
        features = ["prix_m2_moyen", "population", "income_median", "taux_chomage", "poverty_rate"]
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
            vmin=-1, vmax=1,
            square=True,
            cbar_kws={"shrink": 0.75},
            ax=ax
        )
        # Ajustement dynamique de la couleur des annotations
        for text in ax.texts:
            val = float(text.get_text())
            text.set_color("white" if abs(val) > 0.5 else "black")
        labels_reg = [COLS_NICE.get(c, c) for c in corr_reg.columns]
        ax.set_xticklabels(labels_reg, rotation=45, ha="right")
        ax.set_yticklabels(labels_reg)
        st.pyplot(fig)

# === VUE MÉTHODOLOGIE ===
elif view == "Méthodologie":
    st.header("📚 Méthodologie & Choix techniques")

    st.markdown("""
    ### Pré-processing des données
    - **Transactions DVF 2024** : nettoyage des valeurs foncières / surfaces, suppression des valeurs aberrantes, extraction du code département.
    - **Indicateurs INSEE (revenu, chômage, pauvreté, population)** : filtrage des mesures fiables, conversion numérique, agrégation au niveau départemental.
    - Tous les scripts sont disponibles dans `src/backend/ingest_*.py`.

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
    - **PySpark** : agrégations rapides sur ~2 M de lignes DVF  
    - **folium** : cartes choroplèthes interactives

    ### Architecture
    ```text
    CSV / Scraping  →  Scripts ETL  →  SQLite (homepedia.db)
                          │
                          └─► Streamlit 5 vues  →  Docker
    ```

    ### Limites & pistes
    - Ajouter indicateurs démographie/âge  
    - Tests unitaires sur chaque ingestion  
    - Déploiement cloud (railway.app, Render, etc.)
    """)

def show_mongo_debug():
    col = get_mongo_collection("properties")  
    doc = col.find_one()
    st.write("Exemple document Mongo :", doc)

# Clôture
conn.close()