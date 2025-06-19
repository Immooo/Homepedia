import streamlit as st
import pandas as pd
import geopandas as gpd
from streamlit_folium import st_folium
import folium
import os

# 1. Titre et sidebar
st.set_page_config(page_title="Homepedia – Prix Immobilier France", layout="wide")
st.title("🏠 Homepedia – Carte des prix moyens au m² par département")

# Filtres
st.sidebar.header("Filtres")
min_date = pd.to_datetime("2024-01-01")
max_date = pd.to_datetime("2024-12-31")
date_range = st.sidebar.date_input("Période", [min_date, max_date], min_value=min_date, max_value=max_date)
logement_types = ["Tous"] + sorted(pd.read_csv("data/processed/transactions_2024.csv")["type_local"].dropna().unique().tolist())
choix_type = st.sidebar.selectbox("Type de logement", logement_types)

# 2. Charger les données et filtrer
@st.cache_data
def load_data():
    df = pd.read_csv(
        os.path.join("data", "processed", "transactions_2024.csv"),
        parse_dates=["date_mutation"],
        dtype={"code_postal": str},
    )
    df["valeur_fonciere"] = (
        df["valeur_fonciere"].str.replace(" ", "").str.replace(",", ".", regex=False).astype(float)
    )
    df["surface_reelle_bati"] = pd.to_numeric(df["surface_reelle_bati"], errors="coerce")
    df = df[df["surface_reelle_bati"] > 0]
    df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]
    df["dept"] = df["code_postal"].str[:2]
    return df

data = load_data()
# Appliquer filtres
mask = (data["date_mutation"] >= pd.to_datetime(date_range[0])) & (data["date_mutation"] <= pd.to_datetime(date_range[1]))
if choix_type != "Tous":
    mask &= data["type_local"] == choix_type
df_filt = data[mask]

# 3. Calcul du prix moyen
prix_dept = df_filt.groupby("dept")["prix_m2"].mean().reset_index().rename(columns={"dept": "code", "prix_m2": "prix_m2_moyen"})

# 4. Charger GeoJSON
geo = gpd.read_file(os.path.join("data", "raw", "geo", "departements_simplifie.geojson"))[["code", "geometry"]]
geo = geo.merge(prix_dept, on="code", how="left")

# 5. Construire la carte
m = folium.Map(location=[46.6, 2.4], zoom_start=5)
folium.Choropleth(
    geo_data=geo,
    data=geo,
    columns=["code", "prix_m2_moyen"],
    key_on="feature.properties.code",
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name="Prix moyen (€ / m²)",
    nan_fill_color="white",
).add_to(m)

st.subheader("Carte interactive")
st_folium(m, width=800, height=600)

# 6. Statistiques globales
st.sidebar.markdown(f"**Transactions affichées :** {len(df_filt)}")
st.sidebar.markdown(f"**Prix moyen (tous départements) :** {df_filt['prix_m2'].mean():.0f} €/m²")