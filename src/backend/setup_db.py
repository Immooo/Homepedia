import os
import sqlite3
import math

import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
import matplotlib.pyplot as plt
from streamlit_folium import st_folium
from textblob import TextBlob
from wordcloud import WordCloud

# 1. Config page et choix de la vue
st.set_page_config(page_title="Homepedia – Analyses Immobilier France", layout="wide")
st.title("🏠 Homepedia – Analyses Immobilier France")

view = st.sidebar.radio("Choix de la vue", ["Standard", "Spark Analysis", "Text Analysis"])

# 2. Connexion SQLite
DB_PATH = os.path.join("data", "homepedia.db")
conn = sqlite3.connect(DB_PATH)

# --- Vue Standard (inchangée) ---
if view == "Standard":
    st.header("Vue Standard (live SQL + Pandas)")
    # ... (Standard view code remains) ...

# --- Vue Spark Analysis ---
elif view == "Spark Analysis":
    st.header("Vue Spark Analysis (pré-agrégation)")
    # ... (Spark Analysis code remains) ...

# --- Vue Text Analysis ---
else:
    st.header("Vue Text Analysis (Sentiment & Word Cloud)")

    # 3. Chargement des commentaires
    comments_path = os.path.join("data", "processed", "comments.csv")
    if not os.path.exists(comments_path):
        st.error(f"Fichier de commentaires manquant : {comments_path}")
        st.stop()
    df_comments = pd.read_csv(comments_path)
    st.subheader("Aperçu des commentaires")
    st.dataframe(df_comments.head(10))

    # 4. Sentiment analysis
    def sentiment_score(text):
        return TextBlob(text).sentiment.polarity
    df_comments['sentiment'] = df_comments['commentaire'].astype(str).apply(sentiment_score)
    st.subheader("Sentiment des commentaires")
    st.write(df_comments[['commentaire','sentiment']].head(10))

    # 5. Word Cloud
    text = " ".join(df_comments['commentaire'].dropna().tolist())
    wc = WordCloud(width=800, height=400, background_color='white').generate(text)
    fig_wc, ax_wc = plt.subplots(figsize=(10,5))
    ax_wc.imshow(wc, interpolation='bilinear')
    ax_wc.axis('off')
    st.subheader("Word Cloud des commentaires")
    st.pyplot(fig_wc)

conn.close()