# File: src/backend/setup_db.py
import os
import sqlite3

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st
from textblob import TextBlob
from wordcloud import WordCloud

from src.backend.logging_setup import setup_logging

logger = setup_logging()

# 1. Config page et choix de la vue
st.set_page_config(page_title="Homepedia – Analyses Immobilier France", layout="wide")
st.title("🏠 Homepedia – Analyses Immobilier France")

view = st.sidebar.radio(
    "Choix de la vue", ["Standard", "Spark Analysis", "Text Analysis"]
)
logger.info("Vue sélectionnée: %s", view)

# 2. Connexion SQLite
DB_PATH = os.path.join("data", "homepedia.db")
logger.info("Ouverture de la base SQLite: %s", DB_PATH)

try:
    conn = sqlite3.connect(DB_PATH)

    # --- Vue Standard (inchangée) ---
    if view == "Standard":
        st.header("Vue Standard (live SQL + Pandas)")
        logger.info("Affichage de la vue Standard")
        # ... (Standard view code remains) ...

    # --- Vue Spark Analysis ---
    elif view == "Spark Analysis":
        st.header("Vue Spark Analysis (pré-agrégation)")
        logger.info("Affichage de la vue Spark Analysis")
        # ... (Spark Analysis code remains) ...

    # --- Vue Text Analysis ---
    else:
        st.header("Vue Text Analysis (Sentiment & Word Cloud)")
        logger.info("Affichage de la vue Text Analysis")

        # 3. Chargement des commentaires
        comments_path = os.path.join("data", "processed", "comments.csv")
        if not os.path.exists(comments_path):
            msg = f"Fichier de commentaires manquant : {comments_path}"
            logger.error(msg)
            st.error(msg)
            st.stop()

        logger.info("Lecture des commentaires: %s", comments_path)
        df_comments = pd.read_csv(comments_path)
        st.subheader("Aperçu des commentaires")
        st.dataframe(df_comments.head(10))

        # 4. Sentiment analysis
        def sentiment_score(text):
            return TextBlob(text).sentiment.polarity

        logger.info("Calcul des scores de sentiment")
        df_comments["sentiment"] = (
            df_comments["commentaire"].astype(str).apply(sentiment_score)
        )
        st.subheader("Sentiment des commentaires")
        st.write(df_comments[["commentaire", "sentiment"]].head(10))

        # 5. Word Cloud
        logger.info("Génération du Word Cloud")
        text = " ".join(df_comments["commentaire"].dropna().tolist())
        wc = WordCloud(width=800, height=400, background_color="white").generate(text)
        fig_wc, ax_wc = plt.subplots(figsize=(10, 5))
        ax_wc.imshow(wc, interpolation="bilinear")
        ax_wc.axis("off")
        st.subheader("Word Cloud des commentaires")
        st.pyplot(fig_wc)

except Exception as e:
    logger.exception("Erreur dans l'application Streamlit: %s", e)
    st.error(f"Erreur dans l'application : {e}")
finally:
    try:
        conn.close()
        logger.info("Fermeture de la base SQLite")
    except Exception:
        # Si la connexion n'a jamais été ouverte, on ignore
        pass
