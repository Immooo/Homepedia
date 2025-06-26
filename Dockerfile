# Étape 1 : construire l’environnement Python
FROM python:3.11-slim

# 1. Variables d’environnement pour un conteneur non interactif
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

# 2. Répertoire de travail
WORKDIR /app

# 3. Copier uniquement les fichiers nécessaires pour installer les dépendances
COPY requirements.txt .

# 4. Installer les dépendances
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# 5. Copier le reste de l’application
COPY . .

# 6. Exposer le port Streamlit par défaut
EXPOSE 8501

# 7. Commande de démarrage
ENTRYPOINT ["streamlit", "run", "src/app/streamlit_app.py", "--server.address=0.0.0.0"]
