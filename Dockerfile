# syntax=docker/dockerfile:1.4
############################################################
# Étape 1 : builder avec OS-libs
############################################################
FROM python:3.11-slim AS builder

# 1. Installer les dépendances système pour GIS + build tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    gdal-bin \
    libgdal-dev \
    gcc \
    wget && \
    rm -rf /var/lib/apt/lists/*

# 2. Préparer le cache pip
ENV PIP_NO_CACHE_DIR=off \
    PIP_DEFAULT_TIMEOUT=100

WORKDIR /app

# 3. Copier et installer vos requirements
COPY requirements.txt .

# 4. BuildKit cache de pip
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip && \
    pip install -r requirements.txt

############################################################
# Étape 2 : runtime léger
############################################################
FROM python:3.11-slim AS runtime

# Copier seulement les paquets déjà compilés
COPY --from=builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/

WORKDIR /app
COPY . .

EXPOSE 8501
ENTRYPOINT ["streamlit","run","src/app/streamlit_app.py","--server.address=0.0.0.0"]
