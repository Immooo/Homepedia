#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Création d'index SQLite pour optimiser les requêtes.
Index créés :
 - idx_tx_code_postal  sur transactions(code_postal)
 - idx_tx_date         sur transactions(date_mutation)
 - idx_pop_code        sur population(code)
"""

import os
import sqlite3

# 1. Chemin vers la base
DB_PATH = os.path.join('data', 'homepedia.db')
if not os.path.exists(DB_PATH):
    raise FileNotFoundError(f"Base non trouvée : {DB_PATH}")

# 2. Connexion
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# 3. Création des index
print("Création de idx_tx_code_postal...")
cur.execute("CREATE INDEX IF NOT EXISTS idx_tx_code_postal ON transactions(code_postal);")

print("Création de idx_tx_date...")
cur.execute("CREATE INDEX IF NOT EXISTS idx_tx_date ON transactions(date_mutation);")

print("Création de idx_pop_code...")
cur.execute("CREATE INDEX IF NOT EXISTS idx_pop_code ON population(code);")

# 4. Commit & fermeture
conn.commit()
conn.close()
print("Indexes créés avec succès.")
