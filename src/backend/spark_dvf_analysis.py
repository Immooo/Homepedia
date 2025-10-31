import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, substring, avg, count
from pyspark.sql.types import DoubleType
import sqlite3

from backend.logging_setup import setup_logging
logger = setup_logging()

# 1. Créer la session Spark
logger.info("Initialisation de la session Spark pour 'DVF Spark Analysis'")
spark = SparkSession.builder.appName("DVF Spark Analysis").getOrCreate()

# 2. Chemins
CSV_PATH = os.path.join("data", "processed", "transactions_2024.csv")
DB_PATH = os.path.join("data", "homepedia.db")
logger.info("Chemins utilisés - CSV: %s | DB: %s", CSV_PATH, DB_PATH)

# 3. Lire le CSV dans un DataFrame Spark
logger.info("Lecture du CSV dans Spark DataFrame")
df = spark.read.csv(CSV_PATH, header=True, sep=",", inferSchema=False)

# 4. Nettoyer et caster
logger.info("Nettoyage et typage des colonnes (valeur_fonciere, surface_reelle_bati, prix_m2, dept)")
df = df.withColumn(
    "valeur_fonciere_num",
    regexp_replace(regexp_replace(col("valeur_fonciere"), " ", ""), ",", ".").cast(DoubleType()),
)
df = df.withColumn("surf_bati_num", col("surface_reelle_bati").cast(DoubleType()))
df = df.filter(col("surf_bati_num") > 0)
df = df.withColumn("prix_m2", col("valeur_fonciere_num") / col("surf_bati_num"))
df = df.withColumn("dept", substring(col("code_postal"), 1, 2))

# 5. Agrégations
logger.info("Calcul des agrégats par département (nb_transactions, prix_m2_moyen)")
agg = (
    df.groupBy("dept")
    .agg(count("*").alias("nb_transactions"), avg("prix_m2").alias("prix_m2_moyen"))
    .orderBy("dept")
)

# 6. Collect & persister dans SQLite
logger.info("Conversion vers pandas et écriture dans SQLite (spark_dept_analysis)")
pdf = agg.toPandas()

conn = sqlite3.connect(DB_PATH)
# On retire le paramètre dtype pour éviter l'erreur
pdf.to_sql("spark_dept_analysis", conn, if_exists="replace", index=False)
conn.close()

logger.info("✅ Spark analysis terminée et résultats écrits dans la table 'spark_dept_analysis' de SQLite.")

spark.stop()
logger.info("Session Spark arrêtée proprement")
