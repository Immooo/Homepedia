import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, substring, avg, count
)
from pyspark.sql.types import DoubleType
import sqlite3

# 1. Créer la session Spark
spark = SparkSession.builder \
    .appName("DVF Spark Analysis") \
    .getOrCreate()

# 2. Chemins
CSV_PATH = os.path.join("data", "processed", "transactions_2024.csv")
DB_PATH  = os.path.join("data", "homepedia.db")

# 3. Lire le CSV dans un DataFrame Spark
df = spark.read.csv(CSV_PATH, header=True, sep=",", inferSchema=False)

# 4. Nettoyer et caster
df = df.withColumn(
    "valeur_fonciere_num",
    regexp_replace(regexp_replace(col("valeur_fonciere"), " ", ""), ",", ".").cast(DoubleType())
)
df = df.withColumn("surf_bati_num", col("surface_reelle_bati").cast(DoubleType()))
df = df.filter(col("surf_bati_num") > 0)
df = df.withColumn("prix_m2", col("valeur_fonciere_num") / col("surf_bati_num"))
df = df.withColumn("dept", substring(col("code_postal"), 1, 2))

# 5. Agrégations
agg = df.groupBy("dept") \
        .agg(
            count("*").alias("nb_transactions"),
            avg("prix_m2").alias("prix_m2_moyen")
        ) \
        .orderBy("dept")

# 6. Collect & persister dans SQLite
pdf = agg.toPandas()

conn = sqlite3.connect(DB_PATH)
# On retire le paramètre dtype pour éviter l'erreur
pdf.to_sql("spark_dept_analysis", conn, if_exists="replace", index=False)
conn.close()

print("✅ Spark analysis terminée et résultats écrits dans la table 'spark_dept_analysis' de SQLite.")

spark.stop()