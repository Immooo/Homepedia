import os
import sqlite3

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    regexp_replace,
    substring,
    lpad,
    when,
)
from pyspark.sql.types import DoubleType, StringType

from src.backend.logging_setup import setup_logging

logger = setup_logging()

# 1. Session Spark
logger.info("Initialisation de la session Spark pour 'DVF Spark Analysis'")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[*]")
builder = (
    SparkSession.builder.appName("DVF Spark Analysis")
    .master(SPARK_MASTER_URL)
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.ui.enabled", "false")
)
if os.getenv("SPARK_DRIVER_HOST"):
    builder = builder.config("spark.driver.host", os.environ["SPARK_DRIVER_HOST"])
spark = builder.getOrCreate()
logger.info("Spark master : %s", SPARK_MASTER_URL)

# 2. Chemins
CSV_PATH = os.getenv(
    "DVF_PROCESSED_PATH", os.path.join("data", "processed", "transactions_2024.csv")
)
DB_PATH = os.getenv("DB_PATH", os.path.join("data", "homepedia.db"))
logger.info("CSV : %s | SQLite : %s", CSV_PATH, DB_PATH)

# 3. Lecture CSV
df = spark.read.csv(CSV_PATH, header=True, sep=",", inferSchema=False)

# 4. Nettoyage & typage
logger.info("Nettoyage des colonnes DVF")

df = df.withColumn(
    "valeur_fonciere_num",
    regexp_replace(regexp_replace(col("valeur_fonciere"), " ", ""), ",", ".").cast(
        DoubleType()
    ),
)

df = df.withColumn(
    "surface_reelle_bati_num",
    col("surface_reelle_bati").cast(DoubleType()),
)

df = df.filter(col("surface_reelle_bati_num") > 0)
df = df.filter(col("valeur_fonciere_num") >= 1000)

df = df.withColumn(
    "prix_m2",
    col("valeur_fonciere_num") / col("surface_reelle_bati_num"),
)
df = df.filter((col("prix_m2") > 0) & (col("prix_m2") <= 20000))

# 🔥 Correction critique : code postal propre
df = df.withColumn(
    "code_postal_clean",
    regexp_replace(col("code_postal").cast(StringType()), r"\.0$", ""),
)

df = df.withColumn(
    "code_postal_clean",
    lpad(col("code_postal_clean"), 5, "0"),
)
df = df.filter(col("code_postal_clean").rlike(r"^[0-9]{5}$"))

# Département sur 2 chiffres
df = df.withColumn(
    "dept",
    when(
        substring(col("code_postal_clean"), 1, 2) == "20",
        when(col("code_postal_clean").cast("int") < 20200, "2A").otherwise("2B"),
    ).otherwise(substring(col("code_postal_clean"), 1, 2)),
)
df = df.filter(col("dept") != "00")

# 5. Agrégations
logger.info("Agrégation par département")

agg = (
    df.groupBy("dept")
    .agg(
        count("*").alias("nb_transactions"),
        avg("prix_m2").alias("prix_m2_moyen"),
    )
    .orderBy("dept")
)

# 6. Écriture SQLite
logger.info("Écriture dans SQLite (analyse_departementale)")
pdf = agg.toPandas()

conn = sqlite3.connect(DB_PATH)
pdf.to_sql("analyse_departementale", conn, if_exists="replace", index=False)
conn.close()

logger.info("✅ analyse_departementale générée avec succès")

spark.stop()
logger.info("Session Spark arrêtée")
