import pandas as pd
from pymongo import MongoClient

client = MongoClient("mongodb://homepedia-mongo:27017/")
db = client["homepedia"]

# Import Mongo unemployment
df = pd.DataFrame(list(db.unemployment.find({}, {"_id": 0})))

# Vérifie que les colonnes existent pour grouper
print(df.head())

# Agrégation par département
df_dept = (
    df.groupby("code_dept")
    .agg(
        {
            "taux_chomage": "mean",
        }
    )
    .reset_index()
)

# Export Parquet
df_dept.to_parquet("data/processed/unemployment_dept.parquet", index=False)

print("OK → unemployment_dept.parquet généré")
