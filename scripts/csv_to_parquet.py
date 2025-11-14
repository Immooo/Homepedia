import glob
import os

import pandas as pd

PROCESSED = os.path.join("data", "processed")

for csv in glob.glob(os.path.join(PROCESSED, "*.csv")):
    df = pd.read_csv(csv, dtype=str)
    out = csv.replace(".csv", ".parquet")
    df.to_parquet(out, compression="snappy")
    print("→", out)
print("✅ Conversion terminée.")
