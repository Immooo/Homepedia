import os
import pandas as pd
import sqlite3

def extract_dept(code_insee: str) -> str:
    """
    Extrait le code départemental (2, 3 ou 4 caractères) à partir
    du code INSEE de la commune.
    """
    code5 = str(code_insee).zfill(5)
    if code5.startswith(("97", "98")):
        return code5[:3]
    elif code5[:2] == "20":
        return "2A" if int(code5[2]) < 2 else "2B"
    return code5[:2]

def main() -> None:
    RAW     = os.path.join("data", "raw", "insee", "DS_FILOSOFI_CC_2021_data.csv")
    OUT_CSV = os.path.join("data", "processed", "income_dept.csv")
    DB_PATH = os.path.join("data", "homepedia.db")
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)

    df = pd.read_csv(RAW, dtype=str, sep=";")

    df_filt = df[
        (df["FILOSOFI_MEASURE"] == "MED_SL") &
        (df["UNIT_MEASURE"]    == "EUR_YR") &
        (df["CONF_STATUS"]     == "F") &
        (df["TIME_PERIOD"]     == "2021") &
        df["OBS_VALUE"].notna() &
        (df["OBS_VALUE"] != "")
    ].copy()

    df_filt["dept"] = df_filt["GEO"].apply(extract_dept)
    df_filt["income_median"] = (
        df_filt["OBS_VALUE"]
        .str.replace(",", ".")
        .astype(float)
    )

    df_dept = (
        df_filt
        .groupby("dept", as_index=False)["income_median"]
        .median()
        .rename({"dept": "code"}, axis=1)
    )

    df_dept.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(f"CSV revenu médian départemental écrit → {OUT_CSV}")

    conn = sqlite3.connect(DB_PATH)
    df_dept.to_sql("income", conn, if_exists="replace", index=False)
    conn.close()
    print("✅ Table 'income' créée dans SQLite")

if __name__ == "__main__":
    main()
