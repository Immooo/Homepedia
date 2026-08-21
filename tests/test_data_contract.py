import sqlite3


DB_FILE = "data/homepedia.db"

CONTRACT = {
    "transactions": {"date_mutation", "valeur_fonciere", "code_postal", "commune"},
    "population": {"code", "population"},
    "revenus": {"code", "revenu_median"},
    "chomage": {"code", "taux_chomage"},
    "pauvrete": {"code", "taux_pauvrete"},
    "analyse_departementale": {"dept", "nb_transactions", "prix_m2_moyen"},
    "analyse_regionale": {
        "code_region",
        "nb_transactions",
        "prix_m2_moyen",
        "population",
        "revenu_median",
        "taux_chomage",
        "taux_pauvrete",
    },
}


def test_canonical_sqlite_contract():
    with sqlite3.connect(DB_FILE) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert CONTRACT.keys() <= tables
        for table, expected_columns in CONTRACT.items():
            actual = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
            assert expected_columns <= actual, f"Colonnes manquantes dans {table}"


def test_legacy_english_tables_are_absent():
    with sqlite3.connect(DB_FILE) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    assert not tables.intersection(
        {"income", "unemployment", "poverty", "spark_dept_analysis", "region_analysis"}
    )


def test_transactions_are_unique_and_market_relevant():
    with sqlite3.connect(DB_FILE) as conn:
        total = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        unique = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT 1 FROM transactions
                GROUP BY date_mutation, nature_mutation, valeur_fonciere, code_postal,
                         commune, type_local, surface_reelle_bati,
                         nombre_pieces_principales
            )
            """
        ).fetchone()[0]
        minimum_value = conn.execute(
            "SELECT MIN(valeur_fonciere) FROM transactions"
        ).fetchone()[0]
    assert total == unique
    assert minimum_value >= 1_000


def test_geographic_and_price_aggregates_are_coherent():
    with sqlite3.connect(DB_FILE) as conn:
        invalid_postcodes = conn.execute(
            """
            SELECT COUNT(*) FROM transactions
            WHERE length(code_postal) != 5
               OR code_postal GLOB '*[^0-9]*'
            """
        ).fetchone()[0]
        invalid_departments = conn.execute(
            """
            SELECT COUNT(*) FROM analyse_departementale
            WHERE dept = '00' OR length(dept) NOT IN (2, 3)
               OR prix_m2_moyen <= 0 OR prix_m2_moyen > 20000
            """
        ).fetchone()[0]
        corsica = conn.execute(
            "SELECT COUNT(*) FROM analyse_regionale WHERE code_region = '94'"
        ).fetchone()[0]
    assert invalid_postcodes == 0
    assert invalid_departments == 0
    assert corsica == 1
