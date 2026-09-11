"""Migration idempotente de qualité pour la table DVF SQLite."""

import os
import sqlite3


DB_PATH = os.getenv("DB_PATH", "data/homepedia.db")


def main() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        before = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        conn.executescript(
            """
            DROP TABLE IF EXISTS transactions_clean;
            CREATE TABLE transactions_clean (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date_mutation DATE NOT NULL,
                nature_mutation TEXT,
                valeur_fonciere REAL NOT NULL,
                code_postal TEXT NOT NULL,
                commune TEXT NOT NULL,
                type_local TEXT,
                surface_reelle_bati REAL,
                nombre_pieces_principales INTEGER
            );

            INSERT INTO transactions_clean (
                date_mutation, nature_mutation, valeur_fonciere, code_postal,
                commune, type_local, surface_reelle_bati, nombre_pieces_principales
            )
            SELECT
                date_mutation, nature_mutation, valeur_fonciere, code_postal,
                commune, type_local, surface_reelle_bati, nombre_pieces_principales
            FROM transactions
            WHERE valeur_fonciere >= 1000
            GROUP BY
                date_mutation, nature_mutation, valeur_fonciere, code_postal,
                commune, type_local, surface_reelle_bati, nombre_pieces_principales;

            DROP TABLE transactions;
            ALTER TABLE transactions_clean RENAME TO transactions;
            """
        )
        after = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        conn.execute(
            """
            UPDATE transactions
            SET code_postal = printf('%05d', CAST(REPLACE(code_postal, '.0', '') AS INTEGER))
            WHERE code_postal GLOB '[0-9]*'
            """
        )
        print(f"Transactions avant={before}, après={after}, supprimées={before-after}")


if __name__ == "__main__":
    main()
