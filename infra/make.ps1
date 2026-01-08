param(
  [Parameter(Mandatory = $true)]
  [ValidateSet(
    'build','rebuild','up','down','logs','ps','sh-app',
    'etl-ls','etl-valeurs','etl-insee','etl-spark','etl-agg','etl-export','etl-schema','etl-rename-fr','etl-all'
  )]
  [string]$cmd
)

function Invoke-Compose {
  [CmdletBinding()]
  param(
    # IMPORTANT: les args doivent être le 1er param pour éviter que "exec" se lie au mauvais paramètre
    [Parameter(Position = 0, ValueFromRemainingArguments = $true)]
    [string[]]$Args,

    [Parameter(ValueFromPipeline = $true)]
    $InputObject
  )

  process {
    if ($PSBoundParameters.ContainsKey('InputObject')) {
      $InputObject | & docker compose -f infra/docker-compose.yml --env-file .env @Args
    } else {
      & docker compose -f infra/docker-compose.yml --env-file .env @Args
    }
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  }
}

switch ($cmd) {
  'build'   { Invoke-Compose build }
  'rebuild' { Invoke-Compose build '--no-cache' }
  'up'      { Invoke-Compose up '-d' }
  'down'    { Invoke-Compose down }
  'logs'    { Invoke-Compose logs '-f' '--tail=200' }
  'ps'      { Invoke-Compose ps }
  'sh-app'  { Invoke-Compose exec app bash }

  'etl-ls' {
    Write-Host "ETL disponibles : etl-insee, etl-valeurs, etl-spark, etl-agg, etl-export, etl-schema, etl-rename-fr, etl-all"
  }

  'etl-valeurs' { Invoke-Compose exec app bash '-lc' 'set -e; PYTHONPATH=/app python -m src.backend.ingest_valeursfoncieres' }

  'etl-insee' {
    Invoke-Compose exec app bash '-lc' 'set -e; PYTHONPATH=/app python -m src.backend.ingest_insee_population && PYTHONPATH=/app python -m src.backend.ingest_insee_income && PYTHONPATH=/app python -m src.backend.ingest_insee_unemployment && PYTHONPATH=/app python -m src.backend.ingest_insee_poverty && PYTHONPATH=/app python -m src.backend.ingest_insee_region'
  }

  'etl-spark' { Invoke-Compose exec app bash '-lc' 'set -e; PYTHONPATH=/app python -m src.backend.spark_dvf_analysis' }
  'etl-agg'   { Invoke-Compose exec app bash '-lc' 'set -e; PYTHONPATH=/app python -m src.backend.aggregate_by_region' }
  'etl-export'{ Invoke-Compose exec app bash '-lc' 'set -e; PYTHONPATH=/app python src/etl/sqlite_to_mongo_all_tables.py' }

  'etl-schema' {
@'
import os, sqlite3
db = os.environ.get("DB_PATH", "data/homepedia.db")
con = sqlite3.connect(db)
tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;").fetchall()]
print("DB:", db)
print("Tables:", ", ".join(tables) if tables else "(aucune)")

def show_cols(tbl):
    try:
        cols = con.execute(f"PRAGMA table_info({tbl});").fetchall()
        print("\n#", tbl)
        for c in cols:
            print(" -", c[1], c[2])
    except Exception as e:
        print("\n#", tbl, "(err)", e)

for t in ("revenus","income","pauvrete","poverty","analyse_regionale","region_analysis"):
    show_cols(t)

con.close()
'@ | Invoke-Compose exec '-T' app python '-'
  }

  'etl-rename-fr' {
@'
import os, sqlite3
db = os.environ.get("DB_PATH", "data/homepedia.db")
con = sqlite3.connect(db)
cur = con.cursor()

def try_rename(tbl, old, new):
    try:
        cur.execute(f"ALTER TABLE {tbl} RENAME COLUMN {old} TO {new};")
        print(f"OK {tbl}: {old} -> {new}")
    except Exception as e:
        print(f"SKIP {tbl}: {old} -> {new} ({e})")

try_rename("revenus", "income_median", "revenu_median")
try_rename("pauvrete", "poverty_rate", "taux_pauvrete")

con.commit()
con.close()
print("Done.")
'@ | Invoke-Compose exec '-T' app python '-'
  }

  'etl-all' {
    & powershell -ExecutionPolicy Bypass -File infra\make.ps1 etl-insee
    & powershell -ExecutionPolicy Bypass -File infra\make.ps1 etl-valeurs
    & powershell -ExecutionPolicy Bypass -File infra\make.ps1 etl-spark
    & powershell -ExecutionPolicy Bypass -File infra\make.ps1 etl-agg
    & powershell -ExecutionPolicy Bypass -File infra\make.ps1 etl-export
  }
}
