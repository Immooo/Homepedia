param(
  [Parameter(Mandatory=$true)]
  [ValidateSet('build','rebuild','up','down','logs','ps','health','sh-app','init-db')]
  [string]$cmd
)
$compose = "docker compose -f infra/docker-compose.yml --env-file .env"

switch ($cmd) {
  'build'   { iex "$compose build" }
  'rebuild' { iex "$compose build --no-cache" }
  'up'      { iex "$compose up -d" }
  'down'    { iex "$compose down" }
  'logs'    { iex "$compose logs -f --tail=200" }
  'ps'      { iex "$compose ps" }
  'health'  { Invoke-WebRequest "http://localhost:$env:STREAMLIT_SERVER_PORT/_stcore/health" | Select-Object -Expand Content }
  'sh-app'  { iex "$compose exec app bash" }
  'init-db' {
    $py = "import os,sqlite3; db=os.getenv('DB_PATH','data/homepedia.db'); os.makedirs(os.path.dirname(db), exist_ok=True); con=sqlite3.connect(db); con.execute('CREATE TABLE IF NOT EXISTS _bootstrap (id INTEGER PRIMARY KEY, created_at TEXT)'); con.commit(); con.close(); print('DB ready at',db)"
    iex "$compose exec app python -c `"${py}`""
  }
}
