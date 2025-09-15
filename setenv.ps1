# setenv.ps1
$env:GOOGLE_CLOUD_PROJECT = "golang-poc-469612"
$env:BQ_LOCATION          = "asia-south2"   # change if your dataset is EU/asia-...

# read SQL from file (no multi-line paste hassles)
$env:DASH_QUERY = Get-Content -Raw ".\query.sql"

# use your key FILE PATH (safe for local dev)
$env:GOOGLE_APPLICATION_CREDENTIALS = "D:\Go\golang-poc-469612-0200c92dbeec.json"

# make sure the JSON-content env isn't set at same time
Remove-Item Env:\GOOGLE_APPLICATION_CREDENTIALS_JSON -ErrorAction SilentlyContinue

# (optional) quick echo to confirm
"PROJECT=$($env:GOOGLE_CLOUD_PROJECT)  LOCATION=$($env:BQ_LOCATION)"
"QUERY len=$($env:DASH_QUERY.Length)"
"CREDS file exists: $(Test-Path $env:GOOGLE_APPLICATION_CREDENTIALS)"
