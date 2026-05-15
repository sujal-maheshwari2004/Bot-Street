# Run from the root of the bot-street repo
# ── DELETIONS ──────────────────────────────────────────────────────────────────

Remove-Item -Force -ErrorAction SilentlyContinue `
    "main.py", `
    "docker-compose.yml"

Remove-Item -Recurse -Force -ErrorAction SilentlyContinue `
    "display", `
    "user", `
    "scripts"

Write-Host "Deleted monolith files and dead directories." -ForegroundColor Yellow

# ── NEW DIRECTORIES ────────────────────────────────────────────────────────────

$dirs = @(
    "services",
    "db",
    "k8s\kafka",
    "k8s\deployments",
    "k8s\services",
    "k8s\ingress"
)

foreach ($d in $dirs) {
    New-Item -ItemType Directory -Force -Path $d | Out-Null
}

Write-Host "Created new directories." -ForegroundColor Yellow

# ── NEW EMPTY FILES ────────────────────────────────────────────────────────────

$files = @(
    # services — thin pod entrypoints
    "services\entrypoint.py",
    "services\run_engine.py",
    "services\run_ledger.py",
    "services\run_price_feed.py",
    "services\run_sentiment.py",
    "services\run_candles.py",
    "services\run_circuit.py",
    "services\run_bots.py",
    "services\run_api.py",

    # db — MongoDB layer
    "db\__init__.py",
    "db\client.py",
    "db\trade_store.py",
    "db\candle_store.py",

    # k8s — manifests
    "k8s\namespace.yaml",
    "k8s\configmap.yaml",
    "k8s\secret.yaml",
    "k8s\kafka\values.yaml",
    "k8s\deployments\engine.yaml",
    "k8s\deployments\ledger.yaml",
    "k8s\deployments\price-feed.yaml",
    "k8s\deployments\sentiment.yaml",
    "k8s\deployments\candles.yaml",
    "k8s\deployments\circuit.yaml",
    "k8s\deployments\bots.yaml",
    "k8s\deployments\api.yaml",
    "k8s\services\api-service.yaml",
    "k8s\ingress\ingress.yaml"
)

foreach ($f in $files) {
    New-Item -ItemType File -Force -Path $f | Out-Null
}

Write-Host "Scaffolded empty files." -ForegroundColor Yellow

# ── SUMMARY ────────────────────────────────────────────────────────────────────

Write-Host ""
Write-Host "Done. Repo structure ready:" -ForegroundColor Green
Write-Host "  Deleted  : main.py, docker-compose.yml, display/, user/, scripts/"
Write-Host "  Created  : services/ (9 files), db/ (4 files), k8s/ (14 files)"
Write-Host ""
Write-Host "Files to edit next (already exist, need changes):" -ForegroundColor Cyan
Write-Host "  config.py          <- add MONGO_URI, MONGO_DB"
Write-Host "  engine\trade_logger.py  <- swap file write for MongoDB"
Write-Host "  api\main.py        <- strip lifespan services, mount MCP"
Write-Host "  api\mcp_server.py  <- convert to FastAPI sub-app"
Write-Host "  Dockerfile         <- switch CMD to services/entrypoint.py"
Write-Host "  pyproject.toml     <- add pymongo"