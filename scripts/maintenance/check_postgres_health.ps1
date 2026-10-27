# PostgreSQL health check for the kbo_postgres container.
# Logs failures to D:\server-data\KBO_playwright\logs\postgres_health.log
# Exit code: 0 = healthy, 1 = unhealthy

$ErrorActionPreference = "Stop"
$logDir = "D:\server-data\KBO_playwright\logs"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }
$logFile = Join-Path $logDir "postgres_health.log"
$timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")

$container = "kbo_postgres"
$containerState = (docker inspect $container --format '{{.State.Status}}' 2>&1)
$health = "unknown"
try {
    docker exec $container pg_isready -U postgres -d bega_prod 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) { $health = "ready" } else { $health = "not-ready" }
} catch {
    $health = "error"
}

$line = "$timestamp state=$containerState health=$health"
if ($health -ne "ready" -or $containerState -ne "running") {
    Add-Content -Path $logFile -Value "$line UNHEALTHY"
    exit 1
} else {
    # Keep log small: only record one healthy line per day.
    $lastHealthy = Get-Content $logFile -ErrorAction SilentlyContinue | Select-String "HEALTHY_OK" | Select-Object -Last 1
    if (-not $lastHealthy -or $lastHealthy.Line -notlike "*$(Get-Date -Format yyyy-MM-dd)*") {
        Add-Content -Path $logFile -Value "$line HEALTHY_OK"
    }
    exit 0
}