param(
    [string]$BackupPath = "metabase\backup\metabase_metadata.dump",
    [switch]$Force
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$HostBackupPath = Join-Path $ProjectRoot $BackupPath
$ContainerBackupPath = "/tmp/metabase_metadata.dump"

if (-not (Test-Path $HostBackupPath)) {
    throw "Backup file does not exist: $HostBackupPath"
}

if (-not $Force) {
    throw "This restore replaces the local Metabase metadata database. Re-run with -Force if you want to continue."
}

Write-Host "Restoring Metabase metadata database..."
Write-Host ""

Write-Host "1. Stopping Metabase so the metadata database can be restored safely..."
docker compose stop metabase
if ($LASTEXITCODE -ne 0) {
    throw "Stopping Metabase failed."
}

Write-Host ""
Write-Host "2. Copying local backup into the PostgreSQL container..."
docker cp $HostBackupPath "postgres:$ContainerBackupPath"
if ($LASTEXITCODE -ne 0) {
    throw "Copying Metabase backup into PostgreSQL container failed."
}

Write-Host ""
Write-Host "3. Recreating database 'metabase'..."
docker exec postgres psql -U fintech -d postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'metabase';"
if ($LASTEXITCODE -ne 0) {
    throw "Terminating Metabase database connections failed."
}

docker exec postgres dropdb -U fintech --if-exists metabase
if ($LASTEXITCODE -ne 0) {
    throw "Dropping Metabase database failed."
}

docker exec postgres createdb -U fintech metabase
if ($LASTEXITCODE -ne 0) {
    throw "Creating Metabase database failed."
}

Write-Host ""
Write-Host "4. Restoring backup into database 'metabase'..."
docker exec postgres pg_restore -U fintech -d metabase --no-owner --no-privileges $ContainerBackupPath
if ($LASTEXITCODE -ne 0) {
    throw "Restoring Metabase database backup failed."
}

Write-Host ""
Write-Host "5. Removing temporary container backup file..."
docker exec postgres rm -f $ContainerBackupPath
if ($LASTEXITCODE -ne 0) {
    throw "Removing temporary Metabase backup file failed."
}

Write-Host ""
Write-Host "6. Starting Metabase..."
docker compose start metabase
if ($LASTEXITCODE -ne 0) {
    throw "Starting Metabase failed."
}

Write-Host ""
Write-Host "Metabase metadata restore complete."
