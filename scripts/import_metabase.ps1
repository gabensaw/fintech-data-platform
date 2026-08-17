param(
    [string]$BackupPath = "metabase\backup\metabase_metadata.dump",
    [switch]$Force
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$HostBackupPath = Join-Path $ProjectRoot $BackupPath
$ContainerBackupPath = "/tmp/metabase_metadata.dump"

function Write-Step {
    param(
        [int]$Number,
        [string]$Message
    )

    Write-Host ""
    Write-Host "[$Number/6] $Message"
}

if (-not (Test-Path $HostBackupPath)) {
    throw "Backup file does not exist: $HostBackupPath"
}

if (-not $Force) {
    throw "Restore cancelled. This command replaces the local Metabase metadata database. Re-run with -Force if you want to continue."
}

Write-Host "Metabase metadata restore"
Write-Host "Target database : metabase"
Write-Host "Target container: postgres"
Write-Host "Backup file     : $HostBackupPath"
Write-Host "Temporary file  : postgres:$ContainerBackupPath"
Write-Host ""
Write-Host "WARNING: This restore replaces dashboards, questions, collections, users, and Metabase settings with the content of the backup."

Write-Step 1 "Stopping Metabase to release metadata database connections"
docker compose stop metabase
if ($LASTEXITCODE -ne 0) {
    throw "Stopping Metabase failed."
}

Write-Step 2 "Copying local backup into PostgreSQL container"
Write-Host "From: $HostBackupPath"
Write-Host "To  : postgres:$ContainerBackupPath"
docker cp $HostBackupPath "postgres:$ContainerBackupPath"
if ($LASTEXITCODE -ne 0) {
    throw "Copying Metabase backup into PostgreSQL container failed."
}

Write-Step 3 "Recreating target database 'metabase'"
Write-Host "Terminating active connections to database 'metabase'..."
docker exec postgres psql -U fintech -d postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'metabase';"
if ($LASTEXITCODE -ne 0) {
    throw "Terminating Metabase database connections failed."
}

Write-Host "Dropping database 'metabase' if it exists..."
docker exec postgres dropdb -U fintech --if-exists metabase
if ($LASTEXITCODE -ne 0) {
    throw "Dropping Metabase database failed."
}

Write-Host "Creating empty database 'metabase'..."
docker exec postgres createdb -U fintech metabase
if ($LASTEXITCODE -ne 0) {
    throw "Creating Metabase database failed."
}

Write-Step 4 "Restoring backup into database 'metabase'"
Write-Host "Command: pg_restore -U fintech -d metabase --no-owner --no-privileges"
docker exec postgres pg_restore -U fintech -d metabase --no-owner --no-privileges $ContainerBackupPath
if ($LASTEXITCODE -ne 0) {
    throw "Restoring Metabase database backup failed."
}

Write-Step 5 "Removing temporary backup file from PostgreSQL container"
Write-Host "File: postgres:$ContainerBackupPath"
docker exec postgres rm -f $ContainerBackupPath
if ($LASTEXITCODE -ne 0) {
    throw "Removing temporary Metabase backup file failed."
}

Write-Step 6 "Starting Metabase"
docker compose start metabase
if ($LASTEXITCODE -ne 0) {
    throw "Starting Metabase failed."
}

Write-Host ""
Write-Host "Restore completed successfully."
Write-Host "Open Metabase: http://localhost:3000"
