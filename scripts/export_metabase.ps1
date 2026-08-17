param(
    [string]$BackupPath = "metabase\backup\metabase_metadata.dump"
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$HostBackupPath = Join-Path $ProjectRoot $BackupPath
$HostBackupDirectory = Split-Path $HostBackupPath -Parent
$ContainerBackupPath = "/tmp/metabase_metadata.dump"

Write-Host "Backing up Metabase metadata database..."
Write-Host ""

Write-Host "1. Preparing local backup directory..."
if (-not (Test-Path $HostBackupDirectory)) {
    New-Item -ItemType Directory -Force $HostBackupDirectory | Out-Null
}

Write-Host ""
Write-Host "2. Creating PostgreSQL dump of database 'metabase'..."
docker exec postgres pg_dump -U fintech -d metabase -Fc -f $ContainerBackupPath
if ($LASTEXITCODE -ne 0) {
    throw "Metabase database backup failed."
}

Write-Host ""
Write-Host "3. Copying backup to local project directory..."
docker cp "postgres:$ContainerBackupPath" $HostBackupPath
if ($LASTEXITCODE -ne 0) {
    throw "Copying Metabase backup failed."
}

Write-Host ""
Write-Host "4. Removing temporary container backup file..."
docker exec postgres rm -f $ContainerBackupPath
if ($LASTEXITCODE -ne 0) {
    throw "Removing temporary Metabase backup file failed."
}

Write-Host ""
Write-Host "Metabase metadata backup complete:"
Write-Host $HostBackupPath
