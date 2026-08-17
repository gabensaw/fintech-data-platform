param(
    [string]$BackupPath = "metabase\backup\metabase_metadata.dump"
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$HostBackupPath = Join-Path $ProjectRoot $BackupPath
$HostBackupDirectory = Split-Path $HostBackupPath -Parent
$ContainerBackupPath = "/tmp/metabase_metadata.dump"

function Write-Step {
    param(
        [int]$Number,
        [string]$Message
    )

    Write-Host ""
    Write-Host "[$Number/4] $Message"
}

Write-Host "Metabase metadata backup"
Write-Host "Source database : metabase"
Write-Host "Source container: postgres"
Write-Host "Backup file     : $HostBackupPath"
Write-Host "Temporary file  : postgres:$ContainerBackupPath"

Write-Step 1 "Preparing local backup directory"
Write-Host "Directory: $HostBackupDirectory"
if (-not (Test-Path $HostBackupDirectory)) {
    New-Item -ItemType Directory -Force $HostBackupDirectory | Out-Null
}

Write-Step 2 "Creating PostgreSQL custom-format dump"
Write-Host "Command: pg_dump -U fintech -d metabase -Fc"
docker exec postgres pg_dump -U fintech -d metabase -Fc -f $ContainerBackupPath
if ($LASTEXITCODE -ne 0) {
    throw "Metabase database backup failed."
}

Write-Step 3 "Copying backup from PostgreSQL container to project directory"
Write-Host "From: postgres:$ContainerBackupPath"
Write-Host "To  : $HostBackupPath"
docker cp "postgres:$ContainerBackupPath" $HostBackupPath
if ($LASTEXITCODE -ne 0) {
    throw "Copying Metabase backup failed."
}

Write-Step 4 "Removing temporary backup file from PostgreSQL container"
Write-Host "File: postgres:$ContainerBackupPath"
docker exec postgres rm -f $ContainerBackupPath
if ($LASTEXITCODE -ne 0) {
    throw "Removing temporary Metabase backup file failed."
}

Write-Host ""
Write-Host "Backup completed successfully."
Write-Host "Saved file: $HostBackupPath"
