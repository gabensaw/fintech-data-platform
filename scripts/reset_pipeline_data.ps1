param(
    [switch]$SkipKafkaReset
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$SparkDataPath = Join-Path $ProjectRoot "spark\data"

function Write-Step {
    param(
        [int]$Number,
        [string]$Message
    )

    Write-Host ""
    Write-Host "[$Number/4] $Message"
}

function Invoke-NativeCommand {
    param(
        [string]$Command,
        [string[]]$Arguments,
        [string]$ErrorMessage
    )

    $PreviousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"

    try {
        & $Command @Arguments
        $ExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $PreviousErrorActionPreference
    }

    if ($ExitCode -ne 0) {
        throw "$ErrorMessage Exit code: $ExitCode"
    }
}

Write-Host "Local pipeline data reset"
Write-Host "Project root       : $ProjectRoot"
Write-Host "Spark data path    : $SparkDataPath"
Write-Host "Warehouse database : fintech"
Write-Host "Kafka topic        : transactions"
Write-Host "Kafka reset        : $(-not $SkipKafkaReset)"
Write-Host "Metabase metadata  : preserved"
Write-Host ""
Write-Host "This removes generated pipeline data, but it does not delete Metabase dashboards."

Write-Step 1 "Stopping producer to prevent new Kafka messages during reset"
Invoke-NativeCommand `
    -Command "docker" `
    -Arguments @("compose", "stop", "producer") `
    -ErrorMessage "Stopping producer failed."

Write-Step 2 "Removing Spark data lake files and checkpoints"
Write-Host "Path: $SparkDataPath"
if (Test-Path $SparkDataPath) {
    Remove-Item -Recurse -Force $SparkDataPath
}
New-Item -ItemType Directory -Force $SparkDataPath | Out-Null

Write-Step 3 "Dropping warehouse and dbt tables from PostgreSQL database 'fintech'"
$DropSql = @"
drop table if exists daily_transaction_metrics cascade;
drop table if exists merchant_metrics cascade;
drop table if exists mart_daily_kpis cascade;
drop table if exists mart_merchant_performance cascade;
drop table if exists mart_merchant_tier cascade;
drop table if exists mart_platform_summary cascade;
drop table if exists mart_top_merchants cascade;
"@

Invoke-NativeCommand `
    -Command "docker" `
    -Arguments @("exec", "postgres", "psql", "-U", "fintech", "-d", "fintech", "-c", $DropSql) `
    -ErrorMessage "Dropping warehouse and dbt tables failed."

if (-not $SkipKafkaReset) {
    Write-Step 4 "Recreating Kafka topic 'transactions'"
    Write-Host "Deleting topic if it exists..."
    Invoke-NativeCommand `
        -Command "docker" `
        -Arguments @("exec", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092", "--delete", "--topic", "transactions", "--if-exists") `
        -ErrorMessage "Deleting Kafka topic 'transactions' failed."

    Write-Host "Creating empty topic..."
    Invoke-NativeCommand `
        -Command "docker" `
        -Arguments @("exec", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092", "--create", "--topic", "transactions", "--partitions", "1", "--replication-factor", "1", "--if-not-exists") `
        -ErrorMessage "Creating Kafka topic 'transactions' failed."
} else {
    Write-Step 4 "Skipping Kafka topic reset because -SkipKafkaReset was provided"
    Write-Host "Existing Kafka messages and offsets are preserved."
}

Write-Host ""
Write-Host "Reset completed successfully."
Write-Host ""
Write-Host "Next steps:"
Write-Host "1. Start producer: docker compose start producer"
Write-Host "2. Let it run for 1-2 minutes."
Write-Host "3. Stop producer: docker compose stop producer"
Write-Host "4. Trigger Airflow DAG: daily_fintech_pipeline"
