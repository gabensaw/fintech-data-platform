param(
    [switch]$SkipKafkaReset
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$SparkDataPath = Join-Path $ProjectRoot "spark\data"

Write-Host "Resetting local pipeline data without deleting Metabase metadata..."
Write-Host ""

Write-Host "1. Stopping producer..."
docker compose stop producer

Write-Host ""
Write-Host "2. Removing Spark data lake files and checkpoints..."
if (Test-Path $SparkDataPath) {
    Remove-Item -Recurse -Force $SparkDataPath
}
New-Item -ItemType Directory -Force $SparkDataPath | Out-Null

Write-Host ""
Write-Host "3. Dropping warehouse and dbt tables from PostgreSQL database 'fintech'..."
$DropSql = @"
drop table if exists daily_transaction_metrics cascade;
drop table if exists merchant_metrics cascade;
drop table if exists mart_daily_kpis cascade;
drop table if exists mart_merchant_performance cascade;
drop table if exists mart_merchant_tier cascade;
drop table if exists mart_platform_summary cascade;
drop table if exists mart_top_merchants cascade;
"@

docker exec postgres psql -U fintech -d fintech -c $DropSql

if (-not $SkipKafkaReset) {
    Write-Host ""
    Write-Host "4. Recreating Kafka topic 'transactions'..."
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic transactions --if-exists
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic transactions --partitions 1 --replication-factor 1 --if-not-exists
} else {
    Write-Host ""
    Write-Host "4. Skipping Kafka topic reset because -SkipKafkaReset was provided."
}

Write-Host ""
Write-Host "Reset complete."
Write-Host ""
Write-Host "Next steps:"
Write-Host "1. Start producer: docker compose start producer"
Write-Host "2. Let it run for 1-2 minutes."
Write-Host "3. Stop producer: docker compose stop producer"
Write-Host "4. Trigger Airflow DAG: daily_fintech_pipeline"
