Write-Host "Creating Kafka topics..."

docker compose exec broker `
    /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --create `
    --if-not-exists `
    --topic social-events `
    --partitions 3 `
    --replication-factor 1

Write-Host ""
Write-Host "Kafka topics:"
docker compose exec broker `
    /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --list

Write-Host ""
Write-Host "Kafka initialization completed."