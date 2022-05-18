# Iguana

The service acts as a proxy for Kafka and validates transferring messages with JSON-Schema from Confluent Schema Registry.

## Run example

```shell
docker-compose up -d --build
// Wait for several seconds to everything would start
go run ./cmd/test_producer/*.go
```

## Run service

Eng config:

- `ADDRESS` (string) -- host and port to listen for, default: `localhost:9092`.
- `BROKER` (string) -- Kafka broker to transfer messages, default: `localhost:9093`.
- `SCHEMA_REGISTRY` (url string) -- Confluent schema registry URL, default: `http://localhost:8081`.

Iguana can't still replace broker metadata from the broker's "Metadata response" therefore broker's host and port must be
equal to the `ADDRESS` config. The example of the right config is presented in the `docker-compose.yml`.

Iguana doesn't perform any balancing or Kafka node selection, so You need to start a standalone instance for each Kafka broker.

Iguana still supports only V3 produce request&response.