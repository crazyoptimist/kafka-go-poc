# Kafka with Schema Registry POC

In Go with Protobuf as schema type

### Kafka Local Setup

Use this [docker compose file](https://gist.github.com/crazyoptimist/b71971ce5daea9d1cdf7f2b84e7abeb3).

```
docker compose up -d
```

### Generate Code from Proto

```
make generate
```

### Run Producer

```
go run ./cmd/producer
```
