# Kafka Idempotent Consumer With DynamoDB Project

Spring Boot application demonstrating the Kafka Idempotent Consumer pattern using DynamoDB as the backing store for the
event deduplication.

## Integration Tests

Run integration tests with `mvn clean test`

The tests demonstrate event deduplication with the Idempotent Consumer pattern when duplicate events are consumed by the 
application.

## Component Tests

The tests demonstrate event deduplication when duplicate events are consumed by the application using the Idempotent
Consumer pattern.   They use a dockerised Kafka broker, a dockerised Localstack container running DynamoDB as the 
database, and a dockerised wiremock to represent a third party service.  

This call to the third party service simulates a delayed response, enabling messages sent in parallel to be processed
in the same time window.  This enables demonstration of event deduplication.

Two instances of the service are also running in docker containers.

Build Spring Boot application jar:
```
mvn clean install
```

Build Docker container:
```
docker build -t ct/kafka-idempotent-consumer-dynamodb:latest .
```

Run tests:
```
mvn test -Pcomponent
```

Run tests leaving containers up:
```
mvn test -Pcomponent -Dcontainers.stayup
```

Manual clean up (if left containers up):
```
docker rm -f $(docker ps -aq)
```
