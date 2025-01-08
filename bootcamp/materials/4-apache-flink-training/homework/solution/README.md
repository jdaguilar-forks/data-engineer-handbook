# Flink Sessionization Job

## Overview

This Flink job sessionizes input data by IP address and host using a 5-minute gap. The sessionized data is then stored in a PostgreSQL database.

## Prerequisites

- Apache Flink
- Kafka
- PostgreSQL
- Python 3.x
- Required Python packages: `pyflink`

## Environment Variables

Ensure the following environment variables are set:

```plaintext
KAFKA_WEB_TRAFFIC_SECRET="<GET FROM WEBSITE>"
KAFKA_WEB_TRAFFIC_KEY="<GET FROM WEBSITE>
IP_CODING_KEY="MAKE AN ACCOUNT AT https://www.ip2location.io/ TO GET KEY"

KAFKA_GROUP=web-events
KAFKA_TOPIC=bootcamp-events-prod
KAFKA_URL=pkc-rgm37.us-west-2.aws.confluent.cloud:9092

FLINK_VERSION=1.16.0
PYTHON_VERSION=3.7.9



POSTGRES_URL="jdbc:postgresql://host.docker.internal:5432/postgres"
JDBC_BASE_URL="jdbc:postgresql://host.docker.internal:5432"
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=postgres
```

## Running the Job

1. Set up the environment variables, use `example.env`.

2. Start the Flink job:

   ```bash
   python homework_solution.py
   ```