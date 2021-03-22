[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

# Kafka Admin API

This repository contains the Kafka Admin API and its implementation.
Kafka Admin API provides a way for managing Kafka topics.

## Getting Started

### Prerequisites

There are a few things you need to have installed to run this project:

- [Maven](https://maven.apache.org/)
- [JDK 11+](https://openjdk.java.net/projects/jdk/11/)
- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/) (optional but recommended)

### Download

To run this project locally, first clone it with Git:

```shell
git clone git@github.com:bf2fc6cc711aee1a0c2a/kafka-admin-api.git
cd kafka-admin-api
```

### Install dependencies
Now you can install the required dependencies with Maven:

```shell
mvn install
```

### Start a local Kafka cluster

The Kafka Admin API needs a Kafka cluster to connect to. There is a [docker-compose.yml](./docker-compose.yml) file with default Kafka containers you can use to run the server against.

Run the local Kafka cluster:

```shell
docker-compose up -d
```

This will start a Kafka cluster at localhost:9092

### Configure IDE

If you are using IntelliJ there are two Run configurations: `Main` and `Main (EnvFile)` available in the `.run` directory.

**Main**

`Main` uses the default environment variables needed to connect to the Kafka cluster we created in [Start a Kafka cluster](#start-a-local-kafka-cluster).

**Main (EnvFile)**

`Main (EnvFile)` uses the [EnvFile](https://plugins.jetbrains.com/plugin/7861-envfile) plugin to load environment variables from a `.env` file. To use this configuration please do the following:

1. Install [EnvFile](https://plugins.jetbrains.com/plugin/7861-envfile).
2. Create a `.env` file in your project root. You can use the example file, which already has the correct variables: `cp .env.example .env`.

### Run the Admin Server

Once all steps above have been completed, you can run the Kafka Admin API. The server will start at [http://localhost:8080](http://localhost:8080).

### Admin Server Configuration

| Environment Variable | Description |
| -------------------- | ----------- |
| KAFKA_ADMIN_BOOTSTRAP_SERVERS | A comma-separated list of host and port pairs that are the addresses of the Kafka brokers in a "bootstrap" Kafka cluster.   |
| KAFKA_ADMIN_OAUTH_ENABLED | Enables a third party application to obtain limited access to the Admin API. |
| INTERNAL_TOPICS_ENABLED | Internal topics are used internally by the Kafka Streams application while executing. |
| REPLICATION_FACTORY | Replication factor defines the number of copies of a topic in a Kafka cluster. |