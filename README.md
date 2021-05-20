[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

# Kafka Admin API

This repository contains the Kafka Admin API and its implementation.
Kafka Admin API provides a way for managing Kafka topics.

## Getting Started

###_Prerequisites_

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
mvn install -DskipTests
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

Once all steps above have been completed, you can run the Kafka Admin API. The server will start the following interfaces:
- Management (`/metrics` and `/health` endpoints) [http://localhost:9990](http://localhost:9990)
- REST API (`/rest/*`) on either [http://localhost:8080](http://localhost:8080) or [https://localhost:8443](https://localhost:8443), depending on TLS configuration.

### Admin Server Configuration

| Environment Variable | Description |
| -------------------- | ----------- |
| KAFKA_ADMIN_BOOTSTRAP_SERVERS | A comma-separated list of host and port pairs that are the addresses of the Kafka brokers in a "bootstrap" Kafka cluster.   |
| KAFKA_ADMIN_OAUTH_ENABLED | Enables a third party application to obtain limited access to the Admin API. |
| KAFKA_ADMIN_TLS_CERT | TLS encryption certificate in PEM format. The value may be either a path to a file containing the certificate *or* text of the certificate. |
| KAFKA_ADMIN_TLS_KEY | TLS encryption private key in PEM format. The value may be either a path to a file containing the key *or* text of the key. *required when `KAFKA_ADMIN_TLS_CERT` is used* |
| KAFKA_ADMIN_TLS_VERSION | A comma-separated list of TLS versions to support for TLS/HTTPS endpoints. E.g. `TLSv1.3,TLSv1.2`. Default value if not specified is `TLSv1.3` |
| KAFKA_ADMIN_INTERNAL_TOPICS_ENABLED | Internal topics are used internally by the Kafka Streams application while executing. |
| KAFKA_ADMIN_INTERNAL_CONSUMER_GROUPS_ENABLED | Internal consumer groups are used internally by the Strimzi Canary application. |
| KAFKA_ADMIN_REPLICATION_FACTOR | Replication factor defines the number of copies of a topic in a Kafka cluster. |
| KAFKA_ADMIN_NUM_PARTITIONS_MAX | Maximum (inclusive) number of partitions that may be used for the creation of a new topic. |


## Releasing

### Milestones
Each release requires an open milestone that includes the issues/pull requests that are part of the release. All issues in the release milestone must be closed. The name of the milestone must match the version number to be released.

### Configuration
The release action flow requires that the following secrets are configured in the repository:
* `IMAGE_REPO_HOSTNAME` - the host (optionally including a port number) of the image repository where images will be pushed
* `IMAGE_REPO_NAMESPACE` - namespace/library/user where the image will be pushed
* `IMAGE_REPO_USERNAME` - user name for authentication to server `IMAGE_REPO_HOSTNAME`
* `IMAGE_REPO_PASSWORD` - password for authentication to server `IMAGE_REPO_HOSTNAME`
These credentials will be used to push the release image to the repository configured in the `.github/workflows/release.yml` workflow.

### Performing the Release
Releases are performed by modifying the `.github/project.yml` file, setting `current-version` to the release version and `next-version` to the next SNAPSHOT. Open a pull request with the changed `project.yml` to initiate the pre-release workflows. At this phase, the project milestone will be checked and it will be verified that no issues for the release milestone are still open. Additionally, the project's integration test will be run.
Once approved and the pull request is merged, the release action will execute. This action will execute the Maven release plugin to tag the release commit, build the application artifacts, create the build image, and push the image to (currently) quay.io. If successful, the action will push the new tag to the Github repository and generate release notes listing all of the closed issues included in the milestone. Finally, the milestone will be closed.

## Logging Configuration Override
The container image built from this repository includes support for providing an additional Log4J properties file at run time. The properties file must be given to the running
container at mount point `/opt/kafka-admin-api/custom-config/log4j2.properties`. Configuration specified in the override file is merged with the static configuration built in
to the image from [docker/log4j2.properties](./docker/log4j2.properties). Individual configuration items in the override properties will replace the static properties of the same
name, as defined in the Apache Log4J 2.x [composite configuration documentation](https://logging.apache.org/log4j/2.x/manual/configuration.html#CompositeConfiguration).
