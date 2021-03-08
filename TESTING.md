# Testing kafka-admin-api
## Running systemtests
### Requirements
Docker environment with at least 2 GB of RAM

### Prerequisition
Prepare images
```
make prepare-tests
```
For cleaning
```
make clean-test 
```
Add hostname resolution for OAUTH tests into your /etc/hosts.
```
127.0.0.1            keycloak
127.0.0.1            kafka
```
Config is only necessary for running OAUTH tests, if you run tests without OAUTH you don't have to run add that config.
### Running tests
Parallel execution
```
mvn clean verify -pl systemtests -Djunit.jupiter.execution.parallel.enabled=true -Djunit.jupiter.execution.parallel.config.fixed.parallelism=3
```
Classic execution
```
mvn clean verify -pl systemtests
```
Single class/test execution for parallel
```
mvn clean verify -pl systemtests -Dit.test=RestEndpointTestIT -Djunit.jupiter.execution.parallel.enabled=true -Djunit.jupiter.execution.parallel.config.fixed.parallelism=3
mvn clean verify -pl systemtests -Dit.test=RestEndpointTestIT#testTopicListAfterCreation -Djunit.jupiter.execution.parallel.enabled=true -Djunit.jupiter.execution.parallel.config.fixed.parallelism=3
```
Single class/test execution for classic execution
```
mvn clean verify -pl systemtests -Dit.test=RestEndpointTestIT
mvn clean verify -pl systemtests -Dit.test=RestEndpointTestIT#testTopicListAfterCreation
```