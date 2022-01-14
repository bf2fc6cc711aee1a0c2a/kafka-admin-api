# Testing kafka-admin-api
## Running systemtests
### Requirements
Docker environment with at least 2 GB of RAM

### Running tests
Execution
```
mvn clean verify
```
Single class/test execution
```
mvn clean verify -Dit.test=RestEndpointTestIT
mvn clean verify -Dit.test=RestEndpointTestIT#testTopicListAfterCreation
```

### Running tests in an IDE
Before running the tests, ensure that the `generate-sources` and `pre-integration-test` Maven phases have run to build the server image and generate TLS keys/certificates for the tests. If not run by your IDE execute:
```
mvn pre-integration-test
```

### Remote Debugging
The system tests will run with remote debugging enabled on the host's port configured via the `debugPort` system property (e.g. `-DdebugPort=5005`). You can attach your IDE to the remote debug port by first setting a breakpoint in the test method you would like to debug, then attach to the remote debugger on the configured port once the test method breakpoint is hit. The debugger will not be able to attach prior to the Admin container being deployed.
