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
make clean-tests
```

### Running tests
Execution
```
mvn clean verify -pl systemtests
```
Single class/test execution
```
mvn clean verify -pl systemtests -Dit.test=RestEndpointTestIT
mvn clean verify -pl systemtests -Dit.test=RestEndpointTestIT#testTopicListAfterCreation
```

### Remote Debugging
The system tests will run with remote debugging enabled on the host's port configured via the `debugPort` system property (e.g. `-DdebugPort=5005`). You can attach your IDE to the remote debug port by first setting a breakpoint in the test method you would like to debug, then attach to the remote debugger on the configured port once the test method breakpoint is hit. The debugger will not be able to attach prior to the Admin container being deployed.
