## Contributing to the project

### Installing Checkstyle

Project uses checkstyle mvn plugin that is executed during `mvn validate` pase.
Please follow your ide setup for checkstyle. For example for intelij:

https://plugins.jetbrains.com/plugin/1065-checkstyle-idea

## Regenerating OpenAPI file

PRs that make changes in the API should update openapi file by executing:

```
mvn -Popenapi-generate process-classes
```

Please commit generated files along with the PR for review.

### Interacting with local kafka

1. Creating topic

```
kafka-topics.sh --create --bootstrap-server localhost:9092  --partitions=3 --replication-factor=1 --topic test --command-config ./hack/binscripts.properties
```

2. Produce messages using kcat
```
kcat -b localhost:9092 -F ./hack/kcat.properties -P -t test
```


4. Consume messages
```
 kcat -b localhost:9092 -F ./hack/kcat.properties  -C -t test
```

6. Interact with the API to view results
`
curl -s -u admin:admin-secret http://localhost:8080/rest/consumer-groups | jq
`

