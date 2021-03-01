prepare-tests:
	cd systemtests/docker/certificates && ls && ./gen-ca.sh && ./gen-keycloak-certs.sh && ./gen-kafka-certs.sh && cd -
	mvn clean install -DskipTests
	docker build ./systemtests -f systemtests/docker/kafka/Dockerfile -t strimzi-admin-kafka
	docker build ./systemtests -f systemtests/docker/keycloak/Dockerfile -t strimzi-admin-keycloak
	docker build ./systemtests -f systemtests/docker/keycloak-import/Dockerfile -t strimzi-admin-keycloak-import
	docker build ./systemtests -f systemtests/docker/zookeeper/Dockerfile -t strimzi-admin-zookeeper
	docker build ./ -t strimzi-admin

clean-tests:
	rm -rf ./systemtests/docker/certificates/c*
	rm -rf ./systemtests/docker/certificates/key*
	rm -rf systemtests/docker/target
	docker image rm strimzi-admin-kafka strimzi-admin-keycloak strimzi-admin-zookeeper