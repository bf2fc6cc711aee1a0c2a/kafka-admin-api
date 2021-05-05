prepare-tests:
	cd systemtests/docker/certificates && ls && ./gen-ca.sh && ./gen-keycloak-certs.sh && ./gen-kafka-certs.sh && ./gen-admin-certs.sh && cd -
	mvn clean install -DskipTests --no-transfer-progress
	docker build ./systemtests -f systemtests/docker/kafka/Dockerfile -t kafka-admin-kafka
	docker build ./systemtests -f systemtests/docker/keycloak/Dockerfile -t kafka-admin-keycloak
	docker build ./systemtests -f systemtests/docker/keycloak-import/Dockerfile -t kafka-admin-keycloak-import
	docker build ./systemtests -f systemtests/docker/zookeeper/Dockerfile -t kafka-admin-zookeeper
	docker build ./ -t kafka-admin

clean-tests:
	rm -rf ./systemtests/docker/certificates/c*
	rm -rf ./systemtests/docker/certificates/key*
	rm -rf ./systemtests/docker/certificates/admin-tls*
	rm -rf systemtests/docker/target
	docker image rm -f kafka-admin kafka-admin-keycloak kafka-admin-kafka kafka-admin-zookeeper kafka-admin-keycloak-import
