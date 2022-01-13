prepare-tests:
	cd systemtests/docker/certificates \
	  && ls \
	  && ./gen-ca.sh \
	  && ./gen-keycloak-certs.sh \
	  && ./gen-kafka-certs.sh \
	  && ./gen-admin-certs.sh \
	  && cd - \
	  && mkdir -vp ./systemtests/target/test-classes \
	  && cp -v systemtests/docker/certificates/*.p12 ./systemtests/target/test-classes/
	mvn install -DskipTests --no-transfer-progress
	docker build ./kafka-admin -f kafka-admin/src/main/docker/Dockerfile -t kafka-admin --pull

clean-tests:
	rm -rf ./systemtests/docker/certificates/c*
	rm -rf ./systemtests/docker/certificates/key*
	rm -rf ./systemtests/docker/certificates/admin-tls*
	rm -rf ./systemtests/docker/target
	docker image rm -f kafka-admin || true
	mvn clean
