Systemtests

To be able to run test suite, you need docker with at least 2GB RAM and this: 
```
127.0.0.1            keycloak
127.0.0.1            kafka
```
in your /etc/hosts. This step is required for communication with kafka
inside docker container.