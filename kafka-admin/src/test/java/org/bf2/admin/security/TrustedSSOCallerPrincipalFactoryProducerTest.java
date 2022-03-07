package org.bf2.admin.security;

import io.smallrye.jwt.auth.principal.JWTAuthContextInfo;
import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.bf2.admin.security.TrustedSSOCallerPrincipalFactoryProducer.TrustedSSOKeyLocationResolver;
import org.jose4j.http.SimpleGet;
import org.jose4j.jwk.HttpsJwks;
import org.junit.jupiter.api.Test;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.security.KeyStore;
import java.security.cert.CertificateEncodingException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Deprecated(forRemoval = true)
class TrustedSSOCallerPrincipalFactoryProducerTest {

    @Test
    void testInitializeHttpsJwksWithValidCert() throws Exception {
        JWTAuthContextInfo authContextInfo = new JWTAuthContextInfo();
        authContextInfo.setJwksRefreshInterval(1);
        TrustedSSOKeyLocationResolver target = new TrustedSSOKeyLocationResolver(authContextInfo) {
            @Override
            protected void initializeKeyContent() throws Exception {
                // No-op
            }
        };

        System.setProperty(KafkaAdminConfigRetriever.OAUTH_TRUSTED_CERT, getValidCertificate());
        TestHttpsJwks httpsJwks;

        try {
            httpsJwks = (TestHttpsJwks) target.initializeHttpsJwks(new TestHttpsJwks("https://dummy"));
        } finally {
            System.clearProperty(KafkaAdminConfigRetriever.OAUTH_TRUSTED_CERT);
        }

        assertEquals(1, httpsJwks.setSimpleHttpGetInvocationCount);
    }

    @Test
    void testInitializeHttpsJwksWithInvalidCert() throws Exception {
        JWTAuthContextInfo authContextInfo = new JWTAuthContextInfo();
        authContextInfo.setJwksRefreshInterval(1);
        TrustedSSOKeyLocationResolver target = new TrustedSSOKeyLocationResolver(authContextInfo) {
            @Override
            protected void initializeKeyContent() throws Exception {
                // No-op
            }
        };
        System.setProperty(KafkaAdminConfigRetriever.OAUTH_TRUSTED_CERT, "INVALID");
        TestHttpsJwks httpsJwks;

        try {
            httpsJwks = (TestHttpsJwks) target.initializeHttpsJwks(new TestHttpsJwks("https://dummy"));
        } finally {
            System.clearProperty(KafkaAdminConfigRetriever.OAUTH_TRUSTED_CERT);
        }

        assertEquals(0, httpsJwks.setSimpleHttpGetInvocationCount);
    }

    @Test
    void testInitializeHttpsJwksWithoutCert() throws Exception {
        JWTAuthContextInfo authContextInfo = new JWTAuthContextInfo();
        authContextInfo.setJwksRefreshInterval(1);
        TrustedSSOKeyLocationResolver target = new TrustedSSOKeyLocationResolver(authContextInfo) {
            @Override
            protected void initializeKeyContent() throws Exception {
                // No-op
            }
        };
        TestHttpsJwks httpsJwks;
        httpsJwks = (TestHttpsJwks) target.initializeHttpsJwks(new TestHttpsJwks("https://dummy"));
        assertEquals(0, httpsJwks.setSimpleHttpGetInvocationCount);
    }

    static class TestHttpsJwks extends HttpsJwks {
        int setSimpleHttpGetInvocationCount = 0;

        public TestHttpsJwks(String location) {
            super(location);
        }

        @Override
        public void setSimpleHttpGet(SimpleGet simpleHttpGet) {
            super.setSimpleHttpGet(simpleHttpGet);
            setSimpleHttpGetInvocationCount++;
        }
    }

    static String getValidCertificate() throws Exception {
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init((KeyStore) null);

        List<TrustManager> trustManagers = Arrays.asList(trustManagerFactory.getTrustManagers());

        return trustManagers.stream()
                .filter(X509TrustManager.class::isInstance)
                .map(X509TrustManager.class::cast)
                .map(trustManager -> Arrays.asList(trustManager.getAcceptedIssuers()))
                .flatMap(Collection::stream)
                .map(cert -> {
                    try {
                        return Base64.getEncoder().encodeToString(cert.getEncoded());
                    } catch (CertificateEncodingException e) {
                        throw new IllegalArgumentException(e);
                    }
                })
                .map(cert -> String.format("-----BEGIN CERTIFICATE-----\n%s\n-----END CERTIFICATE-----", cert))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException());
    }
}
