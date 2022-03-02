package org.bf2.admin.security;

import io.smallrye.jwt.auth.principal.DefaultJWTCallerPrincipal;
import io.smallrye.jwt.auth.principal.DefaultJWTCallerPrincipalFactory;
import io.smallrye.jwt.auth.principal.DefaultJWTTokenParser;
import io.smallrye.jwt.auth.principal.JWTAuthContextInfo;
import io.smallrye.jwt.auth.principal.JWTCallerPrincipal;
import io.smallrye.jwt.auth.principal.KeyLocationResolver;
import io.smallrye.jwt.auth.principal.ParseException;
import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jose4j.http.Get;
import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jwt.consumer.JwtContext;
import org.jose4j.keys.resolvers.VerificationKeyResolver;
import org.jose4j.lang.UnresolvableKeyException;

import java.io.IOException;

/**
 * {@link DefaultJWTCallerPrincipalFactory} implementation that supports
 * providing a trusted certificate for JWKS retrieval.
 *
 * @deprecated needed until https://github.com/smallrye/smallrye-jwt/issues/571
 *             is fixed and released
 */
@Deprecated(forRemoval = true)
public class TrustedSSOPrincipalFactory extends DefaultJWTCallerPrincipalFactory {

    private final DefaultJWTTokenParser parser = new DefaultJWTTokenParser() {
        private volatile VerificationKeyResolver keyResolver;

        protected VerificationKeyResolver getVerificationKeyResolver(JWTAuthContextInfo authContextInfo) throws UnresolvableKeyException {
            if (keyResolver == null) {
                synchronized (this) {
                    if (keyResolver == null) {
                        keyResolver = new KeyLocationResolver(authContextInfo) {
                            @Override
                            protected HttpsJwks initializeHttpsJwks(String location) throws IOException {
                                HttpsJwks theHttpsJwks = new HttpsJwks(location);

                                Config config = ConfigProvider.getConfig();
                                config.getOptionalValue(KafkaAdminConfigRetriever.OAUTH_TRUSTED_CERT, String.class)
                                      .ifPresent(cert -> {
                                          Get httpGet = new Get();
                                          httpGet.setTrustedCertificates(loadPEMCertificate(cert));
                                          theHttpsJwks.setSimpleHttpGet(httpGet);
                                      });

                                theHttpsJwks.setDefaultCacheDuration(authContextInfo.getJwksRefreshInterval().longValue() * 60L);
                                return theHttpsJwks;
                            }
                        };
                    }
                }
            }
            return keyResolver;
        }
    };

    @Override
    public JWTCallerPrincipal parse(final String token, final JWTAuthContextInfo authContextInfo) throws ParseException {
        JwtContext jwtContext = parser.parse(token, authContextInfo);
        String type = jwtContext.getJoseObjects().get(0).getHeader("typ");
        return new DefaultJWTCallerPrincipal(type, jwtContext.getJwtClaims());
    }

}
