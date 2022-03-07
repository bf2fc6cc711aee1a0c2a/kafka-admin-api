/*
 * Copyright 2022 Red Hat, Inc, and individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.bf2.admin.security;

import io.smallrye.jwt.auth.principal.DefaultJWTCallerPrincipal;
import io.smallrye.jwt.auth.principal.DefaultJWTCallerPrincipalFactory;
import io.smallrye.jwt.auth.principal.DefaultJWTTokenParser;
import io.smallrye.jwt.auth.principal.JWTAuthContextInfo;
import io.smallrye.jwt.auth.principal.JWTCallerPrincipal;
import io.smallrye.jwt.auth.principal.JWTCallerPrincipalFactory;
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

import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Alternative;
import javax.enterprise.inject.Produces;
import javax.interceptor.Interceptor;

import java.util.logging.Logger;

/**
 * CDI producer to provide instance of {@code JWTCallerPrincipalFactory} capable
 * of using a trusted certificate for JWKS retrieval.
 *
 * @deprecated needed until https://github.com/smallrye/smallrye-jwt/issues/571
 *             is fixed and released
 */
@Deprecated(forRemoval = true)
@ApplicationScoped
@Alternative
@Priority(Interceptor.Priority.APPLICATION + 10)
public class TrustedSSOCallerPrincipalFactoryProducer {

    private static final Logger LOG = Logger.getLogger(TrustedSSOCallerPrincipalFactoryProducer.class.getName());

    private final JWTCallerPrincipalFactory factory;

    public TrustedSSOCallerPrincipalFactoryProducer() {
        initCallerPrincipalFactory();
    }

    @Produces
    public JWTCallerPrincipalFactory getFactory() {
        return factory;
    }

    private void initCallerPrincipalFactory() {
        LOG.info("Loading JWTCallerPrincipalFactory from " + getClass());
        factory = new TrustedSSOCallerPrincipalFactory();
        JWTCallerPrincipalFactory.setInstance(factory);
    }

    private static class TrustedSSOCallerPrincipalFactory extends DefaultJWTCallerPrincipalFactory {
        private final DefaultJWTTokenParser parser = new TrustedSSOTokenParser();

        @Override
        public JWTCallerPrincipal parse(final String token, final JWTAuthContextInfo authContextInfo) throws ParseException {
            JwtContext jwtContext = parser.parse(token, authContextInfo);
            String type = jwtContext.getJoseObjects().get(0).getHeader("typ");
            return new DefaultJWTCallerPrincipal(type, jwtContext.getJwtClaims());
        }
    }

    private static class TrustedSSOTokenParser extends DefaultJWTTokenParser {
        private volatile VerificationKeyResolver keyResolver;

        protected VerificationKeyResolver getVerificationKeyResolver(JWTAuthContextInfo authContextInfo)
                throws UnresolvableKeyException {

            if (keyResolver == null) {
                synchronized (this) {
                    if (keyResolver == null) {
                        keyResolver = new TrustedSSOKeyLocationResolver(authContextInfo);
                    }
                }
            }

            return keyResolver;
        }
    }

    static class TrustedSSOKeyLocationResolver extends KeyLocationResolver {
        public TrustedSSOKeyLocationResolver(JWTAuthContextInfo authContextInfo) throws UnresolvableKeyException {
            super(authContextInfo);
        }

        @Override
        protected HttpsJwks initializeHttpsJwks(String location) {
            HttpsJwks theHttpsJwks = new HttpsJwks(location);
            return this.initializeHttpsJwks(theHttpsJwks);
        }

        HttpsJwks initializeHttpsJwks(HttpsJwks theHttpsJwks) {
            Config config = ConfigProvider.getConfig();
            config.getOptionalValue(KafkaAdminConfigRetriever.OAUTH_TRUSTED_CERT, String.class)
                .map(super::loadPEMCertificate)
                .ifPresent(cert -> {
                    Get httpGet = new Get();
                    httpGet.setTrustedCertificates(cert);
                    theHttpsJwks.setSimpleHttpGet(httpGet);
                });

            theHttpsJwks.setDefaultCacheDuration(authContextInfo.getJwksRefreshInterval().longValue() * 60L);
            return theHttpsJwks;
        }
    }
}
