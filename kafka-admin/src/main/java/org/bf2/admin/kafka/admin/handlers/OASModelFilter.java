package org.bf2.admin.kafka.admin.handlers;

import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.openapi.OASFactory;
import org.eclipse.microprofile.openapi.OASFilter;
import org.eclipse.microprofile.openapi.models.OpenAPI;
import org.eclipse.microprofile.openapi.models.security.SecurityScheme.Type;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OASModelFilter implements OASFilter {

    private static final String SECURITY_SCHEME_NAME_OAUTH = "Bearer";
    private static final String SECURITY_SCHEME_NAME_BASIC = "BasicAuth";

    @Override
    public void filterOpenAPI(OpenAPI openAPI) {
        Config config = ConfigProvider.getConfig();

        if (config.getOptionalValue(KafkaAdminConfigRetriever.OAUTH_ENABLED, Boolean.class).orElse(true)) {
            Optional<String> tokenUrl = config.getOptionalValue(KafkaAdminConfigRetriever.OAUTH_TOKEN_ENDPOINT_URI, String.class);

            openAPI.getComponents()
                    .getSecuritySchemes()
                    .get(SECURITY_SCHEME_NAME_OAUTH)
                    .getFlows()
                    .getClientCredentials()
                    .setTokenUrl(tokenUrl.orElse(null));
        } else if (config.getOptionalValue(KafkaAdminConfigRetriever.BASIC_ENABLED, Boolean.class).orElse(false)) {
            openAPI.setSecurity(List.of(OASFactory.createSecurityRequirement()
                                        .addScheme(SECURITY_SCHEME_NAME_BASIC, Collections.emptyList())));
            openAPI.getComponents()
                .setSecuritySchemes(Map.of(SECURITY_SCHEME_NAME_BASIC,
                                           OASFactory.createSecurityScheme().type(Type.HTTP).scheme("basic")));
        } else {
            openAPI.setSecurity(null);
            openAPI.getComponents().setSecuritySchemes(null);
        }
    }

}
