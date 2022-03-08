package org.bf2.admin.kafka.admin.handlers;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;

import java.io.IOException;

/**
 * Adds Strict-Transport-Security header to all responses
 *
 * @deprecated to be replaced with built-in Quarkus functionality upon platform upgrade
 *
 */
@Provider
@Deprecated(forRemoval = true) // Remove this class when upgrading to Quarkus >= 2.7
public class StrictTransportSecurityHeaderFilter implements ContainerResponseFilter {

    @Inject
    @ConfigProperty(name = "quarkus.http.header.\"Strict-Transport-Security\".value")
    String hstsValue;

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
        responseContext.getHeaders().add("Strict-Transport-Security", hstsValue);
    }

}
