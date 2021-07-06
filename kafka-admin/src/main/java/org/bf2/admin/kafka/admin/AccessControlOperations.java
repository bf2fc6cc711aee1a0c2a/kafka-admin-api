package org.bf2.admin.kafka.admin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.admin.model.Types;
import org.bf2.admin.kafka.admin.model.Types.PagedResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class AccessControlOperations {

    private static final Logger log = LogManager.getLogger(AccessControlOperations.class);

    private static final String WILDCARD_PRINCIPAL = KafkaPrincipal.USER_TYPE + ":*";

    private static final TypeReference<Map<String, List<String>>> TYPEREF_MAP_LIST_STRING =
        new TypeReference<>() {
            // Intentionally blank
        };

    public static final TypeReference<Types.AclBinding> TYPEREF_ACL_BINDING =
        new TypeReference<>() {
            // Intentionally blank
        };

    private final Map<String, List<String>> resourceOperations;

    static class AccessControlOperationException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        AccessControlOperationException(Throwable cause) {
            super(cause);
        }
    }

    public AccessControlOperations(KafkaAdminConfigRetriever config) {
        try {
            this.resourceOperations = new ObjectMapper().readValue(config.getAclResourceOperations(), TYPEREF_MAP_LIST_STRING);
        } catch (JsonProcessingException e) {
            log.error("Failed to parse value of ACL resource-operations", e);
            throw new AccessControlOperationException(e);
        }
    }

    public void createAcl(Admin client, Promise<Void> promise, Types.AclBinding binding) {
        if (!validAclBinding(binding)) {
            promise.fail(new IllegalArgumentException("Invalid ACL binding resourceType or operation"));
            return;
        }

        client.createAcls(List.of(binding.toKafkaBinding()))
            .all()
            .whenComplete((nothing, exception) -> {
                if (exception != null) {
                    promise.fail(exception);
                } else {
                    promise.complete();
                }
            });
    }

    public void getAcls(Admin client,
                        Promise<Types.PagedResponse<Types.AclBinding>> promise,
                        Types.AclBinding filter,
                        Types.PageRequest pageRequest,
                        Types.OrderByInput orderByInput) {

        var pendingResults = new ArrayList<KafkaFuture<Collection<AclBinding>>>(2);

        pendingResults.add(client.describeAcls(filter.toKafkaBindingFilter()).values());

        if (!filter.getPrincipal().isBlank() && !WILDCARD_PRINCIPAL.equals(filter.getPrincipal())) {
            // Include results that apply for "all principals"
            filter.setPrincipal(WILDCARD_PRINCIPAL);
            pendingResults.add(client.describeAcls(filter.toKafkaBindingFilter()).values());
        }

        KafkaFuture.allOf(pendingResults.toArray(KafkaFuture[]::new))
            .whenComplete((nothing, error) ->
                collectBindings(pendingResults, error)
                    .onFailure(promise::fail)
                    .onSuccess(bindings ->
                        PagedResponse.forPage(pageRequest, bindings)
                            .onFailure(promise::fail)
                            .onSuccess(promise::complete)));
    }

    public void deleteAcls(Admin client,
                           Promise<Types.PagedResponse<Types.AclBinding>> promise,
                           Types.AclBinding filter) {

        client.deleteAcls(List.of(filter.toKafkaBindingFilter()))
            .all()
            .whenComplete((bindingCollection, error) ->
                collectBindings(bindingCollection, error)
                    .onFailure(promise::fail)
                    .onSuccess(bindings ->
                        PagedResponse.forItems(bindings)
                            .onFailure(promise::fail)
                            .onSuccess(promise::complete)));
    }

    private boolean validAclBinding(Types.AclBinding binding) {
        return resourceOperations.getOrDefault(binding.getResourceType().toLowerCase(Locale.ENGLISH),
                                               Collections.emptyList())
                .contains(binding.getOperation().toLowerCase(Locale.ENGLISH));
    }

    static Future<List<Types.AclBinding>> collectBindings(Collection<AclBinding> bindings, Throwable error) {
        Promise<List<Types.AclBinding>> promise = Promise.promise();
        if (error == null) {
            promise.complete(bindings.stream().map(Types.AclBinding::fromKafkaBinding).collect(Collectors.toList()));
        } else {
            promise.fail(error);
        }
        return promise.future();
    }

    static Future<List<Types.AclBinding>> collectBindings(List<KafkaFuture<Collection<AclBinding>>> pendingResults, Throwable error) {
        Promise<List<Types.AclBinding>> promise = Promise.promise();

        if (error == null) {
            promise.complete(pendingResults.stream()
                .map(kafkaFuture -> {
                    // The future is already completed, nothing _should_ be thrown
                    try {
                        return kafkaFuture.get();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AccessControlOperationException(e);
                    } catch (Exception e) {
                        throw new AccessControlOperationException(e);
                    }
                })
                .flatMap(Collection::stream)
                .sorted(AccessControlOperations::denyFirst)
                .map(Types.AclBinding::fromKafkaBinding)
                .collect(Collectors.toList()));
        } else {
            promise.fail(error);
        }

        return promise.future();
    }

    static int denyFirst(AclBinding b1, AclBinding b2) {
        AclPermissionType p1 = b1.entry().permissionType();
        AclPermissionType p2 = b2.entry().permissionType();

        if (p1 == p2) {
            return 0;
        }

        return p1 == AclPermissionType.DENY ? -1 : 1;
    }
}
