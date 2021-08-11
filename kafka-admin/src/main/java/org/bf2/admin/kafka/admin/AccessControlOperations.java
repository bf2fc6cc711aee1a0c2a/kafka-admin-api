package org.bf2.admin.kafka.admin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.admin.model.Types;
import org.bf2.admin.kafka.admin.model.Types.PagedResponse;
import org.bf2.admin.kafka.admin.model.Types.SortDirectionEnum;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AccessControlOperations {

    public static final String INVALID_ACL_RESOURCE_OPERATION = "Invalid ACL binding resourceType or operation";

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
            promise.fail(new IllegalArgumentException(INVALID_ACL_RESOURCE_OPERATION));
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
                        Types.OrderByInput sortOrder) {

        var pendingResults = new ArrayList<KafkaFuture<Collection<AclBinding>>>(2);

        pendingResults.add(client.describeAcls(filter.toKafkaBindingFilter()).values());

        if (!filter.getPrincipal().isBlank() && !WILDCARD_PRINCIPAL.equals(filter.getPrincipal())) {
            // Include results that apply for "all principals"
            filter.setPrincipal(WILDCARD_PRINCIPAL);
            pendingResults.add(client.describeAcls(filter.toKafkaBindingFilter()).values());
        }

        KafkaFuture.allOf(pendingResults.toArray(KafkaFuture[]::new))
            .whenComplete((nothing, error) ->
                collectBindings(pendingResults, sortOrder, error)
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

    static Future<List<Types.AclBinding>> collectBindings(List<KafkaFuture<Collection<AclBinding>>> pendingResults, Types.OrderByInput sortOrder, Throwable error) {
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
                .sorted(getComparator(sortOrder))
                .map(Types.AclBinding::fromKafkaBinding)
                .collect(Collectors.toList()));
        } else {
            promise.fail(error);
        }

        return promise.future();
    }

    static Comparator<AclBinding> getComparator(Types.OrderByInput sortOrder) {
        Function<AclBinding, String> extractor;

        switch (sortOrder.getField()) {
            case Types.AclBinding.PROP_RESOURCE_NAME:
                extractor = binding -> binding.pattern().name();
                break;
            case Types.AclBinding.PROP_RESOURCE_TYPE:
                extractor = binding -> binding.pattern().resourceType().name();
                break;
            case Types.AclBinding.PROP_PATTERN_TYPE:
                extractor = binding -> binding.pattern().patternType().name();
                break;
            case Types.AclBinding.PROP_PRINCIPAL:
                extractor = binding -> binding.entry().principal();
                break;
            case Types.AclBinding.PROP_OPERATION:
                extractor = binding -> binding.entry().operation().name();
                break;
            case Types.AclBinding.PROP_PERMISSION:
            default:
                extractor = binding -> binding.entry().permissionType().name();
                break;
        }

        Comparator<AclBinding> comparator = Comparator.comparing(extractor);

        if (sortOrder.getOrder() == SortDirectionEnum.DESC) {
            comparator = comparator.reversed();
        }

        return comparator;
    }
}
