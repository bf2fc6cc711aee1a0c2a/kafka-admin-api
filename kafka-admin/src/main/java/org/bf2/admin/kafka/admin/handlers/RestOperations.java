package org.bf2.admin.kafka.admin.handlers;

import io.micrometer.core.annotation.Counted;
import io.micrometer.core.annotation.Timed;
import io.vertx.core.Vertx;
import io.vertx.kafka.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.admin.AccessControlOperations;
import org.bf2.admin.kafka.admin.ConsumerGroupOperations;
import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.bf2.admin.kafka.admin.TopicOperations;
import org.bf2.admin.kafka.admin.model.Types;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.context.ThreadContext;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Path;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.regex.Pattern;

@Path("/rest")
public class RestOperations implements OperationsHandler {

    private static final Logger log = LogManager.getLogger(RestOperations.class);

    private static final Pattern MATCH_ALL = Pattern.compile(".*");

    @Inject
    Vertx vertx;

    @Inject
    @ConfigProperty(name = "kafka.admin.num.partitions.max")
    int maxPartitions;

    @Inject
    KafkaAdminConfigRetriever config;

    @Inject
    AdminClientFactory clientFactory;

    @Inject
    AccessControlOperations aclOperations;

    @Inject
    TopicOperations topicOperations;

    @Inject
    ThreadContext threadContext;

    @Override
    @Counted("create_topic_requests")
    @Timed("create_topic_request_time")
    public CompletionStage<Response> createTopic(Types.NewTopic inputTopic) {
        if (!numPartitionsValid(inputTopic.getSettings(), maxPartitions)) {
            return badRequest(String.format("Number of partitions for topic %s must between 1 and %d (inclusive)",
                    inputTopic.getName(),
                    maxPartitions));
        }

        return withAdminClient(client -> topicOperations.createTopic(KafkaAdminClient.create(vertx, client), inputTopic))
                .thenApply(createdTopic -> Response.created(URI.create("/rest/topics/" + createdTopic.getName())).entity(createdTopic).build());
    }

    @Override
    @Counted("describe_topic_requests")
    @Timed("describe_topic_request_time")
    public CompletionStage<Response> describeTopic(String topicToDescribe) {
        return withAdminClient(client -> topicOperations.describeTopic(KafkaAdminClient.create(vertx, client), topicToDescribe))
                .thenApply(topic -> Response.ok().entity(topic).build());
    }

    @Override
    @Counted("update_topic_requests")
    @Timed("update_topic_request_time")
    public CompletionStage<Response> updateTopic(String topicToUpdate, Types.UpdatedTopic updatedTopic) {
        if (!numPartitionsLessThanMax(updatedTopic, maxPartitions)) {
            return badRequest(String.format("Number of partitions for topic %s must between 1 and %d (inclusive)",
                                            topicToUpdate,
                                            maxPartitions));
        }

        updatedTopic.setName(topicToUpdate);

        return withAdminClient(client -> topicOperations.updateTopic(KafkaAdminClient.create(vertx, client), updatedTopic))
                .thenApply(topic -> Response.ok().entity(topic).build());
    }

    @Override
    @Counted("delete_topic_requests")
    @Timed("delete_topic_request_time")
    public CompletionStage<Response> deleteTopic(String topicToDelete) {
        return withAdminClient(client -> topicOperations.deleteTopics(KafkaAdminClient.create(vertx, client), Collections.singletonList(topicToDelete)))
                .thenApply(topicNames -> Response.ok().entity(topicNames).build());
    }

    @Override
    @Counted("list_topics_requests")
    @Timed("list_topics_request_time")
    public CompletionStage<Response> listTopics(String filter, UriInfo requestUri) {
        final Pattern pattern;

        if (filter != null && !filter.isEmpty()) {
            pattern = Pattern.compile(filter, Pattern.CASE_INSENSITIVE);
        } else {
            pattern = null;
        }

        Types.PageRequest pageParams = parsePageRequest(requestUri);
        Types.OrderByInput orderBy = getOrderByInput(requestUri);

        return withAdminClient(client -> topicOperations.getTopicList(KafkaAdminClient.create(vertx, client), pattern, pageParams, orderBy))
               .thenApply(topicList -> Response.ok().entity(topicList).build());
    }

    @Override
    @Counted("list_groups_requests")
    @Timed("list_groups_request_time")
    public CompletionStage<Response> listGroups(String consumerGroupIdFilter, String topicFilter, UriInfo requestUri) {
        Types.PageRequest pageParams = parsePageRequest(requestUri);
        Types.OrderByInput orderBy = getOrderByInput(requestUri);

        if (log.isDebugEnabled()) {
            log.debug("listGroups orderBy: field: {}, order: {}", orderBy.getField(), orderBy.getOrder());
        }

        final Pattern topicPattern = filterPattern(topicFilter);
        final Pattern groupPattern = filterPattern(consumerGroupIdFilter);

        return withAdminClient(client -> ConsumerGroupOperations.getGroupList(KafkaAdminClient.create(vertx, client), topicPattern, groupPattern, pageParams, orderBy))
                .thenApply(groupList -> Response.ok().entity(groupList).build());
    }

    @Override
    @Counted("get_group_requests")
    @Timed("describe_group_request_time")
    public CompletionStage<Response> describeGroup(String groupToDescribe, Optional<Integer> partitionFilter, UriInfo requestUri) {
        Types.OrderByInput orderBy = getOrderByInput(requestUri);

        return withAdminClient(client -> ConsumerGroupOperations.describeGroup(KafkaAdminClient.create(vertx, client), groupToDescribe, orderBy, partitionFilter.orElse(-1)))
                .thenApply(consumerGroup -> Response.ok().entity(consumerGroup).build());
    }

    @Override
    @Counted("delete_group_requests")
    @Timed("delete_group_request_time")
    public CompletionStage<Response> deleteGroup(String groupToDelete) {
        return withAdminClient(client ->  ConsumerGroupOperations.deleteGroup(KafkaAdminClient.create(vertx, client), Collections.singletonList(groupToDelete)))
                .thenApply(consumerGroupNames -> Response.noContent().build());
    }

    @Override
    @Counted("reset_group_offset_requests")
    @Timed("reset_group_offset_request_time")
    public CompletionStage<Response> resetGroupOffset(String groupToReset, Types.ConsumerGroupOffsetResetParameters parameters) {
        parameters.setGroupId(groupToReset);

        return withAdminClient(client -> ConsumerGroupOperations.resetGroupOffset(KafkaAdminClient.create(vertx, client), parameters))
                .thenApply(groupList -> Response.ok().entity(groupList).build());
    }

    @Override
    @Counted("get_acl_resource_operations_requests")
    @Timed("get_acl_resource_operations_request_time")
    public Response getAclResourceOperations() {
        return Response.ok(config.getAclResourceOperations()).build();
    }

    @Override
    @Counted("describe_acls_requests")
    @Timed("describe_acls_request_time")
    public CompletionStage<Response> describeAcls(UriInfo requestUri) {
        var filter = Types.AclBinding.fromQueryParams(requestUri.getQueryParameters());

        return withAdminClient(client -> aclOperations.getAcls(client, filter, parsePageRequest(requestUri), getOrderByInput(requestUri, Types.AclBinding.DEFAULT_ORDER)))
                .thenApply(aclList -> Response.ok().entity(aclList).build());
    }

    @Override
    @Counted("create_acls_requests")
    @Timed("create_acls_request_time")
    public CompletionStage<Response> createAcl(Types.AclBinding binding) {
        return withAdminClient(client -> aclOperations.createAcl(client, binding))
                .thenApply(nothing -> Response.created(binding.buildUri(UriBuilder.fromResource(getClass()).path(getClass(), "describeAcls"))).build());
    }

    @Override
    @Counted("delete_acls_requests")
    @Timed("delete_acls_request_time")
    public CompletionStage<Response> deleteAcls(UriInfo requestUri) {
        var filter = Types.AclBinding.fromQueryParams(requestUri.getQueryParameters());
        return withAdminClient(client -> aclOperations.deleteAcls(client, filter))
                .thenApply(aclList -> Response.ok().entity(aclList).build());
    }

    private boolean numPartitionsValid(Types.NewTopicInput settings, int maxPartitions) {
        int partitions = settings.getNumPartitions() != null ?
                settings.getNumPartitions() :
                    TopicOperations.DEFAULT_PARTITIONS;

        return partitions > 0 && partitions <= maxPartitions;
    }

    private boolean numPartitionsLessThanMax(Types.UpdatedTopic settings, int maxPartitions) {
        if (settings.getNumPartitions() != null) {
            return settings.getNumPartitions() <= maxPartitions;
        } else {
            // user did not change the partitions
            return true;
        }
    }

    private Types.OrderByInput getOrderByInput(UriInfo requestUri, Types.OrderByInput defaultOrderBy) {
        final String paramOrderKey = requestUri.getQueryParameters().getFirst("orderKey");
        final String paramOrder = requestUri.getQueryParameters().getFirst("order");

        if (paramOrderKey == null && paramOrder == null) {
            return defaultOrderBy;
        }

        Types.SortDirectionEnum sortReverse = paramOrder == null ? defaultOrderBy.getOrder() : Types.SortDirectionEnum.fromString(paramOrder);
        String sortKey = paramOrderKey == null ? defaultOrderBy.getField() : paramOrderKey;

        Types.OrderByInput orderBy = new Types.OrderByInput();
        orderBy.setField(sortKey);
        orderBy.setOrder(sortReverse);
        return orderBy;
    }

    private Types.OrderByInput getOrderByInput(UriInfo requestUri) {
        return getOrderByInput(requestUri, new Types.OrderByInput("name", Types.SortDirectionEnum.ASC));
    }

    private Types.PageRequest parsePageRequest(UriInfo requestUri) {
        Types.PageRequest pageRequest = new Types.PageRequest();
        var queryParams = requestUri.getQueryParameters();

        boolean deprecatedPaginationUsed = false;
        if (queryParams.get("offset") != null || queryParams.get("limit") != null) {
            deprecatedPaginationUsed = true;
        }
        pageRequest.setDeprecatedFormat(deprecatedPaginationUsed);

        if (deprecatedPaginationUsed) {
            Integer offsetInt = parseParameter(queryParams, "offset", 0);
            Integer limitInt = parseParameter(queryParams, "limit", 10);
            pageRequest.setOffset(offsetInt);
            pageRequest.setLimit(limitInt);
        } else {
            Integer pageInt = parseParameter(queryParams, "page", 1);
            Integer sizeInt = parseParameter(queryParams, "size", 10);
            pageRequest.setPage(pageInt);
            pageRequest.setSize(sizeInt);

            if (sizeInt < 1 || pageInt < 1) {
                throw new BadRequestException(Response.status(Status.BAD_REQUEST)
                      .entity(new Types.Error(Status.BAD_REQUEST.getStatusCode(), "Size and page have to be positive integers."))
                      .build());
            }
        }

        return pageRequest;
    }

    private Integer parseParameter(MultivaluedMap<String, String> queryParams, String name, int defaultValue) {
        if (queryParams.containsKey(name)) {
            try {
                return Integer.valueOf(queryParams.getFirst(name));
            } catch (Exception e) {
                throw new BadRequestException(Response.status(Status.BAD_REQUEST)
                                              .entity(new Types.Error(Status.BAD_REQUEST.getStatusCode(), "Invalid parameter value for `" + name + "`"))
                                              .build());
            }
        }

        return defaultValue;
    }

    private Pattern filterPattern(String filter) {
        if (filter == null || filter.isBlank()) {
            return MATCH_ALL;
        }

        return Pattern.compile(Pattern.quote(filter), Pattern.CASE_INSENSITIVE);
    }

    <R> CompletionStage<R> withAdminClient(Function<AdminClient, CompletionStage<R>> function) {
        final AdminClient client = clientFactory.createAdminClient();

        return threadContext.withContextCapture(function.apply(client))
                .whenComplete((result, error) -> {
                    try {
                        client.close();
                    } catch (Exception e) {
                        log.warn("Exception closing Kafka AdminClient", e);
                    }
                });
    }

    CompletionStage<Response> badRequest(String message) {
        ResponseBuilder response =
                Response.status(Status.BAD_REQUEST)
                    .entity(new Types.Error(Status.BAD_REQUEST.getStatusCode(), message));

        return CompletableFuture.completedStage(response.build());
    }
}
