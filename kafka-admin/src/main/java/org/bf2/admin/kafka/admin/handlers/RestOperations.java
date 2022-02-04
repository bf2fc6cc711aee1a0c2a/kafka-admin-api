package org.bf2.admin.kafka.admin.handlers;

import io.micrometer.core.annotation.Counted;
import io.micrometer.core.annotation.Timed;
import io.smallrye.common.annotation.Blocking;
import io.vertx.core.Vertx;
import io.vertx.kafka.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.header.Header;
import org.bf2.admin.kafka.admin.AccessControlOperations;
import org.bf2.admin.kafka.admin.ConsumerGroupOperations;
import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.bf2.admin.kafka.admin.TopicOperations;
import org.bf2.admin.kafka.admin.model.Types;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.context.ThreadContext;
import org.jboss.logging.Logger;

import javax.inject.Inject;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.ws.rs.BeanParam;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Path("/rest")
public class RestOperations implements OperationsHandler {

    private static final Logger log = Logger.getLogger(RestOperations.class);

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
                .thenApply(createdTopic -> Response.created(uriBuilder("describeTopic").build(createdTopic.getName())).entity(createdTopic).build());
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
    public CompletionStage<Response> updateTopic(String topicName, Types.TopicSettings updatedTopic) {
        if (!numPartitionsLessThanEqualToMax(updatedTopic, maxPartitions)) {
            return badRequest(String.format("Number of partitions for topic %s must between 1 and %d (inclusive)",
                                            topicName,
                                            maxPartitions));
        }

        return withAdminClient(client -> topicOperations.updateTopic(KafkaAdminClient.create(vertx, client), topicName, updatedTopic))
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
    public CompletionStage<Response> listTopics(String filter, Types.DeprecatedPageRequest pageParams, Types.TopicSortParams sortParams) {
        final Pattern pattern;

        if (filter != null && !filter.isEmpty()) {
            pattern = Pattern.compile(filter, Pattern.CASE_INSENSITIVE);
        } else {
            pattern = null;
        }

        sortParams.setDefaultsIfNecessary();

        return withAdminClient(client -> topicOperations.getTopicList(KafkaAdminClient.create(vertx, client), pattern, pageParams, sortParams))
               .thenApply(topicList -> Response.ok().entity(topicList).build());
    }

    @GET
    @Path("topics/{topicName}/records")
    @Blocking
    public Response consumeRecords(@PathParam("topicName") String topicName,
                                   @QueryParam("partition") Integer partition,
                                   @QueryParam("offset") Integer offset,
                                   @QueryParam("timestamp") String timestamp,
                                   @QueryParam("limit") @DefaultValue("20") Integer limit,
                                   @QueryParam("include") List<String> include,
                                   UriInfo requestUri) {

        try (Consumer<String, String> consumer = clientFactory.createConsumer(limit)) {
            List<PartitionInfo> partitions = consumer.partitionsFor(topicName);

            if (partitions.isEmpty()) {
                return Response.status(Status.NOT_FOUND)
                        .entity(Json.createObjectBuilder()
                                .add("code", Status.NOT_FOUND.getStatusCode())
                                .add("error_message", "No such topic")
                                .build()
                                .toString()) // FIXME: toString shouldn't be required
                        .build();
            }

            List<TopicPartition> assignments = partitions.stream()
                .filter(p -> partition == null || partition.equals(p.partition()))
                .map(p -> new TopicPartition(p.topic(), p.partition()))
                .collect(Collectors.toList());

            if (assignments.isEmpty()) {
                return Response.status(Status.BAD_REQUEST)
                        .entity(Json.createObjectBuilder()
                                .add("code", Status.BAD_REQUEST.getStatusCode())
                                .add("error_message", String.format("No such partition for topic %s: %d", topicName, partition))
                                .build()
                                .toString()) // FIXME: toString shouldn't be required
                        .build();
            }

            consumer.assign(assignments);

            if (timestamp != null) {
                Long tsMillis = ZonedDateTime.parse(timestamp).toInstant().toEpochMilli();
                Map<TopicPartition, Long> timestampsToSearch =
                        assignments.stream().collect(Collectors.toMap(Function.identity(), p -> tsMillis));
                consumer.offsetsForTimes(timestampsToSearch)
                    .forEach((p, tsOffset) -> consumer.seek(p, tsOffset.offset()));
            } else if (offset != null) {
                assignments.forEach(p -> consumer.seek(p, offset));
            }

            var records = consumer.poll(Duration.ofSeconds(2));
            JsonArrayBuilder items = Json.createArrayBuilder();
            records.forEach(rec -> {
                JsonObjectBuilder item = Json.createObjectBuilder();

                addField("partition", include, item, rec::partition);
                addField("offset", include, item, rec::offset);
                addField("timestamp", include, item, () -> Instant.ofEpochMilli(rec.timestamp()).atZone(ZoneOffset.UTC).toString());
                addField("timestampType", include, item, rec::timestampType);
                addField("key", include, item, () -> rec.key() != null ? Json.createValue(rec.key()) : JsonValue.NULL);

                if (include.isEmpty() || include.contains("headers")) {
                    JsonObjectBuilder headers = Json.createObjectBuilder();
                    rec.headers().forEach(h -> headers.add(h.key(), new String(h.value())));
                    item.add("headers", headers);
                }

                addField("value", include, item, rec::value);
                items.add(item);
            });

            return Response.ok()
                    .entity(Json.createObjectBuilder()
                            .add("size", records.count())
                            .add("items", items)
                            .build()
                            .toString()) // FIXME: toString shouldn't be required
                    .build();
        } catch (Exception e) {
            if (e instanceof AuthorizationException) {
                throw (RuntimeException) e;
            }

            log.errorf(e, "Error consuming messages");
            return Response.serverError()
                    .entity(Json.createObjectBuilder()
                            .add("code", 500)
                            .add("error_message", e.getMessage())
                            .build()
                            .toString()) // FIXME: toString shouldn't be required
                    .build();
        }
    }

    void addField(String fieldName, List<String> include, JsonObjectBuilder item, Supplier<Object> source) {
        if (include.isEmpty() || include.contains(fieldName)) {
            item.add(fieldName, String.valueOf(source.get()));
        }
    }

    @POST
    @Path("topics/{topicName}/records")
    @Blocking
    public Response produceRecord(@PathParam("topicName") String topicName, JsonObject recordJson, UriInfo requestUri) {
        try (Producer<String, String> producer = clientFactory.createProducer()) {
            String key = recordJson.containsKey("key") ? recordJson.getString("key") : null;
            List<Header> headers = recordJson.containsKey("headers") ? recordJson.getJsonObject("headers")
                .entrySet()
                .stream()
                .map(h -> new Header() {
                    @Override
                    public String key() {
                        return h.getKey();
                    }

                    @Override
                    public byte[] value() {
                        return ((JsonString) h.getValue()).getString().getBytes();
                    }
                })
                .collect(Collectors.toList()) : Collections.emptyList();

            var meta = producer.send(new ProducerRecord<>(topicName, recordJson.getInt("partition"), key, recordJson.getString("value"), headers)).get();
            return Response.ok()
                    .entity(Json.createObjectBuilder()
                            .add("partition", meta.partition())
                            .add("offset", meta.offset())
                            .add("timestamp", Instant.ofEpochMilli(meta.timestamp()).atZone(ZoneOffset.UTC).toString())
                            .add("key", recordJson.get("key"))
                            .add("value", recordJson.getString("value"))
                            .add("headers", recordJson.get("headers"))
                            .build()
                            .toString()) // FIXME: toString shouldn't be required
                    .build();
        } catch (Exception e) {
            return Response.serverError()
                    .entity(Json.createObjectBuilder()
                            .add("code", 500)
                            .add("error_message", e.getMessage())
                            .build()
                            .toString()) // FIXME: toString shouldn't be required
                    .build();
        }
    }

    @Override
    @Counted("list_groups_requests")
    @Timed("list_groups_request_time")
    public CompletionStage<Response> listGroups(String consumerGroupIdFilter, String topicFilter, Types.DeprecatedPageRequest pageParams, Types.ConsumerGroupSortParams sortParams, UriInfo requestUri) {
        final Pattern topicPattern = filterPattern(topicFilter);
        final Pattern groupPattern = filterPattern(consumerGroupIdFilter);

        return withAdminClient(client -> ConsumerGroupOperations.getGroupList(KafkaAdminClient.create(vertx, client), topicPattern, groupPattern, pageParams, sortParams))
                .thenApply(groupList -> Response.ok().entity(groupList).build());
    }

    @Override
    @Counted("get_group_requests")
    @Timed("describe_group_request_time")
    public CompletionStage<Response> describeGroup(String groupToDescribe, Optional<Integer> partitionFilter, String topicFilter, @BeanParam Types.ConsumerGroupDescriptionSortParams sortParams) {
        // FIXME: topicFilter exposed in API but not implemented
        sortParams.setDefaultsIfNecessary();

        return withAdminClient(client -> ConsumerGroupOperations.describeGroup(KafkaAdminClient.create(vertx, client), groupToDescribe, sortParams, partitionFilter.orElse(-1)))
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
    public CompletionStage<Response> describeAcls(Types.AclBindingFilterParams filterParams, Types.PageRequest pageParams, Types.AclBindingSortParams sortParams) {
        sortParams.setDefaultsIfNecessary();

        return withAdminClient(client -> aclOperations.getAcls(client, filterParams, pageParams, sortParams))
                .thenApply(aclList -> Response.ok().entity(aclList).build());
    }

    @Override
    @Counted("create_acls_requests")
    @Timed("create_acls_request_time")
    public CompletionStage<Response> createAcl(Types.AclBinding binding) {
        return withAdminClient(client -> aclOperations.createAcl(client, binding))
                .thenApply(nothing -> binding.buildUri(uriBuilder("describeAcls")))
                .thenApply(location -> Response.created(location).build());
    }

    @Override
    @Counted("delete_acls_requests")
    @Timed("delete_acls_request_time")
    public CompletionStage<Response> deleteAcls(@BeanParam Types.AclBindingFilterParams filterParams) {
        return withAdminClient(client -> aclOperations.deleteAcls(client, filterParams))
                .thenApply(aclList -> Response.ok().entity(aclList).build());
    }

    private UriBuilder uriBuilder(String methodName) {
        return UriBuilder.fromResource(RestOperations.class)
                .path(OperationsHandler.class, methodName);
    }

    private boolean numPartitionsValid(Types.TopicSettings settings, int maxPartitions) {
        int partitions = settings.getNumPartitions() != null ?
                settings.getNumPartitions() :
                    TopicOperations.DEFAULT_PARTITIONS;

        return partitions > 0 && partitions <= maxPartitions;
    }

    boolean numPartitionsLessThanEqualToMax(Types.TopicSettings settings, int maxPartitions) {
        if (settings.getNumPartitions() != null) {
            return settings.getNumPartitions() <= maxPartitions;
        } else {
            // user did not change the partitions
            return true;
        }
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
                        log.warnf("Exception closing Kafka AdminClient", e);
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
