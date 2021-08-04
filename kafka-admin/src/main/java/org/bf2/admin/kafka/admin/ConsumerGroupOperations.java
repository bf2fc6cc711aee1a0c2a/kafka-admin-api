package org.bf2.admin.kafka.admin;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.kafka.admin.ConsumerGroupDescription;
import io.vertx.kafka.admin.ConsumerGroupListing;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.ListOffsetsResultInfo;
import io.vertx.kafka.admin.MemberDescription;
import io.vertx.kafka.admin.OffsetSpec;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.admin.handlers.CommonHandler;
import org.bf2.admin.kafka.admin.model.Types;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BinaryOperator;
import java.util.function.ToLongFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
public class ConsumerGroupOperations {
    protected static final Logger log = LogManager.getLogger(ConsumerGroupOperations.class);
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssz");

    public static void getGroupList(KafkaAdminClient ac, Promise prom, Pattern pattern, Types.PageRequest pageRequest, final String groupIdPrefix, Types.OrderByInput orderByInput) {
        Promise<List<ConsumerGroupListing>> listConsumerGroupsFuture = Promise.promise();

        ac.listConsumerGroups(listConsumerGroupsFuture);
        listConsumerGroupsFuture.future()
            .compose(list -> {
                boolean internalGroupsAllowed = System.getenv("KAFKA_ADMIN_INTERNAL_CONSUMER_GROUPS_ENABLED") == null
                        ? false : Boolean.valueOf(System.getenv("KAFKA_ADMIN_INTERNAL_CONSUMER_GROUPS_ENABLED"));

                List<String> groupIds = list.stream().map(group -> group.getGroupId())
                        .filter(groupId -> !internalGroupsAllowed ? !groupId.startsWith("strimzi") : true)
                        .filter(groupId -> groupId.startsWith(groupIdPrefix))
                        .collect(Collectors.toList());
                Promise<Map<String, ConsumerGroupDescription>> describeConsumerGroupsPromise = Promise.promise();
                ac.describeConsumerGroups(groupIds, describeConsumerGroupsPromise);
                return describeConsumerGroupsPromise.future();
            })
            .compose(descriptionMap -> {
                List<Future> futures = descriptionMap.entrySet().stream().map(entry -> {
                    Promise<Map<TopicPartition, OffsetAndMetadata>> listOffsetsPromise = Promise.promise();
                    ac.listConsumerGroupOffsets(entry.getKey(), listOffsetsPromise);
                    return ConsumerGroupInfo.future(entry.getKey(), Future.succeededFuture(descriptionMap), listOffsetsPromise.future());
                }).collect(Collectors.toList());
                return CompositeFuture.join(futures);
            })
            .compose(infos -> {
                List<ConsumerGroupInfo> consumerGroupInfos = infos.list();

                // Collect a distinct list of all TopicPartitions consumed by all the consumer
                // groups in the list, then create a map them to `OffsetSpec.LATEST`. This mapping
                // is just needed for the non-group-specific `ac.listOffsets` method, so that we
                // only invoke it once rather than once per group (with potentially a lot of
                // overlap).
                Map<TopicPartition, OffsetSpec> topicPartitionOffsetSpecs = consumerGroupInfos.stream()
                    .map(cgInfo -> getTopicPartitions(Collections.singletonMap(cgInfo.getGroupId(), cgInfo.getDescription())))
                    .flatMap(List::stream)
                    .distinct()
                    .filter(topicPartition -> topicPartition != null)
                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.LATEST));

                Promise<Map<TopicPartition, ListOffsetsResultInfo>> latestOffsetsPromise = Promise.promise();
                ac.listOffsets(topicPartitionOffsetSpecs, latestOffsetsPromise);

                return CompositeFuture.join(Future.succeededFuture(consumerGroupInfos), latestOffsetsPromise.future());
            })
            .compose(composite -> {
                List<ConsumerGroupInfo> consumerGroupInfos = composite.resultAt(0);
                Map<TopicPartition, ListOffsetsResultInfo> latestOffsets = composite.resultAt(1);

                Types.OrderByInput blankOrderBy = new Types.OrderByInput();
                List<Types.ConsumerGroupDescription> list = consumerGroupInfos.stream()
                    .map(e -> getConsumerGroupsDescription(pattern, blankOrderBy, -1, Collections.singletonMap(e.getGroupId(), e.getDescription()), e.getOffsets(), latestOffsets))
                    .flatMap(List::stream)
                    .filter(i -> i != null)
                    .collect(Collectors.toList());

                if (Types.SortDirectionEnum.DESC.equals(orderByInput.getOrder())) {
                    list.sort(new CommonHandler.ConsumerGroupComparator(orderByInput.getField()).reversed());
                } else {
                    list.sort(new CommonHandler.ConsumerGroupComparator(orderByInput.getField()));
                }

                Types.ConsumerGroupList response = new Types.ConsumerGroupList();
                List<Types.ConsumerGroupDescription> croppedList;
                if (pageRequest.isDeprecatedFormat()) {
                    if (pageRequest.getOffset() > list.size()) {
                        return Future.failedFuture(new InvalidRequestException("Offset (" + pageRequest.getOffset() + ") cannot be greater than consumer group list size (" + list.size() + ")"));
                    }
                    int tmpLimit = pageRequest.getLimit();
                    if (tmpLimit == 0) {
                        tmpLimit = list.size();
                    }
                    croppedList = list.subList(pageRequest.getOffset(), Math.min(pageRequest.getOffset() + tmpLimit, list.size()));
                    response.setLimit(pageRequest.getLimit());
                    response.setOffset(pageRequest.getOffset());
                    response.setCount(croppedList.size());
                } else {
                    if (list.size() > 0 && pageRequest.getSize() * (pageRequest.getPage() - 1) >= list.size()) {
                        return Future.failedFuture(new InvalidRequestException("Requested pagination incorrect. Beginning of list greater than full list size (" + list.size() + ")"));
                    }
                    croppedList = list.subList((pageRequest.getPage() - 1) * pageRequest.getSize(), Math.min(pageRequest.getSize() * pageRequest.getPage(), list.size()));
                    response.setSize(pageRequest.getSize());
                    response.setPage(pageRequest.getPage());
                    response.setTotal(list.size());
                }

                response.setItems(croppedList);

                return Future.succeededFuture(response);
            })
            .onComplete(finalRes -> {
                if (finalRes.failed()) {
                    prom.fail(finalRes.cause());
                } else {
                    prom.complete(finalRes.result());
                }
                ac.close();
            });
    }

    public static void deleteGroup(KafkaAdminClient ac, List<String> groupsToDelete, Promise prom) {
        ac.deleteConsumerGroups(groupsToDelete, res -> {
            if (res.failed()) {
                prom.fail(res.cause());
            } else {
                prom.complete(groupsToDelete);
            }
            ac.close();
        });
    }

    @SuppressWarnings({"checkstyle:JavaNCSS", "checkstyle:MethodLength"})
    public static void resetGroupOffset(KafkaAdminClient ac, Types.ConsumerGroupOffsetResetParameters parameters, Promise<Types.PagedResponse<Types.TopicPartitionResetResult>> prom) {

        if (!"latest".equals(parameters.getOffset()) && !"earliest".equals(parameters.getOffset()) && parameters.getValue() == null) {
            throw new InvalidRequestException("Value has to be set when " + parameters.getOffset() + " offset is used.");
        }

        Set<TopicPartition> topicPartitionsToReset = new HashSet<>();

        @SuppressWarnings("rawtypes") // CompositeFuture#join requires raw type
        List<Future> promises = new ArrayList<>();

        if (parameters.getTopics() == null || parameters.getTopics().isEmpty()) {
            // reset everything
            Promise promise = Promise.promise();
            promises.add(promise.future());
            ac.listConsumerGroupOffsets(parameters.getGroupId())
                    .compose(consumerGroupOffsets -> {
                        consumerGroupOffsets.entrySet().forEach(offset -> {
                            topicPartitionsToReset.add(offset.getKey());
                        });
                        return Future.succeededFuture(topicPartitionsToReset);
                    }).onComplete(topicPartitions -> {
                        promise.complete();
                    });
        } else {
            parameters.getTopics().forEach(paramPartition -> {
                Promise promise = Promise.promise();
                promises.add(promise.future());
                if (paramPartition.getPartitions() == null || paramPartition.getPartitions().isEmpty()) {
                    ac.describeTopics(Collections.singletonList(paramPartition.getTopic())).compose(topicsDesc -> {
                        topicsDesc.entrySet().forEach(topicEntry -> {
                            topicsDesc.get(topicEntry.getKey()).getPartitions().forEach(partition -> {
                                topicPartitionsToReset.add(new TopicPartition(topicEntry.getKey(), partition.getPartition()));
                            });
                        });
                        promise.complete();
                        return Future.succeededFuture(topicPartitionsToReset);
                    });
                } else {
                    paramPartition.getPartitions().forEach(numPartition -> {
                        topicPartitionsToReset.add(new TopicPartition(paramPartition.getTopic(), numPartition));
                    });
                    promise.complete();
                }
            });
        }

        // get the set of partitions we want to reset
        CompositeFuture.join(promises).compose(nothing -> {
            return getTopicPartitions(ac, parameters.getGroupId(), topicPartitionsToReset);
        }).map(topicPartitions -> {
            topicPartitionsToReset.forEach(topicPartition ->
                validatePartitionResettable(topicPartitions, topicPartition));
            return null;
        }).compose(nothing -> {
            Map<TopicPartition, OffsetSpec> partitionsToFetchOffset = new HashMap<>();
            topicPartitionsToReset.forEach(topicPartition -> {
                OffsetSpec offsetSpec;
                // absolute - just for the warning that set offset could be higher than latest
                if ("latest".equals(parameters.getOffset())) {
                    offsetSpec = OffsetSpec.LATEST;
                } else if ("earliest".equals(parameters.getOffset())) {
                    offsetSpec = OffsetSpec.EARLIEST;
                } else if ("timestamp".equals(parameters.getOffset())) {
                    try {
                        offsetSpec = OffsetSpec.TIMESTAMP(ZonedDateTime.parse(parameters.getValue(), DATE_TIME_FORMATTER).toInstant().toEpochMilli());
                    } catch (DateTimeParseException e) {
                        throw new InvalidRequestException("Timestamp must be in format 'yyyy-MM-dd'T'HH:mm:ssz'" + e.getMessage());
                    }
                } else if ("absolute".equals(parameters.getOffset())) {
                    // we are checking whether offset is not negative (set behind latest)
                    offsetSpec = OffsetSpec.LATEST;
                } else {
                    throw new InvalidRequestException("Offset can be 'absolute', 'latest', 'earliest' or 'timestamp' only");
                }
                partitionsToFetchOffset.put(topicPartition, offsetSpec);
            });
            return Future.succeededFuture(partitionsToFetchOffset);
        }).compose(partitionsToFetchOffset -> {
            Promise<Map<TopicPartition, ListOffsetsResultInfo>> promise = Promise.promise();
            ac.listOffsets(partitionsToFetchOffset, partitionsOffsets -> {
                if (partitionsOffsets.failed()) {
                    promise.fail(partitionsOffsets.cause());
                    return;
                }
                if ("absolute".equals(parameters.getOffset())) {
                    // numeric offset provided; check whether x > latest
                    promise.complete(partitionsOffsets.result().entrySet().stream().collect(Collectors.toMap(
                        entry -> entry.getKey(),
                        entry -> {
                            if (entry.getValue().getOffset() < Long.parseLong(parameters.getValue())) {
                                log.warn("Selected offset {} is larger than latest {}", parameters.getValue(), entry.getValue().getOffset());
                            }
                            return new ListOffsetsResultInfo(Long.parseLong(parameters.getValue()), entry.getValue().getTimestamp(), entry.getValue().getLeaderEpoch());
                        })));
                } else {
                    Map<TopicPartition, ListOffsetsResultInfo> kokot = partitionsOffsets.result().entrySet().stream().collect(Collectors.toMap(
                        entry -> entry.getKey(),
                        entry -> new ListOffsetsResultInfo(partitionsOffsets.result().get(entry.getKey()).getOffset(), entry.getValue().getTimestamp(), entry.getValue().getLeaderEpoch())));
                    promise.complete(kokot);
                }
            });
            return promise.future();
        }).compose(newOffsets -> {
            // assemble new offsets object
            Promise<Map<TopicPartition, OffsetAndMetadata>> promise = Promise.promise();
            ac.listConsumerGroupOffsets(parameters.getGroupId(), list -> {
                if (list.failed()) {
                    promise.fail(list.cause());
                    return;
                }
                if (list.result().isEmpty()) {
                    promise.fail(new InvalidRequestException("Consumer Group " + parameters.getGroupId() + " does not consume any topics/partitions"));
                    return;
                }
                promise.complete(newOffsets.entrySet().stream().collect(Collectors.toMap(
                    entry -> entry.getKey(),
                    entry -> new OffsetAndMetadata(newOffsets.get(entry.getKey()).getOffset(), list.result().get(entry.getKey()) == null ? null : list.result().get(entry.getKey()).getMetadata()))));
            });
            return promise.future();
        }).compose(newOffsets -> {
            Promise<Void> promise = Promise.promise();
            ac.alterConsumerGroupOffsets(parameters.getGroupId(), newOffsets, res -> {
                if (res.failed()) {
                    promise.fail(res.cause());
                    return;
                }
                promise.complete();
                log.info("resetting offsets");
            });
            return promise.future();
        }).compose(i -> {
            Promise<Types.PagedResponse<Types.TopicPartitionResetResult>> promise = Promise.promise();

            ac.listConsumerGroupOffsets(parameters.getGroupId(), res -> {
                if (res.failed()) {
                    promise.fail(res.cause());
                    return;
                }

                var result = res.result()
                        .entrySet()
                        .stream()
                        .map(entry -> {
                            Types.TopicPartitionResetResult reset = new Types.TopicPartitionResetResult();
                            reset.setTopic(entry.getKey().getTopic());
                            reset.setPartition(entry.getKey().getPartition());
                            reset.setOffset(entry.getValue().getOffset());
                            return reset;
                        })
                        .collect(Collectors.toList());

                Types.PagedResponse.forItems(result)
                    .onSuccess(promise::complete)
                    .onFailure(promise::fail);
            });
            return promise.future();
        }).onComplete(res -> {
            if (res.succeeded()) {
                prom.complete(res.result());
            } else {
                prom.fail(res.cause());
            }
            ac.close();
        });
    }

    static Future<Map<TopicPartition, List<MemberDescription>>> getTopicPartitions(KafkaAdminClient ac, String groupId, Set<TopicPartition> topicPartitionsToReset) {
        Map<TopicPartition, List<MemberDescription>> topicPartitions = new ConcurrentHashMap<>();

        List<String> requestedTopics = topicPartitionsToReset
                .stream()
                .map(TopicPartition::getTopic)
                .collect(Collectors.toList());

        Promise<Void> topicDescribe = Promise.promise();

        if (requestedTopics.isEmpty()) {
            topicDescribe.complete();
        } else {
            ac.describeTopics(requestedTopics)
                .onSuccess(describedTopics -> {
                    describedTopics.entrySet()
                        .stream()
                        .flatMap(entry ->
                            entry.getValue()
                                .getPartitions()
                                .stream()
                                .map(part -> new TopicPartition(entry.getKey(), part.getPartition())))
                        .forEach(topicPartition ->
                            topicPartitions.compute(topicPartition, (key, value) -> addTopicPartition(value, null)));

                    topicDescribe.complete();
                })
                .onFailure(error -> {
                    if (error instanceof UnknownTopicOrPartitionException) {
                        topicDescribe.fail(new IllegalArgumentException("Request contained an unknown topic"));
                    } else {
                        topicDescribe.fail(error);
                    }
                });
        }

        Promise<Void> groupDescribe = Promise.promise();

        ac.describeConsumerGroups(List.of(groupId))
            .onSuccess(descriptions -> {
                /*
                 * Find all topic partitions in the group that are actively
                 * being consumed by a client.
                 */
                descriptions.values()
                    .stream()
                    .flatMap(description -> description.getMembers().stream())
                    .filter(member -> member.getClientId() != null)
                    .flatMap(member ->
                        member.getAssignment()
                             .getTopicPartitions()
                             .stream()
                             .map(part -> Map.entry(part, member)))
                    .forEach(entry -> {
                        MemberDescription member = entry.getValue();
                        topicPartitions.compute(entry.getKey(), (key, value) -> addTopicPartition(value, member));
                    });

                groupDescribe.complete();
            })
            .onFailure(groupDescribe::fail);

        return CompositeFuture.all(topicDescribe.future(), groupDescribe.future())
                .map(topicPartitions);
    }

    static List<MemberDescription> addTopicPartition(List<MemberDescription> members, MemberDescription newMember) {
        if (members == null) {
            members = new ArrayList<>();
        }

        if (newMember != null) {
            members.add(newMember);
        }

        return members;
    }

    static void validatePartitionResettable(Map<TopicPartition, List<MemberDescription>> topicClients, TopicPartition topicPartition) {
        if (!topicClients.containsKey(topicPartition)) {
            throw new IllegalArgumentException(String.format("Topic %s, partition %d is not valid",
                                                          topicPartition.getTopic(),
                                                          topicPartition.getPartition()));
        } else if (!topicClients.get(topicPartition).isEmpty()) {
            /*
             * Reject the request if any of the topic partitions
             * being reset is also being consumed by a client.
             */
            String clients = topicClients.get(topicPartition)
                .stream()
                .map(member -> String.format("{ memberId: %s, clientId: %s }", member.getConsumerId(), member.getClientId()))
                .collect(Collectors.joining(", "));

            throw new IllegalArgumentException(String.format("Topic %s, partition %d has connected clients: [%s]",
                                                             topicPartition.getTopic(),
                                                             topicPartition.getPartition(),
                                                             clients));
        }
    }

    public static void describeGroup(KafkaAdminClient ac, Promise prom, List<String> groupToDescribe, Types.OrderByInput orderBy, int partitionFilter) {
        Promise<Map<String, ConsumerGroupDescription>> describeGroupPromise = Promise.promise();
        ac.describeConsumerGroups(groupToDescribe, describeGroupPromise);

        describeGroupPromise.future()
            .compose(descriptions -> {
                Promise<Map<TopicPartition, OffsetAndMetadata>> groupOffsetsPromise = Promise.promise();
                ac.listConsumerGroupOffsets(groupToDescribe.get(0), groupOffsetsPromise);

                Promise<Map<TopicPartition, ListOffsetsResultInfo>> listOffsetsEndPromise = Promise.promise();
                List<TopicPartition> topicPartitions = getTopicPartitions(descriptions);
                ac.listOffsets(topicPartitions.stream().collect(Collectors.toMap(k -> k, k -> OffsetSpec.LATEST)), listOffsetsEndPromise);

                return CompositeFuture.join(Future.succeededFuture(descriptions),
                                            groupOffsetsPromise.future(),
                                            listOffsetsEndPromise.future());
            })
            .onComplete(res -> {
                if (res.failed()) {
                    prom.fail(res.cause());
                } else {
                    Map<String, ConsumerGroupDescription> cgDescriptions = res.result().resultAt(0);
                    Map<TopicPartition, OffsetAndMetadata> cgOffsets = res.result().resultAt(1);
                    Map<TopicPartition, ListOffsetsResultInfo> endOffsets = res.result().resultAt(2);

                    Types.ConsumerGroupDescription groupDescription = getConsumerGroupsDescription(Pattern.compile(".*"), orderBy, partitionFilter, cgDescriptions, cgOffsets, endOffsets).get(0);
                    if ("dead".equalsIgnoreCase(groupDescription.getState())) {
                        prom.fail(new GroupIdNotFoundException("Group " + groupDescription.getGroupId() + " does not exist"));
                    } else {
                        prom.complete(groupDescription);
                    }
                }
                ac.close();
            });
    }

    private static List<Types.ConsumerGroupDescription> getConsumerGroupsDescription(Pattern pattern, Types.OrderByInput orderBy, int partitionFilter, Map<String, io.vertx.kafka.admin.ConsumerGroupDescription> consumerGroupDescriptionMap,
                                                                                     Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap,
                                                                                     Map<TopicPartition, ListOffsetsResultInfo> topicPartitionListOffsetsResultInfoMap) {

        List<TopicPartition> assignedTopicPartitions = topicPartitionOffsetAndMetadataMap.entrySet().stream().map(e -> e.getKey()).filter(topicPartition -> pattern.matcher(topicPartition.getTopic()).matches()).collect(Collectors.toList());
        return consumerGroupDescriptionMap.entrySet().stream().map(group -> {
            Types.ConsumerGroupDescription grp = new Types.ConsumerGroupDescription();

            if (group.getValue().getState().name().equalsIgnoreCase("empty") && !pattern.pattern().equals(".*")) {
                // there are no topics to filter by so the consumer group is not listed
                return null;
            }
            Set<Types.Consumer> members = new HashSet<>();

            if (group.getValue().getMembers().size() == 0) {
                assignedTopicPartitions.forEach(pa -> {
                    Types.Consumer member = getConsumer(topicPartitionOffsetAndMetadataMap, topicPartitionListOffsetsResultInfoMap, group, pa);
                    members.add(member);
                });

            } else {
                assignedTopicPartitions.forEach(pa -> {
                    group.getValue().getMembers().stream().forEach(mem -> {
                        if (mem.getAssignment().getTopicPartitions().size() > 0) {
                            Types.Consumer member = getConsumer(topicPartitionOffsetAndMetadataMap, topicPartitionListOffsetsResultInfoMap, group, pa);
                            if (memberMatchesPartitionFilter(member, partitionFilter)) {
                                if (mem.getAssignment().getTopicPartitions().contains(pa)) {
                                    member.setMemberId(mem.getConsumerId());
                                } else {
                                    // unassigned partition
                                    member.setMemberId(null);
                                }

                                if (members.contains(member)) {
                                    // some member does not consume the partition, so it was flagged as unconsumed
                                    // another member does consume the partition, so we override the member in result
                                    if (member.getMemberId() != null) {
                                        members.remove(member);
                                        members.add(member);
                                    }
                                } else {
                                    members.add(member);
                                }
                            }
                        } else {
                            // more consumers than topic partitions - consumer is in the group but is not consuming
                            Types.Consumer member = new Types.Consumer();
                            member.setMemberId(mem.getConsumerId());
                            member.setTopic(null);
                            member.setPartition(-1);
                            member.setGroupId(group.getValue().getGroupId());
                            member.setLogEndOffset(0);
                            member.setLag(0);
                            member.setOffset(0);
                            if (memberMatchesPartitionFilter(member, partitionFilter)) {
                                members.add(member);
                            }
                        }
                    });
                });
            }

            if (!pattern.pattern().equals(".*") && members.size() == 0) {
                return null;
            }
            grp.setGroupId(group.getValue().getGroupId());
            grp.setState(group.getValue().getState().name());
            List<Types.Consumer> sortedList;

            ToLongFunction<Types.Consumer> fun;
            if ("lag".equalsIgnoreCase(orderBy.getField())) {
                fun = Types.Consumer::getLag;
            } else if ("endOffset".equalsIgnoreCase(orderBy.getField())) {
                fun = Types.Consumer::getLogEndOffset;
            } else if ("offset".equalsIgnoreCase(orderBy.getField())) {
                fun = Types.Consumer::getOffset;
            } else {
                // partitions and unknown keys
                fun = Types.Consumer::getPartition;
            }

            if (Types.SortDirectionEnum.DESC.equals(orderBy.getOrder())) {
                sortedList = members.stream().sorted(Comparator.comparingLong(fun).reversed()).collect(Collectors.toList());
            } else {
                sortedList = members.stream().sorted(Comparator.comparingLong(fun)).collect(Collectors.toList());
            }

            grp.setConsumers(sortedList);
            return grp;

        }).collect(Collectors.toList());

    }

    private static boolean memberMatchesPartitionFilter(Types.Consumer member, int partitionFilter) {
        if (partitionFilter < 0) {
            // filter deactivated
            return true;
        } else {
            return member.getPartition() == partitionFilter;
        }
    }

    private static Types.Consumer getConsumer(Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap, Map<TopicPartition, ListOffsetsResultInfo> topicPartitionListOffsetsResultInfoMap, Map.Entry<String, ConsumerGroupDescription> group, TopicPartition pa) {
        Types.Consumer member = new Types.Consumer();
        member.setTopic(pa.getTopic());
        member.setPartition(pa.getPartition());
        member.setGroupId(group.getValue().getGroupId());
        long currentOffset = topicPartitionOffsetAndMetadataMap.get(pa) == null ? 0 : topicPartitionOffsetAndMetadataMap.get(pa).getOffset();
        long endOffset = topicPartitionListOffsetsResultInfoMap.get(pa) == null ? 0 : topicPartitionListOffsetsResultInfoMap.get(pa).getOffset();
        long lag = endOffset - currentOffset;
        member.setLag(lag);
        member.setLogEndOffset(endOffset);
        member.setOffset(currentOffset);
        return member;
    }

    private static List<TopicPartition> getTopicPartitions(Map<String, io.vertx.kafka.admin.ConsumerGroupDescription> consumerGroupDescriptionMap) {
        final BinaryOperator<Set<TopicPartition>> setAccum = (result, next) -> {
            result.addAll(next);
            return result;
        };

        return new ArrayList<>(consumerGroupDescriptionMap.values().stream()
            .map(consumerGroupDescription -> consumerGroupDescription.getMembers().stream()
                .map(mem -> mem.getAssignment().getTopicPartitions())
                .reduce(new HashSet<TopicPartition>(), setAccum)
        ).reduce(new HashSet<TopicPartition>(), setAccum));
    }
}
