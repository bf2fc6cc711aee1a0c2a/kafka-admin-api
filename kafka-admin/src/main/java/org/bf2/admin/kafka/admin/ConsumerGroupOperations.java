package org.bf2.admin.kafka.admin;

import io.vertx.core.CompositeFuture;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.bf2.admin.kafka.admin.model.Types;
import org.bf2.admin.kafka.admin.handlers.CommonHandler;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.kafka.admin.ConsumerGroupDescription;
import io.vertx.kafka.admin.ConsumerGroupListing;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.ListOffsetsResultInfo;
import io.vertx.kafka.admin.OffsetSpec;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
public class ConsumerGroupOperations {
    protected static final Logger log = LogManager.getLogger(ConsumerGroupOperations.class);


    public static void getGroupList(KafkaAdminClient ac, Promise prom, Pattern pattern, int offset, final int limit, final String groupIdPrefix, Types.OrderByInput orderByInput) {
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

                List<Types.ConsumerGroupDescription> list = consumerGroupInfos.stream()
                    .map(e -> getConsumerGroupsDescription(pattern, Collections.singletonMap(e.getGroupId(), e.getDescription()), e.getOffsets(), latestOffsets))
                    .flatMap(List::stream)
                    .filter(i -> i != null)
                    .collect(Collectors.toList());

                if (offset > list.size()) {
                    return Future.failedFuture(new InvalidRequestException("Offset (" + offset + ") cannot be greater than consumer group list size (" + list.size() + ")"));
                }

                if (Types.SortDirectionEnum.DESC.equals(orderByInput.getOrder())) {
                    list.sort(new CommonHandler.ConsumerGroupComparator(orderByInput.getField()).reversed());
                } else {
                    list.sort(new CommonHandler.ConsumerGroupComparator(orderByInput.getField()));
                }

                int tmpLimit = limit;
                if (tmpLimit == 0) {
                    tmpLimit = list.size();
                }

                List<Types.ConsumerGroupDescription> croppedList = list.subList(offset, Math.min(offset + tmpLimit, list.size()));

                Types.ConsumerGroupList response = new Types.ConsumerGroupList();
                response.setItems(croppedList);
                response.setCount(croppedList.size());
                response.setLimit(tmpLimit);
                response.setOffset(offset);

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

    @SuppressWarnings({"checkstyle:JavaNCSS"})
    public static void resetGroupOffset(KafkaAdminClient ac, Types.ConsumerGroupOffsetResetParameters parameters, Promise prom) {
        if (parameters.getTimestamp() != null && !"timestamp".equals(parameters.getOffset())) {
            log.error("if the timestamp is used, offset should se set to timestamp");
            prom.fail(new InvalidConfigurationException("if the timestamp is used, offset should se set to timestamp"));
            return;
        }
        if ("timestamp".equals(parameters.getOffset()) && parameters.getTimestamp() == null) {
            log.error("Timestamp value needs to be configured");
            prom.fail(new InvalidConfigurationException("Timestamp value needs to be configured"));
            return;
        }

        Set<TopicPartition> topicPartitionsToReset = new HashSet<>();
        Promise partitionsToResetPromise = Promise.promise();
        ArrayList<Future> promises = new ArrayList<>();
        if (parameters.getPartitions() == null) {
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
                        partitionsToResetPromise.complete(topicPartitions);
                    });
        } else {
            parameters.getPartitions().forEach(paramPartition -> {
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
        CompositeFuture.join(promises).compose(i -> {
            if (i.failed()) {
                return Future.failedFuture(i.cause());
            } else {
                return Future.succeededFuture();
            }
        }).compose(i -> {
            Map<TopicPartition, OffsetSpec> partitionsToFetchOffset = new HashMap<>();
            topicPartitionsToReset.forEach(topicPartition -> {
                OffsetSpec offsetSpec;
                if (parameters.getOffset().equals("latest")) {
                    offsetSpec = OffsetSpec.LATEST;
                } else if (parameters.getOffset().equals("earliest")) {
                    offsetSpec = OffsetSpec.EARLIEST;
                } else if (parameters.getOffset().equals("timestamp")) {
                    offsetSpec = OffsetSpec.TIMESTAMP(parameters.getTimestamp());
                } else {
                    // just for the warning that set offset could be higher than latest
                    offsetSpec = OffsetSpec.LATEST;
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
                if (!"latest".equals(parameters.getOffset()) && !"earliest".equals(parameters.getOffset()) && !"timestamp".equals(parameters.getOffset())) {
                    // numeric offset provided; check whether x > latest
                    promise.complete(partitionsOffsets.result().entrySet().stream().collect(Collectors.toMap(
                        entry -> entry.getKey(),
                        entry -> {
                            if (entry.getValue().getOffset() < Long.parseLong(parameters.getOffset())) {
                                log.warn("Selected offset {} is larger than latest {}", parameters.getOffset(), entry.getValue().getOffset());
                            }
                            return new ListOffsetsResultInfo(Long.parseLong(parameters.getOffset()), entry.getValue().getTimestamp(), entry.getValue().getLeaderEpoch());
                        })));
                } else {
                    promise.complete(partitionsOffsets.result().entrySet().stream().collect(Collectors.toMap(
                        entry -> entry.getKey(),
                        entry ->  new ListOffsetsResultInfo(partitionsOffsets.result().get(entry.getKey()).getOffset(), entry.getValue().getTimestamp(), entry.getValue().getLeaderEpoch()))));
                }
            });
            return promise.future();
        }).compose(newOffsets -> {
            // assembly new offsets object
            Promise<Map<TopicPartition, OffsetAndMetadata>> promise = Promise.promise();
            ac.listConsumerGroupOffsets(parameters.getGroupId(), list -> {
                if (list.failed()) {
                    promise.fail(list.cause());
                    return;
                }
                promise.complete(newOffsets.entrySet().stream().collect(Collectors.toMap(
                    entry -> entry.getKey(),
                    entry -> new OffsetAndMetadata(newOffsets.get(entry.getKey()).getOffset(), list.result().get(entry.getKey()).getMetadata()))));
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
            Promise<List<Types.TopicPartitionResetResult>> promise = Promise.promise();
            List<Types.TopicPartitionResetResult> result = new ArrayList<>();
            ac.listConsumerGroupOffsets(parameters.getGroupId(), res -> {
                if (res.failed()) {
                    promise.fail(res.cause());
                    return;
                }
                res.result().forEach((tp, offsetAndMetadata) -> {
                    Types.TopicPartitionResetResult reset = new Types.TopicPartitionResetResult();
                    reset.setTopic(tp.getTopic());
                    reset.setPartition(tp.getPartition());
                    reset.setOffset(offsetAndMetadata.getOffset());
                    result.add(reset);
                });
                promise.complete(result);
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

    public static void describeGroup(KafkaAdminClient ac, Promise prom, List<String> groupToDescribe) {
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

                    Types.ConsumerGroupDescription groupDescription = getConsumerGroupsDescription(Pattern.compile(".*"), cgDescriptions, cgOffsets, endOffsets).get(0);
                    if ("dead".equalsIgnoreCase(groupDescription.getState())) {
                        prom.fail(new GroupIdNotFoundException("Group " + groupDescription.getGroupId() + " does not exist"));
                    } else {
                        prom.complete(groupDescription);
                    }
                }
                ac.close();
            });
    }

    private static List<Types.ConsumerGroupDescription> getConsumerGroupsDescription(Pattern pattern, Map<String, io.vertx.kafka.admin.ConsumerGroupDescription> consumerGroupDescriptionMap,
                                                                                     Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap,
                                                                                     Map<TopicPartition, ListOffsetsResultInfo> topicPartitionListOffsetsResultInfoMap) {
        return consumerGroupDescriptionMap.entrySet().stream().map(group -> {
            Types.ConsumerGroupDescription grp = new Types.ConsumerGroupDescription();

            if (group.getValue().getState().name().equalsIgnoreCase("empty") && !pattern.pattern().equals(".*")) {
                // there are no topics to filter by so the consumer group is not listed
                return null;
            }
            grp.setGroupId(group.getValue().getGroupId());
            grp.setState(group.getValue().getState().name());

            List<Types.Consumer> members = new ArrayList<>();
            group.getValue().getMembers().stream().forEach(mem -> {
                if (mem.getAssignment().getTopicPartitions().size() > 0) {
                    mem.getAssignment().getTopicPartitions().forEach(pa -> {
                        Types.Consumer member = new Types.Consumer();
                        member.setMemberId(mem.getConsumerId());
                        member.setTopic(pa.getTopic());
                        member.setPartition(pa.getPartition());
                        member.setGroupId(group.getValue().getGroupId());
                        long currentOffset = topicPartitionOffsetAndMetadataMap.get(pa) == null ? 0 : topicPartitionOffsetAndMetadataMap.get(pa).getOffset();
                        long endOffset = topicPartitionListOffsetsResultInfoMap.get(pa) == null ? 0 : topicPartitionListOffsetsResultInfoMap.get(pa).getOffset();
                        long lag = endOffset - currentOffset;
                        member.setLag(lag);
                        member.setLogEndOffset(endOffset);
                        member.setOffset(currentOffset);
                        if (pattern.matcher(pa.getTopic()).matches()) {
                            log.debug("Topic matches desired pattern");
                            members.add(member);
                        }
                    });
                } else {
                    Types.Consumer member = new Types.Consumer();
                    member.setMemberId(mem.getConsumerId());
                    member.setTopic(null);
                    member.setPartition(-1);
                    member.setGroupId(group.getValue().getGroupId());
                    member.setLogEndOffset(0);
                    member.setLag(0);
                    member.setOffset(0);
                    members.add(member);
                }
            });

            if (!pattern.pattern().equals(".*") && members.size() == 0) {
                return null;
            }
            grp.setConsumers(members);
            return grp;
        }).collect(Collectors.toList());
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
