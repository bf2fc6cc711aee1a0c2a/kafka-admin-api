package org.bf2.admin.kafka.admin;

import io.vertx.core.CompositeFuture;
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

public class ConsumerGroupOperations {
    protected static final Logger log = LogManager.getLogger(ConsumerGroupOperations.class);


    public static void getGroupList(KafkaAdminClient ac, Promise prom, Pattern pattern, int offset, final int limit) {
        Promise<List<ConsumerGroupListing>> listConsumerGroupsFuture = Promise.promise();

        ac.listConsumerGroups(listConsumerGroupsFuture);
        listConsumerGroupsFuture.future()
            .compose(groups -> {
                List<Types.ConsumerGroup> mappedList = groups.stream().map(item -> {
                    Types.ConsumerGroup consumerGroup = new Types.ConsumerGroup();
                    consumerGroup.setGroupId(item.getGroupId());
                    return consumerGroup;
                }).collect(Collectors.toList());
                return Future.succeededFuture(mappedList);
            })
            .compose(list -> {
                List promises = new ArrayList();
                list.forEach(item -> {
                    Promise<Map<String, ConsumerGroupDescription>> describeConsumerGroupsFuture = Promise.promise();
                    Promise<Map<TopicPartition, OffsetAndMetadata>> listOffsetsPromise = Promise.promise();

                    ac.describeConsumerGroups(Collections.singletonList(item.getGroupId()), describeConsumerGroupsFuture);
                    ac.listConsumerGroupOffsets(item.getGroupId(), listOffsetsPromise);

                    promises.add(describeConsumerGroupsFuture);
                    promises.add(listOffsetsPromise);
                });
                return CompositeFuture.join(promises);
            })
            .compose(descriptions -> {
                Map<TopicPartition, OffsetSpec> topicPartitionOffsetSpecs = new HashMap<>();
                Map<String, ConsumerGroupDescription> cgDescriptions = new HashMap<>();
                Map<String, Map<TopicPartition, OffsetAndMetadata>> cgTopicPartitionOffsets = new HashMap<>();
                for (int i = 0; i < descriptions.result().size(); i += 2) {
                    Map<String, ConsumerGroupDescription> cgDescription = descriptions.resultAt(i);
                    List<TopicPartition> topicPartitions = getTopicPartitions(cgDescription);
                    topicPartitions.forEach(topicPartition -> topicPartitionOffsetSpecs.putIfAbsent(topicPartition, OffsetSpec.LATEST));

                    cgDescriptions.putAll(cgDescription);
                    cgTopicPartitionOffsets.put(cgDescription.keySet().iterator().next(), descriptions.resultAt(i + 1));
                }

                Map<TopicPartition, ListOffsetsResultInfo> topicPartitionListOffsetsResultInfo = new HashMap<>();
                Promise<Map<TopicPartition, ListOffsetsResultInfo>> listOffsetsEndPromise = Promise.promise();
                ac.listOffsets(topicPartitionOffsetSpecs, listOffsetsEndPromise);

                return CompositeFuture.join(Future.succeededFuture(cgDescriptions),
                                            Future.succeededFuture(cgTopicPartitionOffsets),
                                            listOffsetsEndPromise.future());
            })
            .compose(composite -> {
                Map<String, ConsumerGroupDescription> cgDescriptions = composite.resultAt(0);
                Map<String, Map<TopicPartition, OffsetAndMetadata>> cgTopicPartitionOffsets = composite.resultAt(1);
                Map<TopicPartition, ListOffsetsResultInfo> endOffsets = composite.resultAt(2);

                List<Types.ConsumerGroupDescription> list = cgDescriptions.entrySet().stream()
                    .map(e -> getConsumerGroupsDescription(pattern, Collections.singletonMap(e.getKey(), e.getValue()), cgTopicPartitionOffsets.get(e.getKey()), endOffsets).get(0))
                    .filter(i -> i != null)
                    .collect(Collectors.toList());

                if (offset > list.size()) {
                    return Future.failedFuture(new InvalidRequestException("Offset (" + offset + ") cannot be greater than consumer group list size (" + list.size() + ")"));
                }

                list.sort(new CommonHandler.ConsumerGroupComparator());

                int tmpLimit = limit;
                if (tmpLimit == 0) {
                    tmpLimit = list.size();
                }

                List<Types.ConsumerGroupDescription> croppedList = list.subList(offset, Math.min(offset + tmpLimit, list.size()));
                return Future.succeededFuture(croppedList);
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

    public static void resetGroupOffset(KafkaAdminClient ac, String groupToReset, Promise prom) {
        ac.listConsumerGroupOffsets(groupToReset, list -> {
            Map<TopicPartition, OffsetAndMetadata> newOffsets = new HashMap<>();
            list.result().entrySet().forEach(entry -> {
                // TODO figure out what offset we should reset to
                newOffsets.put(entry.getKey(), new OffsetAndMetadata(0, entry.getValue().getMetadata()));
            });
            ac.alterConsumerGroupOffsets(groupToReset, newOffsets, i -> {
                if (i.failed()) {
                    prom.fail(i.cause());
                    ac.close();
                    return;
                }
                Promise describeGroupPromise = Promise.promise();
                Promise listOffsetsPromise = Promise.promise();
                ac.describeConsumerGroups(Collections.singletonList(groupToReset), describeGroupPromise);
                ac.listConsumerGroupOffsets(groupToReset, listOffsetsPromise);

                CompositeFuture.join(describeGroupPromise.future(), listOffsetsPromise.future())
                    .onComplete(res -> {
                        if (res.failed()) {
                            prom.fail(res.cause());
                        } else {
                            Types.ConsumerGroupDescription groupDescription = getConsumerGroupsDescription(Pattern.compile(".*"), res.result().resultAt(0), res.result().resultAt(1), null).get(0);
                            if ("dead".equalsIgnoreCase(groupDescription.getState())) {
                                prom.fail(new GroupIdNotFoundException("Group " + groupDescription.getGroupId() + " does not exist"));
                                ac.close();
                                return;
                            }
                            prom.complete(groupDescription);
                        }
                        ac.close();
                    });
            });
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
                    }
                    prom.complete(groupDescription);
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
