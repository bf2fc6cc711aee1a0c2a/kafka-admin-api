package org.bf2.admin.kafka.admin;

import org.bf2.admin.kafka.admin.model.Types;
import org.bf2.admin.kafka.admin.handlers.CommonHandler;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.kafka.admin.ConsumerGroupListing;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ConsumerGroupOperations {
    protected static final Logger log = LogManager.getLogger(ConsumerGroupOperations.class);


    public static void getGroupList(KafkaAdminClient ac, Promise prom, Pattern pattern, int offset, final int limit) {
        Promise<List<ConsumerGroupListing>> listConsumerGroupsFuture = Promise.promise();
        Promise<Map<String, io.vertx.kafka.admin.ConsumerGroupDescription>> describeConsumerGroupsFuture = Promise.promise();

        ac.listConsumerGroups(listConsumerGroupsFuture);
        listConsumerGroupsFuture.future()
            .compose(groups -> {
                List<ConsumerGroupListing> filteredList = groups.stream().filter(groupId -> CommonHandler.byName(pattern, prom).test(groupId.getGroupId())).collect(Collectors.toList());

                List<Types.ConsumerGroup> mappedList = filteredList.stream().map(item -> {
                    Types.ConsumerGroup consumerGroup = new Types.ConsumerGroup();
                    consumerGroup.setGroupId(item.getGroupId());
                    return consumerGroup;
                }).collect(Collectors.toList());
                return Future.succeededFuture(mappedList);
            })
            .compose(list -> {
                ac.describeConsumerGroups(list.stream().map(l -> l.getGroupId()).collect(Collectors.toList()), describeConsumerGroupsFuture);
                return describeConsumerGroupsFuture.future();
            })
            .compose(descriptions -> {
                List<Types.ConsumerGroupDescription> list = getConsumerGroupsDescription(descriptions);
                list.sort(new CommonHandler.ConsumerGroupComparator());

                if (offset > list.size()) {
                    return Future.failedFuture(new InvalidRequestException("Offset (" + offset + ") cannot be greater than consumer group list size (" + list.size() + ")"));
                }
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

    public static void resetGroupOffset(KafkaAdminClient ac, List<String> groupsToDelete, Promise prom) {
        //TODO
        /*ac.deleteConsumerGroups(groupsToDelete, res -> {
            if (res.failed()) {
                prom.fail(res.cause());
            } else {
                prom.complete(groupsToDelete);
            }
            ac.close();
        });*/
    }

    public static void describeGroup(KafkaAdminClient ac, Promise prom, List<String> groupToDescribe) {
        Promise describeGroupPromise = Promise.promise();
        Promise listOffsetsPromise = Promise.promise();
        ac.describeConsumerGroups(groupToDescribe, describeGroupPromise);
        ac.listConsumerGroupOffsets(groupToDescribe.get(0), listOffsetsPromise);

        CompositeFuture.join(describeGroupPromise.future(), listOffsetsPromise.future())
                .onComplete(res -> {
                    if (res.failed()) {
                        prom.fail(res.cause());
                    } else {
                        Types.ConsumerGroupDescription groupDescription = getConsumerGroupsDescription(res.result().resultAt(0), res.result().resultAt(1)).get(0);
                        prom.complete(groupDescription);
                    }
                    ac.close();
                });
    }

    private static List<Types.ConsumerGroupDescription> getConsumerGroupsDescription(Map<String, io.vertx.kafka.admin.ConsumerGroupDescription> consumerGroupDescriptionMap,
                                                                                     Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap) {
        return consumerGroupDescriptionMap.entrySet().stream().map(group -> {
            Types.ConsumerGroupDescription grp = new Types.ConsumerGroupDescription();
            grp.setGroupId(group.getValue().getGroupId());
            grp.setState(group.getValue().getState().name());

            List<Types.ConsumerGroupConsumers> members = new ArrayList<>();
            group.getValue().getMembers().stream().forEach(mem -> {
                if (mem.getAssignment().getTopicPartitions().size() > 0) {
                    mem.getAssignment().getTopicPartitions().forEach(pa -> {
                        Types.ConsumerGroupConsumers member = new Types.ConsumerGroupConsumers();
                        member.setMemberId(mem.getConsumerId());
                        member.setTopic(pa.getTopic());
                        member.setPartition(pa.getPartition());
                        member.setGroupId(group.getValue().getGroupId());
                        long currentOffset = topicPartitionOffsetAndMetadataMap.get(pa).getOffset();
                        long lag = 0; // TODO!
                        member.setLag(lag);
                        member.setLogEndOffset(currentOffset + lag);
                        member.setOffset(topicPartitionOffsetAndMetadataMap.get(pa).getOffset());
                        members.add(member);
                    });
                } else {
                    Types.ConsumerGroupConsumers member = new Types.ConsumerGroupConsumers();
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
            grp.setConsumers(members);
            return grp;
        }).collect(Collectors.toList());
    }
}
