package org.bf2.admin.kafka.admin;

import java.util.Map;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.kafka.admin.ConsumerGroupDescription;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;

class ConsumerGroupInfo {
    String groupId;
    ConsumerGroupDescription description;
    Map<TopicPartition, OffsetAndMetadata> offsets;

    ConsumerGroupInfo(String groupId, ConsumerGroupDescription description,
                      Map<TopicPartition, OffsetAndMetadata> offsets) {
        super();
        this.groupId = groupId;
        this.description = description;
        this.offsets = offsets;
    }

    String getGroupId() {
        return groupId;
    }

    ConsumerGroupDescription getDescription() {
        return description;
    }

    Map<TopicPartition, OffsetAndMetadata> getOffsets() {
        return offsets;
    }

    static Future<ConsumerGroupInfo> future(String groupId,
                                             Future<Map<String, ConsumerGroupDescription>> descriptionFuture,
                                             Future<Map<TopicPartition, OffsetAndMetadata>> offsetsFuture) {
        Promise<ConsumerGroupInfo> promise = Promise.promise();
        CompositeFuture.join(descriptionFuture, offsetsFuture)
            .onFailure(promise::fail)
            .onSuccess(res -> {
                Map<String, ConsumerGroupDescription> descriptionMap = res.resultAt(0);
                Map<TopicPartition, OffsetAndMetadata> offsets = res.resultAt(1);

                ConsumerGroupDescription description = descriptionMap.get(groupId);
                if (description == null) {
                    promise.fail("Description for consumer group " + groupId + " not found.");
                }

                ConsumerGroupInfo consumerGroupInfo = new ConsumerGroupInfo(groupId, description, offsets);
                promise.complete(consumerGroupInfo);
            });
        return promise.future();
    }
}
