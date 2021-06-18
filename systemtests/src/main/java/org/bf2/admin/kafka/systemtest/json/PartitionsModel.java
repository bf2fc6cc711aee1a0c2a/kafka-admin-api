package org.bf2.admin.kafka.systemtest.json;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

public class PartitionsModel {
    String topic;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    List<Integer> partitions;

    public PartitionsModel(String topic, List<Integer> partitions) {
        this.topic = topic;
        this.partitions = partitions;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public List<Integer> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<Integer> partitions) {
        this.partitions = partitions;
    }
}
