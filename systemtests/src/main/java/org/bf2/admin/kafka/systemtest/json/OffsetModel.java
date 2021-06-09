package org.bf2.admin.kafka.systemtest.json;

import java.util.List;

public class OffsetModel {
    String offset;
    String value;
    List<PartitionsModel> topics;

    public OffsetModel(String offset, String value, List<PartitionsModel> topics) {
        this.offset = offset;
        this.value = value;
        this.topics = topics;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }

    public List<PartitionsModel> getTopics() {
        return topics;
    }

    public void setTopics(List<PartitionsModel> topics) {
        this.topics = topics;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
