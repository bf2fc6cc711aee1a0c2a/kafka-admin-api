package org.bf2.admin.kafka.systemtest.json;

import java.util.List;

public class OffsetModel {
    String offset;
    List<PartitionsModel> partitions;

    public OffsetModel(String offset, List<PartitionsModel> partitions) {
        this.offset = offset;
        this.partitions = partitions;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }

    public List<PartitionsModel> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<PartitionsModel> partitions) {
        this.partitions = partitions;
    }
}
