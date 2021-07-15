package org.bf2.admin.kafka.admin.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.vertx.core.Future;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class Types {

    public static class Node {
        private Integer id;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }
    }

    public static class Partition {
        // ID
        private Integer partition;
        private List<Node> replicas;
        // InSyncReplicas
        private List<Node> isr;
        private Node leader;

        public Integer getPartition() {
            return partition;
        }

        public void setPartition(Integer partition) {
            this.partition = partition;
        }

        public List<Node> getReplicas() {
            return replicas;
        }

        public void setReplicas(List<Node> replicas) {
            this.replicas = replicas;
        }

        public List<Node> getIsr() {
            return isr;
        }

        public void setIsr(List<Node> isr) {
            this.isr = isr;
        }

        public Node getLeader() {
            return leader;
        }

        public void setLeader(Node leader) {
            this.leader = leader;
        }
    }

    public static class ConfigEntry {
        private String key;
        private String value;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public static class Topic implements Comparable<Topic> {
        // ID
        private String name;
        private Boolean isInternal;
        private List<Partition> partitions;
        private List<ConfigEntry> config;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Boolean getIsInternal() {
            return isInternal;
        }

        public void setIsInternal(Boolean internal) {
            isInternal = internal;
        }

        public List<Partition> getPartitions() {
            return partitions;
        }

        public void setPartitions(List<Partition> partitions) {
            this.partitions = partitions;
        }

        public List<ConfigEntry> getConfig() {
            return config;
        }

        public void setConfig(List<ConfigEntry> config) {
            this.config = config;
        }

        @Override
        public int compareTo(Topic topic) {
            return getName().compareTo(topic.getName());
        }
    }

    public static class NewTopicConfigEntry {
        private String key;
        private String value;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public static class NewTopicInput {
        private List<NewTopicConfigEntry> config;

        private Integer numPartitions;

        public Integer getNumPartitions() {
            return numPartitions;
        }

        public void setNumPartitions(Integer numPartitions) {
            this.numPartitions = numPartitions;
        }

        public List<NewTopicConfigEntry> getConfig() {
            return config;
        }

        public void setConfig(List<NewTopicConfigEntry> config) {
            this.config = config;
        }
    }

    public static class NewTopic {
        private String name;
        private NewTopicInput settings;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public NewTopicInput getSettings() {
            return settings;
        }

        public void setSettings(NewTopicInput settings) {
            this.settings = settings;
        }
    }

    public static class TopicsToResetOffset {

        private String topic;
        private List<Integer> partitions;

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

    public static class TopicPartitionResetResult {

        private String topic;
        private Integer partition;
        private Long offset;

        public TopicPartitionResetResult() {
        }

        public TopicPartitionResetResult(String topic, Integer partition, Long offset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public Integer getPartition() {
            return partition;
        }

        public void setPartition(Integer partition) {
            this.partition = partition;
        }

        public Long getOffset() {
            return offset;
        }

        public void setOffset(Long offset) {
            this.offset = offset;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof TopicPartitionResetResult)) {
                return false;
            }
            TopicPartitionResetResult other = (TopicPartitionResetResult) obj;

            return Objects.equals(topic, other.topic)
                    && Objects.equals(partition, other.partition)
                    && Objects.equals(offset, other.offset);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partition, offset);
        }
    }

    public static class ConsumerGroupOffsetResetParameters {

        private String groupId;
        private List<TopicsToResetOffset> topics;
        private String offset;
        private String value;

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public List<TopicsToResetOffset> getTopics() {
            return topics;
        }

        public void setTopics(List<TopicsToResetOffset> topics) {
            this.topics = topics;
        }

        public String getOffset() {
            return offset;
        }

        public void setOffset(String offset) {
            this.offset = offset;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public static class UpdatedTopic {
        private String name;
        private List<NewTopicConfigEntry> config;

        private Integer numPartitions;

        public Integer getNumPartitions() {
            return numPartitions;
        }

        public void setNumPartitions(Integer numPartitions) {
            this.numPartitions = numPartitions;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<NewTopicConfigEntry> getConfig() {
            return config;
        }

        public void setConfig(List<NewTopicConfigEntry> config) {
            this.config = config;
        }
    }

    public static class PageRequest {
        private boolean deprecatedFormat;
        private Integer page;
        private Integer size;

        @Deprecated
        private Integer offset;
        @Deprecated
        private Integer limit;

        public boolean isDeprecatedFormat() {
            return deprecatedFormat;
        }

        public void setDeprecatedFormat(boolean deprecatedFormat) {
            this.deprecatedFormat = deprecatedFormat;
        }

        public Integer getPage() {
            return page;
        }

        public void setPage(Integer page) {
            this.page = page;
        }

        public Integer getSize() {
            return size;
        }

        public void setSize(Integer size) {
            this.size = size;
        }

        public Integer getOffset() {
            return offset;
        }

        public void setOffset(Integer offset) {
            this.offset = offset;
        }

        public Integer getLimit() {
            return limit;
        }

        public void setLimit(Integer limit) {
            this.limit = limit;
        }
    }

    public enum SortDirectionEnum {
        DESC,
        ASC;

        public static SortDirectionEnum fromString(String input) {
            if (input == null) {
                return ASC;
            } else if ("desc".equalsIgnoreCase(input)) {
                return DESC;
            } else {
                return ASC;
            }
        }
    }

    public static class OrderByInput {
        private String field;
        private SortDirectionEnum order;

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public SortDirectionEnum getOrder() {
            return order;
        }

        public void setOrder(SortDirectionEnum order) {
            this.order = order;
        }
    }

    public static class ConsumerGroup {
        private String groupId;

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }
    }

    public static class ConsumerGroupDescription extends ConsumerGroup {
        private Set<Consumer> consumers;
        private String state;

        public Set<Consumer> getConsumers() {
            return consumers;
        }

        public void setConsumers(Set<Consumer> consumers) {
            this.consumers = consumers;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }
    }

    @JsonInclude(Include.NON_NULL)
    public static class PagedResponse<T> {
        private List<T> items;
        private Integer size;
        private Integer page;
        private Integer total;
        // deprecated
        private Integer offset;
        private Integer limit;
        private Integer count;

        public static <I> Future<PagedResponse<I>> forItems(List<I> items) {
            PageRequest allResults = new PageRequest();
            allResults.setPage(1);
            allResults.setSize(items.size());
            return forPage(allResults, items);
        }

        public static <I> Future<PagedResponse<I>> forPage(PageRequest pageRequest, List<I> items) {
            final int offset = (pageRequest.getPage() - 1) * pageRequest.getSize();
            final int total = items.size();

            if (total > 0 && offset >= total) {
                return Future.failedFuture(new InvalidRequestException("Requested pagination incorrect. Beginning of list greater than full list size (" + items.size() + ")"));
            }

            final int pageSize = pageRequest.getSize();
            final int pageNumber = pageRequest.getPage();
            final int offsetEnd = Math.min(pageSize * pageNumber, total);

            PagedResponse<I> response = new PagedResponse<>();
            response.setSize(pageSize);
            response.setPage(pageNumber);
            response.setItems(items.subList(offset, offsetEnd));
            response.setTotal(total);

            return Future.succeededFuture(response);
        }

        public List<T> getItems() {
            return items;
        }

        public void setItems(List<T> items) {
            this.items = items;
        }

        public Integer getSize() {
            return size;
        }

        public void setSize(Integer size) {
            this.size = size;
        }

        public Integer getPage() {
            return page;
        }

        public void setPage(Integer page) {
            this.page = page;
        }

        public Integer getTotal() {
            return total;
        }

        public void setTotal(Integer total) {
            this.total = total;
        }

        //deprecated

        public Integer getOffset() {
            return offset;
        }

        public void setOffset(Integer offset) {
            this.offset = offset;
        }

        public Integer getLimit() {
            return limit;
        }

        public void setLimit(Integer limit) {
            this.limit = limit;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }
    }

    public static class ConsumerGroupList extends PagedResponse<ConsumerGroupDescription> {
    }

    public static class TopicList extends PagedResponse<Topic> {
    }

    public static class Consumer {
        private String memberId;
        private String groupId;
        private String topic;
        private Integer partition;
        private long offset;
        private long lag;
        private long logEndOffset;

        public String getMemberId() {
            return memberId;
        }

        public void setMemberId(String memberId) {
            this.memberId = memberId;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public Integer getPartition() {
            return partition;
        }

        public void setPartition(Integer partition) {
            this.partition = partition;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public long getLag() {
            return lag;
        }

        public void setLag(long lag) {
            this.lag = lag;
        }

        public long getLogEndOffset() {
            return logEndOffset;
        }

        public void setLogEndOffset(long logEndOffset) {
            this.logEndOffset = logEndOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Consumer consumer = (Consumer) o;
            return getOffset() == consumer.getOffset() &&
                    getLag() == consumer.getLag() &&
                    getLogEndOffset() == consumer.getLogEndOffset() &&
                    getGroupId().equals(consumer.getGroupId()) &&
                    // topic can be null in the case if number of consumers is greater than number of partitions
                    Objects.equals(getTopic(), consumer.getTopic()) &&
                    getPartition().equals(consumer.getPartition());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getGroupId(), getTopic(), getPartition(), getOffset(), getLag(), getLogEndOffset());
        }
    }

    public static class AclBinding {
        private String resourceType;
        private String resourceName;
        private String patternType;
        private String principal;
        private String operation;
        private String permission;

        public static AclBinding fromQueryParams(io.vertx.core.MultiMap params) {
            var binding = new AclBinding();
            binding.setResourceType(Objects.requireNonNullElse(params.get("resourceType"), "ANY"));
            binding.setResourceName(params.get("resourceName"));
            binding.setPatternType(Objects.requireNonNullElse(params.get("patternType"), "ANY"));
            binding.setPrincipal(Objects.requireNonNullElse(params.get("principal"), ""));
            binding.setOperation(Objects.requireNonNullElse(params.get("operation"), "ANY"));
            binding.setPermission(Objects.requireNonNullElse(params.get("permission"), "ANY"));

            return binding;
        }

        public static AclBinding fromKafkaBinding(org.apache.kafka.common.acl.AclBinding kafkaBinding) {
            var binding = new AclBinding();
            binding.setResourceType(kafkaBinding.pattern().resourceType().toString());
            binding.setResourceName(kafkaBinding.pattern().name());
            binding.setPatternType(kafkaBinding.pattern().patternType().toString());
            binding.setPrincipal(kafkaBinding.entry().principal());
            binding.setOperation(kafkaBinding.entry().operation().toString());
            binding.setPermission(kafkaBinding.entry().permissionType().toString());

            return binding;
        }

        public org.apache.kafka.common.acl.AclBinding toKafkaBinding() {
            var pattern = new ResourcePattern(getKafkaResourceType(), getResourceName(), getKafkaPatternType());
            var entry = new AccessControlEntry(getPrincipal(), "*", getKafkaOperation(), getKafkaPermissionType());
            return new org.apache.kafka.common.acl.AclBinding(pattern, entry);
        }

        public org.apache.kafka.common.acl.AclBindingFilter toKafkaBindingFilter() {
            var patternFilter = new ResourcePatternFilter(getKafkaResourceType(), getResourceName(), getKafkaPatternType());
            var principalFilter = this.principal.isBlank() ? null : this.principal;
            var entryFilter = new AccessControlEntryFilter(principalFilter, null, getKafkaOperation(), getKafkaPermissionType());
            return new org.apache.kafka.common.acl.AclBindingFilter(patternFilter, entryFilter);
        }

        @JsonIgnore
        public ResourceType getKafkaResourceType() {
            return ResourceType.fromString(Objects.requireNonNullElse(resourceType, ""));
        }

        @JsonIgnore
        public PatternType getKafkaPatternType() {
            return PatternType.fromString(Objects.requireNonNullElse(patternType, ""));
        }

        @JsonIgnore
        public AclOperation getKafkaOperation() {
            return AclOperation.fromString(Objects.requireNonNullElse(operation, ""));
        }

        @JsonIgnore
        public AclPermissionType getKafkaPermissionType() {
            return AclPermissionType.fromString(Objects.requireNonNullElse(permission, ""));
        }

        public String getResourceType() {
            return resourceType;
        }

        public void setResourceType(String resourceType) {
            this.resourceType = resourceType;
        }

        public String getResourceName() {
            return resourceName;
        }

        public void setResourceName(String resourceName) {
            this.resourceName = resourceName;
        }

        public String getPatternType() {
            return patternType;
        }

        public void setPatternType(String patternType) {
            this.patternType = patternType;
        }

        public String getPrincipal() {
            return principal;
        }

        public void setPrincipal(String principal) {
            this.principal = principal;
        }

        public String getOperation() {
            return operation;
        }

        public void setOperation(String operation) {
            this.operation = operation;
        }

        public String getPermission() {
            return permission;
        }

        public void setPermission(String permission) {
            this.permission = permission;
        }
    }
}
