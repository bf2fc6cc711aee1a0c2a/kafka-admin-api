package org.bf2.admin.kafka.systemtest.json;

import org.bf2.admin.kafka.admin.model.Types;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.buffer.Buffer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ModelDeserializer {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public ModelDeserializer() {
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public <T> T deserializeResponse(Buffer responseBuffer, Class<T> clazz) {
        T deserialized = null;
        try {
            deserialized = MAPPER.readValue(responseBuffer.toString(), clazz);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return deserialized;
    }


    public <T> String serializeBody(T object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Set<String> getNames(Buffer responseBuffer) {
        Set<String> names = null;
        try {
            Types.TopicList topicList = MAPPER.readValue(responseBuffer.toString(), Types.TopicList.class);
            names = new HashSet<>();
            for (Types.Topic topic : topicList.getItems()) {
                names.add(topic.getName());
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return names;
    }

    public List<String> getGroupsId(Buffer responseBuffer) {
        List<String> groupsID = null;
        try {
            groupsID = MAPPER.readValue(responseBuffer.toString(), new TypeReference<List<Types.ConsumerGroupDescription>>() { })
                    .stream().map(Types.ConsumerGroup::getGroupId).collect(Collectors.toList());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return groupsID;
    }

    public Types.TopicPartitionResetResult getResetResult(Buffer responseBuffer) {
        Types.TopicPartitionResetResult res = null;
        try {
            res = MAPPER.readValue(responseBuffer.toString(), new TypeReference<List<Types.TopicPartitionResetResult>>() { }).get(0);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return res;
    }

    public Types.ConsumerGroupDescription getGroupDesc(Buffer responseBuffer) {
        Types.ConsumerGroupDescription groupsDesc = null;
        try {
            groupsDesc = MAPPER.readValue(responseBuffer.toString(), Types.ConsumerGroupDescription.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return groupsDesc;
    }
}
