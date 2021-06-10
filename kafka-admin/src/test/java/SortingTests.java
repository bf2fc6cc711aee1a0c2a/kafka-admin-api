import org.bf2.admin.kafka.admin.handlers.CommonHandler;
import org.bf2.admin.kafka.admin.model.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class SortingTests {

    // topic
    @Test
    public void testRetentionMs1() {
        String key = "retention.ms";
        Types.Topic first = new Types.Topic();
        Types.Topic second = new Types.Topic();

        Types.ConfigEntry ce1 = new Types.ConfigEntry();
        ce1.setKey(key);
        ce1.setValue("100");
        first.setConfig(Collections.singletonList(ce1));

        Types.ConfigEntry ce2 = new Types.ConfigEntry();
        ce2.setKey(key);
        ce2.setValue("102");
        second.setConfig(Collections.singletonList(ce2));

        CommonHandler.TopicComparator topicComparator = new CommonHandler.TopicComparator(key);
        topicComparator.compare(first, second);

        Assertions.assertEquals(-1, topicComparator.compare(first, second));
    }

    @Test
    public void testRetentionMs2() {
        String key = "retention.ms";
        Types.Topic first = new Types.Topic();
        Types.Topic second = new Types.Topic();

        Types.ConfigEntry ce1 = new Types.ConfigEntry();
        ce1.setKey(key);
        ce1.setValue("102");
        first.setConfig(Collections.singletonList(ce1));

        Types.ConfigEntry ce2 = new Types.ConfigEntry();
        ce2.setKey(key);
        ce2.setValue("100");
        second.setConfig(Collections.singletonList(ce2));

        CommonHandler.TopicComparator topicComparator = new CommonHandler.TopicComparator(key);
        topicComparator.compare(first, second);

        Assertions.assertEquals(1, topicComparator.compare(first, second));
    }

    @Test
    public void testRetentionMs3() {
        String key = "retention.ms";
        Types.Topic first = new Types.Topic();
        Types.Topic second = new Types.Topic();

        Types.ConfigEntry ce1 = new Types.ConfigEntry();
        ce1.setKey(key);
        ce1.setValue("102");
        first.setConfig(Collections.singletonList(ce1));

        Types.ConfigEntry ce2 = new Types.ConfigEntry();
        ce2.setKey(key);
        ce2.setValue("-1");
        second.setConfig(Collections.singletonList(ce2));

        CommonHandler.TopicComparator topicComparator = new CommonHandler.TopicComparator(key);
        topicComparator.compare(first, second);

        Assertions.assertEquals(-1, topicComparator.compare(first, second));
    }


    @Test
    public void testRetentionMs4() {
        String key = "retention.ms";
        Types.Topic first = new Types.Topic();
        Types.Topic second = new Types.Topic();

        Types.ConfigEntry ce1 = new Types.ConfigEntry();
        ce1.setKey(key);
        ce1.setValue("102");
        first.setConfig(Collections.singletonList(ce1));

        Types.ConfigEntry ce2 = new Types.ConfigEntry();
        ce2.setKey(key);
        ce2.setValue("-1");
        second.setConfig(Collections.singletonList(ce2));

        CommonHandler.TopicComparator topicComparator = new CommonHandler.TopicComparator(key + "_unknown");
        topicComparator.compare(first, second);

        // unknown key to compare - return 0
        Assertions.assertEquals(0, topicComparator.compare(first, second));
    }

    @Test
    public void testRetentionMs5() {
        String key = "retention.ms";
        Types.Topic first = new Types.Topic();
        Types.Topic second = new Types.Topic();

        Types.ConfigEntry ce1 = new Types.ConfigEntry();
        // this topic does not have retention.ms key
        ce1.setKey(key + "_unknown");
        ce1.setValue("102");
        first.setConfig(Collections.singletonList(ce1));

        Types.ConfigEntry ce2 = new Types.ConfigEntry();
        ce2.setKey(key);
        ce2.setValue("-1");
        second.setConfig(Collections.singletonList(ce2));

        CommonHandler.TopicComparator topicComparator = new CommonHandler.TopicComparator(key);
        Assertions.assertEquals(0, topicComparator.compare(first, second));
    }

    @Test
    public void testRetentionMs6() {
        String key = "retention.ms";
        Types.Topic first = new Types.Topic();
        Types.Topic second = new Types.Topic();

        Types.ConfigEntry ce1 = new Types.ConfigEntry();
        // this topic does have retention.ms key, but it is null
        ce1.setKey(key);
        ce1.setValue(null);
        first.setConfig(Collections.singletonList(ce1));

        Types.ConfigEntry ce2 = new Types.ConfigEntry();
        ce2.setKey(key);
        ce2.setValue("-1");
        second.setConfig(Collections.singletonList(ce2));

        CommonHandler.TopicComparator topicComparator = new CommonHandler.TopicComparator(key);
        Assertions.assertEquals(0, topicComparator.compare(first, second));
    }

    // consumer group

    @Test
    public void testConsumerGroup1() {
        String key = "name";
        Types.ConsumerGroup first = new Types.ConsumerGroup();
        first.setGroupId("abc");

        Types.ConsumerGroup second = new Types.ConsumerGroup();
        second.setGroupId("bcd");

        CommonHandler.ConsumerGroupComparator consumerGroupComparator = new CommonHandler.ConsumerGroupComparator(key);
        Assertions.assertEquals(-1, consumerGroupComparator.compare(first, second));
    }

    @Test
    public void testConsumerGroup2() {
        String key = "name";
        Types.ConsumerGroup first = new Types.ConsumerGroup();
        first.setGroupId("abc");

        Types.ConsumerGroup second = new Types.ConsumerGroup();
        second.setGroupId("ABC");

        CommonHandler.ConsumerGroupComparator consumerGroupComparator = new CommonHandler.ConsumerGroupComparator(key);
        Assertions.assertEquals(0, consumerGroupComparator.compare(first, second));
    }

    @Test
    public void testConsumerGroup3() {
        String key = "name";
        Types.ConsumerGroup first = new Types.ConsumerGroup();
        first.setGroupId("abc");

        Types.ConsumerGroup second = new Types.ConsumerGroup();
        second.setGroupId("BCD");

        CommonHandler.ConsumerGroupComparator consumerGroupComparator = new CommonHandler.ConsumerGroupComparator(key);
        Assertions.assertEquals(-1, consumerGroupComparator.compare(first, second));
    }

    @Test
    public void testConsumerGroup4() {
        String key = "name";
        Types.ConsumerGroup first = new Types.ConsumerGroup();
        // this group does not have an ID. It should not happen, but we want robust code!

        Types.ConsumerGroup second = new Types.ConsumerGroup();
        second.setGroupId("my-group");

        CommonHandler.ConsumerGroupComparator consumerGroupComparator = new CommonHandler.ConsumerGroupComparator(key);
        Assertions.assertEquals(0, consumerGroupComparator.compare(first, second));
    }

}
