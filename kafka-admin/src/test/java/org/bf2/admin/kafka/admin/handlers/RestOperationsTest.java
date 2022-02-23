package org.bf2.admin.kafka.admin.handlers;

import org.bf2.admin.kafka.admin.HttpMetrics;
import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.bf2.admin.kafka.admin.model.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RestOperationsTest {
    private static final int MAX_PARTITIONS = 100;
    /*HttpMetrics httpMetrics;
    KafkaAdminConfigRetriever config;*/
    protected RestOperations restOperations;

    @ParameterizedTest(name = "testNumPartitionsLessThanEqualToMax")
    @ValueSource(ints = {99,100})
     void testNumPartitionsLessThanEqualToMaxTrue(int numPartitions){
        Types.UpdatedTopic settings = new Types.UpdatedTopic();
        settings.setNumPartitions(numPartitions);
        assertTrue(restOperations.numPartitionsLessThanEqualToMax(settings, MAX_PARTITIONS));
    }
    @Test
    void testNumPartitionsLessThanEqualToMaxFalse(){
        Types.UpdatedTopic settings = new Types.UpdatedTopic();
        settings.setNumPartitions(101);
        assertFalse(restOperations.numPartitionsLessThanEqualToMax(settings, MAX_PARTITIONS));
    }

}
