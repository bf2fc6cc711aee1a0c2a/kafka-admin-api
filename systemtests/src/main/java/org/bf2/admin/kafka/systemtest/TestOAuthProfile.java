package org.bf2.admin.kafka.systemtest;

import io.quarkus.test.junit.QuarkusTestProfile;
import org.bf2.admin.kafka.systemtest.deployment.KafkaOAuthSecuredResourceManager;

import java.util.List;

public class TestOAuthProfile implements QuarkusTestProfile {

    public static final int MAX_PARTITIONS = 100;
    public static final int EXCESSIVE_PARTITIONS = 101;

    @Override
    public String getConfigProfile() {
        return "testoauth";
    }

    @Override
    public List<TestResourceEntry> testResources() {
        return List.of(new TestResourceEntry(KafkaOAuthSecuredResourceManager.class));
    }
}
