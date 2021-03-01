/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest.utils;

import org.apache.kafka.clients.admin.AdminClient;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class DynamicWait {
    public static  <T> void waitFor(Callable<T> func, T expected, int timeInSec) throws Exception {
        int attempts = 0;
        AtomicBoolean ready = new AtomicBoolean(false);
        while (attempts++ < timeInSec && !ready.get()) {
            T b = func.call();
            if (b == expected) {
                ready.set(true);
            }
            Thread.sleep(1000);
        }
        if (!ready.get()) {
            throw new TimeoutException();
        }
    }

    public static void waitForTopicExists(String topicName, AdminClient kafkaClient) throws Exception {
        waitFor(() -> {
            if (kafkaClient.listTopics().names().get().contains(topicName)) {
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        }, Boolean.TRUE, 10);
    }

    public static void waitForTopicsExists(Collection<String> topicNames, AdminClient kafkaClient) throws Exception {
        waitFor(() -> {
            if (kafkaClient.listTopics().names().get().containsAll(topicNames)) {
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        }, Boolean.TRUE, 20);
    }

    public static void waitForTopicsToBeDeleted(Collection<String> topicNames, AdminClient kafkaClient) throws Exception {
        waitFor(() -> {
            if (!kafkaClient.listTopics().names().get().containsAll(topicNames)) {
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        }, Boolean.TRUE, 10);
    }

    public static void waitForTopicToBeDeleted(String topicName, AdminClient kafkaClient) throws Exception {
        waitFor(() -> {
            if (!kafkaClient.listTopics().names().get().contains(topicName)) {
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        }, Boolean.TRUE, 10);
    }
}
