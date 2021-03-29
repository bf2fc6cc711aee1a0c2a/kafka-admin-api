package org.bf2.admin.kafka.admin;

public class InvalidConsumerGroupException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public InvalidConsumerGroupException(String message) {
        super(message);
    }
}
