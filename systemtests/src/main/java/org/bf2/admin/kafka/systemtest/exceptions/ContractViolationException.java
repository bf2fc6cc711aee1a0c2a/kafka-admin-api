package org.bf2.admin.kafka.systemtest.exceptions;

public class ContractViolationException extends Throwable {
    public ContractViolationException(String message) {
        super(message);
    }
}
