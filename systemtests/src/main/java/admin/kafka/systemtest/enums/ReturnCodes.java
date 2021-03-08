package admin.kafka.systemtest.enums;

public enum ReturnCodes {
    KAFKA_DOWN(503),
    NOT_FOUND(404),
    FAILED_REQUEST(400),
    SERVER_ERROR(500),
    TOPIC_CREATED(201),
    DUPLICATED(409),
    UNAUTHORIZED(401),
    SUCCESS(200);

    public final int code;
    ReturnCodes(int code) {
        this.code = code;
    }
}
