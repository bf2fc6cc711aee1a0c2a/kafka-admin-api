package admin.kafka.admin;

public class InvalidTopicException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public InvalidTopicException(String message) {
        super(message);
    }
}
