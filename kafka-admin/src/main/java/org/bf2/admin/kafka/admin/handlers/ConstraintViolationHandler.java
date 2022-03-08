package org.bf2.admin.kafka.admin.handlers;

import org.bf2.admin.kafka.admin.model.Types;

import javax.validation.ConstraintViolationException;
import javax.validation.Path;
import javax.validation.Path.Node;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Provider
public class ConstraintViolationHandler implements ExceptionMapper<ConstraintViolationException> {

    @Override
    public Response toResponse(ConstraintViolationException exception) {
        final int statusCode = Status.BAD_REQUEST.getStatusCode();
        ResponseBuilder response = Response.status(statusCode);
        Types.Error errorEntity = new Types.Error();

        errorEntity.setCode(statusCode);
        errorEntity.setErrorMessage(exception.getConstraintViolations().stream()
                                    .map(violation ->
                                        String.format("%s %s", lastNode(violation.getPropertyPath()), violation.getMessage()))
                                    .collect(Collectors.joining(", ")));

        response.entity(errorEntity);

        return response.build();
    }

    String lastNode(Path propertyPath) {
        return StreamSupport.stream(propertyPath.spliterator(), false)
            .reduce((first, second) -> second)
            .map(Node::toString)
            .orElse("");
    }
}
