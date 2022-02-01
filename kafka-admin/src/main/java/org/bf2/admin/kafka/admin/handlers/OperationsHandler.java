package org.bf2.admin.kafka.admin.handlers;

import org.bf2.admin.kafka.admin.Operations;
import org.bf2.admin.kafka.admin.model.Types;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface OperationsHandler {

    @POST
    @Path("topics")
    @Operation(operationId = Operations.CREATE_TOPIC)
    CompletionStage<Response> createTopic(Types.NewTopic newTopic);

    @GET
    @Path("topics/{topicName}")
    @Operation(operationId = Operations.GET_TOPIC)
    CompletionStage<Response> describeTopic(@PathParam("topicName") String topicName);

    @PATCH
    @Path("topics/{topicName}")
    @Operation(operationId = Operations.UPDATE_TOPIC)
    CompletionStage<Response> updateTopic(@PathParam("topicName") String topicName, Types.UpdatedTopic updatedTopic);

    @DELETE
    @Path("topics/{topicName}")
    @Operation(operationId = Operations.DELETE_TOPIC)
    CompletionStage<Response> deleteTopic(@PathParam("topicName") String topicName);

    @GET
    @Path("topics")
    @Operation(operationId = Operations.GET_TOPICS_LIST)
    CompletionStage<Response> listTopics(@QueryParam("filter") String filter, UriInfo requestUri);

    @GET
    @Path("consumer-groups")
    @Operation(operationId = Operations.GET_CONSUMER_GROUPS_LIST)
    CompletionStage<Response> listGroups(@QueryParam("group-id-filter") String groupFilter, @QueryParam("topic") String topicFilter, UriInfo requestUri);

    @GET
    @Path("consumer-groups/{consumerGroupId}")
    @Operation(operationId = Operations.GET_CONSUMER_GROUP)
    CompletionStage<Response> describeGroup(@PathParam("consumerGroupId") String consumerGroupId, @QueryParam("partitionFilter") Optional<Integer> partitionFilter, UriInfo requestUri);

    @DELETE
    @Path("consumer-groups/{consumerGroupId}")
    @Operation(operationId = Operations.DELETE_CONSUMER_GROUP)
    CompletionStage<Response> deleteGroup(@PathParam("consumerGroupId") String consumerGroupId);

    @POST
    @Path("consumer-groups/{consumerGroupId}/reset-offset")
    @Operation(operationId = Operations.RESET_CONSUMER_GROUP_OFFSET)
    CompletionStage<Response> resetGroupOffset(@PathParam("consumerGroupId") String consumerGroupId, Types.ConsumerGroupOffsetResetParameters parameters);

    @GET
    @Path("acls/resource-operations")
    @Operation(operationId = Operations.GET_ACL_RESOURCE_OPERATIONS)
    Response getAclResourceOperations();

    @GET
    @Path("acls")
    @Operation(operationId = Operations.GET_ACLS)
    @Parameter(name = Types.AclBinding.PROP_RESOURCE_TYPE, in = ParameterIn.QUERY)
    CompletionStage<Response> describeAcls(UriInfo requestUri);

    @POST
    @Path("acls")
    @Operation(operationId = Operations.CREATE_ACL)
    CompletionStage<Response> createAcl(Types.AclBinding binding);

    @DELETE
    @Path("acls")
    @Operation(operationId = Operations.DELETE_ACLS)
    CompletionStage<Response> deleteAcls(UriInfo requestUri);
}
