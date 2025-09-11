package software.amazon.rds.dbproxyendpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbProxyEndpointRequest;
import software.amazon.awssdk.services.rds.model.CreateDbProxyEndpointResponse;
import software.amazon.awssdk.services.rds.model.DBProxyEndpoint;
import software.amazon.awssdk.services.rds.model.DbProxyEndpointAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyEndpointsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyEndpointsResponse;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class CreateHandler extends BaseHandler<CallbackContext> {
    public static final String TIMED_OUT_MESSAGE = "Timed out waiting for proxyEndpoint to become available.";
    public static final String DB_PROXY_ENDPOINT_ARN_READ_ONLY_MESSAGE = "DBProxyEndpointArn is a read-only property.";
    public static final String ENDPOINT_READ_ONLY_MESSAGE = "Endpoint is a read-only property.";
    public static final String IS_DEFAULT_READ_ONLY_MESSAGE = "IsDefault is a read-only property.";
    public static final String VPC_ID_READ_ONLY_MESSAGE = "VpcId is a read-only property.";

    private AmazonWebServicesClientProxy clientProxy;
    private RdsClient rdsClient;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        clientProxy = proxy;
        rdsClient = RdsClient.create();

        final CallbackContext currentContext = Optional.ofNullable(callbackContext)
                .orElse(CallbackContext.builder()
                        .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                        .build());

        // This Lambda will continually be re-invoked with the current state of the proxyEndpoint, finally succeeding when state stabilizes.
        return createProxyEndpointAndUpdateProgress(model, currentContext);
    }

    private ProgressEvent<ResourceModel, CallbackContext> createProxyEndpointAndUpdateProgress(ResourceModel model,
                                                                                       CallbackContext callbackContext) {
        // This Lambda will continually be re-invoked with the current state of the proxyEndpoint, finally succeeding when state stabilizes.
        final DBProxyEndpoint endpointStateSoFar = callbackContext.getProxyEndpoint();

        if (callbackContext.getStabilizationRetriesRemaining() == 0) {
            throw new RuntimeException(TIMED_OUT_MESSAGE);
        }

        // first time
        if (endpointStateSoFar == null) {
            try {
                return Optional.ofNullable(validateModel(model))
                        .orElseGet(() -> ProgressEvent.<ResourceModel, CallbackContext>builder()
                        .resourceModel(model)
                        .status(OperationStatus.IN_PROGRESS)
                        .callbackContext(CallbackContext.builder()
                                .proxyEndpoint(createProxyEndpoint(model))
                                .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                .build())
                        .build());
            } catch (DbProxyEndpointAlreadyExistsException e) {
                return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.AlreadyExists);
            }
        } else if (endpointStateSoFar.status().toString().equals(Constants.AVAILABLE_ENDPOINT_STATE)) {
            model.setDBProxyEndpointArn(endpointStateSoFar.dbProxyEndpointArn());
            model.setEndpoint(endpointStateSoFar.endpoint());

            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModel(model)
                    .status(OperationStatus.SUCCESS)
                    .build();
        } else if (Constants.TERMINAL_FAILURE_STATES.contains(endpointStateSoFar.status().toString())) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .status(OperationStatus.FAILED)
                    .errorCode(HandlerErrorCode.NotFound)
                    .build();
        } else {
            model.setDBProxyEndpointArn(endpointStateSoFar.dbProxyEndpointArn());
            model.setEndpoint(endpointStateSoFar.endpoint());

            try {
                Thread.sleep(Constants.POLL_RETRY_DELAY_IN_MS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            DBProxyEndpoint proxyEndpoint = updatedProxyEndpointProgress(endpointStateSoFar.dbProxyEndpointName());
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModel(model)
                    .status(OperationStatus.IN_PROGRESS)
                    .callbackContext(CallbackContext.builder()
                            .proxyEndpoint(proxyEndpoint)
                            .stabilizationRetriesRemaining(callbackContext.getStabilizationRetriesRemaining() - 1)
                            .build())
                    .build();
        }
    }

    private DBProxyEndpoint createProxyEndpoint(ResourceModel model) {
        List<Tag> tags = getTags(model);

        CreateDbProxyEndpointRequest request = CreateDbProxyEndpointRequest.builder()
                .dbProxyName(model.getDBProxyName())
                .dbProxyEndpointName(model.getDBProxyEndpointName())
                .vpcSubnetIds(model.getVpcSubnetIds())
                .vpcSecurityGroupIds(model.getVpcSecurityGroupIds())
                .targetRole(model.getTargetRole())
                .tags(tags)
                .build();

        CreateDbProxyEndpointResponse response = clientProxy.injectCredentialsAndInvokeV2(request, rdsClient::createDBProxyEndpoint);
        return response.dbProxyEndpoint();
    }

    private List<Tag> getTags(ResourceModel model) {
        List<Tag> tags = new ArrayList<>();

        if (model.getTags() == null) {
            return tags;
        }

        for(TagFormat tag : model.getTags()){
            Tag t = Tag.builder().key(tag.getKey()).value(tag.getValue()).build();
            tags.add(t);
        }
        return tags;
    }

    private DBProxyEndpoint updatedProxyEndpointProgress(String proxyEndpointName) {
        DescribeDbProxyEndpointsRequest describeDBProxyEndpointsRequest = DescribeDbProxyEndpointsRequest.builder().dbProxyEndpointName(proxyEndpointName).build();
        DescribeDbProxyEndpointsResponse describeDbProxyEndpointsResponse = clientProxy.injectCredentialsAndInvokeV2(describeDBProxyEndpointsRequest, rdsClient::describeDBProxyEndpoints);
        return describeDbProxyEndpointsResponse.dbProxyEndpoints()
                .stream()
                .findFirst()
                .orElse(DBProxyEndpoint.builder().build());
    }

    /**
     * Validate the model in the first run.
     * @return null if validation passed.
     *         ProgressEvent if validation failed
     */
    private ProgressEvent<ResourceModel, CallbackContext> validateModel(ResourceModel model) {
        if (StringUtils.isNotEmpty(model.getDBProxyEndpointArn())){
            return ProgressEvent.defaultFailureHandler(
                    new CfnInvalidRequestException(DB_PROXY_ENDPOINT_ARN_READ_ONLY_MESSAGE),
                    HandlerErrorCode.InvalidRequest);
        }

        if (StringUtils.isNotEmpty(model.getEndpoint())){
            return ProgressEvent.defaultFailureHandler(
                    new CfnInvalidRequestException(ENDPOINT_READ_ONLY_MESSAGE),
                    HandlerErrorCode.InvalidRequest);
        }

        if (StringUtils.isNotEmpty(model.getVpcId())){
            return ProgressEvent.defaultFailureHandler(
                    new CfnInvalidRequestException(VPC_ID_READ_ONLY_MESSAGE),
                    HandlerErrorCode.InvalidRequest);
        }

        if (model.getIsDefault() != null){
            return ProgressEvent.defaultFailureHandler(
                    new CfnInvalidRequestException(IS_DEFAULT_READ_ONLY_MESSAGE),
                    HandlerErrorCode.InvalidRequest);
        }

        return null;
    }
}
