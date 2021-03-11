package software.amazon.rds.dbproxyendpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.CreateDBProxyEndpointRequest;
import com.amazonaws.services.rds.model.CreateDBProxyEndpointResult;
import com.amazonaws.services.rds.model.DBProxyEndpoint;
import com.amazonaws.services.rds.model.DBProxyEndpointAlreadyExistsException;
import com.amazonaws.services.rds.model.DescribeDBProxyEndpointsRequest;
import com.amazonaws.services.rds.model.DescribeDBProxyEndpointsResult;
import com.amazonaws.services.rds.model.Tag;
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
    private AmazonRDS rdsClient;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        clientProxy = proxy;
        rdsClient = AmazonRDSClientBuilder.defaultClient();

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
            } catch (DBProxyEndpointAlreadyExistsException e) {
                return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.AlreadyExists);
            }
        } else if (endpointStateSoFar.getStatus().equals(Constants.AVAILABLE_ENDPOINT_STATE)) {
            model.setDBProxyEndpointArn(endpointStateSoFar.getDBProxyEndpointArn());
            model.setEndpoint(endpointStateSoFar.getEndpoint());

            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModel(model)
                    .status(OperationStatus.SUCCESS)
                    .build();
        } else if (Constants.TERMINAL_FAILURE_STATES.contains(endpointStateSoFar.getStatus())) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .status(OperationStatus.FAILED)
                    .errorCode(HandlerErrorCode.NotFound)
                    .build();
        } else {
            model.setDBProxyEndpointArn(endpointStateSoFar.getDBProxyEndpointArn());
            model.setEndpoint(endpointStateSoFar.getEndpoint());

            try {
                Thread.sleep(Constants.POLL_RETRY_DELAY_IN_MS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            DBProxyEndpoint proxyEndpoint = updatedProxyEndpointProgress(endpointStateSoFar.getDBProxyEndpointName());
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

        CreateDBProxyEndpointRequest request = new CreateDBProxyEndpointRequest()
                .withDBProxyName(model.getDBProxyName())
                .withDBProxyEndpointName(model.getDBProxyEndpointName())
                .withVpcSubnetIds(model.getVpcSubnetIds())
                .withVpcSecurityGroupIds(model.getVpcSecurityGroupIds())
                .withTargetRole(model.getTargetRole())
                .withTags(tags);

        CreateDBProxyEndpointResult result = clientProxy.injectCredentialsAndInvoke(request, rdsClient::createDBProxyEndpoint);
        if (result != null) {
            return result.getDBProxyEndpoint();
        } else {
            return null;
        }
    }

    private List<Tag> getTags(ResourceModel model) {
        List<Tag> tags = new ArrayList<>();

        if (model.getTags() == null) {
            return tags;
        }

        for(TagFormat tag : model.getTags()){
            Tag t = new Tag().withKey(tag.getKey()).withValue(tag.getValue());
            tags.add(t);
        }
        return tags;
    }

    private DBProxyEndpoint updatedProxyEndpointProgress(String proxyEndpointName) {
        DescribeDBProxyEndpointsRequest describeDBProxyEndpointsRequest;
        DescribeDBProxyEndpointsResult describeDBProxyEndpointsResult;

        describeDBProxyEndpointsRequest = new DescribeDBProxyEndpointsRequest().withDBProxyEndpointName(proxyEndpointName);
        describeDBProxyEndpointsResult = clientProxy.injectCredentialsAndInvoke(describeDBProxyEndpointsRequest, rdsClient::describeDBProxyEndpoints);
        return describeDBProxyEndpointsResult.getDBProxyEndpoints()
                .stream()
                .findFirst()
                .orElse(new DBProxyEndpoint());
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
