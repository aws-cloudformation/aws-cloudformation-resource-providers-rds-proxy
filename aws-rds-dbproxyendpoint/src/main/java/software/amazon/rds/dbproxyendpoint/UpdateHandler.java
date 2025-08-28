package software.amazon.rds.dbproxyendpoint;

import static software.amazon.rds.dbproxyendpoint.Utility.listEqualsIgnoreOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.DBProxyEndpoint;
import software.amazon.awssdk.services.rds.model.DbProxyEndpointNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyEndpointsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyEndpointsResponse;
import software.amazon.awssdk.services.rds.model.ModifyDbProxyEndpointRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbProxyEndpointResponse;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandler<CallbackContext> {
    public static final String DB_PROXY_NAME_CREATE_ONLY_MESSAGE = "DBProxyName is a create-only property and cannot be updated.";
    public static final String DB_PROXY_ENDPOINT_NAME_CREATE_ONLY_MESSAGE = "DBProxyEndpointName is a create-only property and cannot be updated.";
    public static final String VPC_SUBNET_ID_CREATE_ONLY_MESSAGE = "VPCSubnetId is a create-only property and cannot be updated.";
    public static final String TARGET_ROLE_CREATE_ONLY_MESSAGE = "TargetRole is a create-only property and cannot be updated.";

    private AmazonWebServicesClientProxy clientProxy;
    private RdsClient rdsClient;
    private Logger log;

    private static final String TIMED_OUT_MESSAGE = "Timed out waiting for proxyEndpoint to finish modification.";
    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        final ResourceModel newModel = request.getDesiredResourceState();
        final ResourceModel oldModel = request.getPreviousResourceState();

        clientProxy = proxy;
        rdsClient = RdsClient.create();
        log = logger;

        final CallbackContext currentContext = callbackContext == null ?
                CallbackContext.builder().stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES).build() :
                callbackContext;

        // This Lambda will continually be re-invoked with the current state of the proxyEndpoint, finally succeeding when deleted.
        return updateProxyEndpointAndUpdateProgress(newModel, oldModel, currentContext);
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateProxyEndpointAndUpdateProgress(ResourceModel newModel,
                                                                                       ResourceModel oldModel,
                                                                                       CallbackContext callbackContext) {
        // This Lambda will continually be re-invoked with the current state of the proxyEndpoint, finally succeeding when state stabilizes.
        final DBProxyEndpoint proxyEndpointStateSoFar = callbackContext.getProxyEndpoint();

        if (callbackContext.getStabilizationRetriesRemaining() == 0) {
            throw new RuntimeException(TIMED_OUT_MESSAGE);
        }

        // Update proxyEndpoint settings
        if (proxyEndpointStateSoFar == null) {
            try {
                return Optional.ofNullable(validateModels(oldModel, newModel))
                        .orElseGet(() -> ProgressEvent.<ResourceModel, CallbackContext>builder()
                        .resourceModel(newModel)
                        .status(OperationStatus.IN_PROGRESS)
                        .callbackContext(CallbackContext.builder()
                                .proxyEndpoint(updateProxyEndpointSettings(oldModel, newModel))
                                .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                .build())
                        .build());
            } catch (DbProxyEndpointNotFoundException e) {
                return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.NotFound);
            }
        }

        // Update tags
        if (!callbackContext.isTagsDeregistered()) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModel(newModel)
                    .status(OperationStatus.IN_PROGRESS)
                    .callbackContext(CallbackContext.builder()
                            .proxyEndpoint(proxyEndpointStateSoFar)
                            .tagsDeregistered(deregisterOldTags(oldModel, newModel, proxyEndpointStateSoFar))
                            .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                            .build())
                    .build();
        }

        if (!callbackContext.isTagsRegistered()) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModel(newModel)
                    .status(OperationStatus.IN_PROGRESS)
                    .callbackContext(CallbackContext.builder()
                            .proxyEndpoint(proxyEndpointStateSoFar)
                            .tagsDeregistered(callbackContext.isTagsDeregistered())
                            .tagsRegistered(registerNewTags(oldModel, newModel, proxyEndpointStateSoFar))
                            .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                            .build())
                    .build();
        }

        if (proxyEndpointStateSoFar.status().toString().equals(Constants.AVAILABLE_ENDPOINT_STATE)) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModel(newModel)
                    .status(OperationStatus.SUCCESS)
                    .build();
        } else if (Constants.TERMINAL_FAILURE_STATES.contains(proxyEndpointStateSoFar.status().toString())) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .status(OperationStatus.FAILED)
                    .errorCode(HandlerErrorCode.NotFound)
                    .build();
        } else {
            try {
                Thread.sleep(Constants.POLL_RETRY_DELAY_IN_MS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            DBProxyEndpoint proxyEndpoint = updatedProxyEndpointProgress(proxyEndpointStateSoFar.dbProxyEndpointName());
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModel(newModel)
                    .status(OperationStatus.IN_PROGRESS)
                    .callbackContext(CallbackContext.builder()
                            .tagsDeregistered(callbackContext.isTagsDeregistered())
                            .tagsRegistered(callbackContext.isTagsRegistered())
                            .proxyEndpoint(proxyEndpoint)
                            .stabilizationRetriesRemaining(callbackContext.getStabilizationRetriesRemaining() - 1)
                            .build())
                    .build();
        }
    }

    private List<TagFormat> getTagKeys(ResourceModel model) {
        return Optional.ofNullable(model.getTags()).orElse(new ArrayList<>()).stream().collect(Collectors.toList());
    }

    private List<TagFormat> listNewTags(List<TagFormat> list1, List<TagFormat> list2) {
        if (!list1.isEmpty() && !list2.isEmpty()) {
            list1.removeAll(list2);
        }
        return list1;
    }

    private List<Tag> toRDSTags(List<TagFormat> tagList) {
        return tagList.stream().map(t -> Tag.builder().key(t.getKey()).value(t.getValue()).build()).collect(Collectors.toList());
    }

    private boolean deregisterOldTags(ResourceModel oldModel, ResourceModel newModel, DBProxyEndpoint proxyEndpoint) {
        List<TagFormat> oldTags = getTagKeys(oldModel);
        List<TagFormat> newTags = getTagKeys(newModel);
        List<TagFormat> tagsToRemove = listNewTags(oldTags, newTags);
        List<String> tagKeyList = tagsToRemove.stream().map(t -> t.getKey()).collect(Collectors.toList());

        if (!tagKeyList.isEmpty()) {
            RemoveTagsFromResourceRequest removeTagsRequest = RemoveTagsFromResourceRequest.builder()
                    .resourceName(proxyEndpoint.dbProxyEndpointArn())
                    .tagKeys(tagKeyList)
                    .build();
            clientProxy.injectCredentialsAndInvokeV2(removeTagsRequest, rdsClient::removeTagsFromResource);
        }
        return true;
    }

    private boolean registerNewTags(ResourceModel oldModel, ResourceModel newModel, DBProxyEndpoint proxyEndpoint) {
        List<TagFormat> oldTags = getTagKeys(oldModel);
        List<TagFormat> newTags = getTagKeys(newModel);
        List<TagFormat> tagsToAdd = listNewTags(newTags, oldTags);
        if (!tagsToAdd.isEmpty()) {
            AddTagsToResourceRequest addTagsRequest = AddTagsToResourceRequest.builder()
                    .resourceName(proxyEndpoint.dbProxyEndpointArn())
                    .tags(toRDSTags(tagsToAdd))
                    .build();
            clientProxy.injectCredentialsAndInvokeV2(addTagsRequest, rdsClient::addTagsToResource);
        }
        return true;
    }

    private DBProxyEndpoint updateProxyEndpointSettings(ResourceModel oldModel, ResourceModel newModel) {
        ModifyDbProxyEndpointRequest request = ModifyDbProxyEndpointRequest.builder()
                .dbProxyEndpointName(oldModel.getDBProxyEndpointName())
                .vpcSecurityGroupIds(newModel.getVpcSecurityGroupIds())
                .build();

        ModifyDbProxyEndpointResponse response = clientProxy.injectCredentialsAndInvokeV2(request, rdsClient::modifyDBProxyEndpoint);
        return response.dbProxyEndpoint();
    }

    private DBProxyEndpoint updatedProxyEndpointProgress(String proxyEndpointName) {
        DescribeDbProxyEndpointsRequest describeDBProxyEndpointsRequest = DescribeDbProxyEndpointsRequest.builder()
                .dbProxyEndpointName(proxyEndpointName)
                .build();
        DescribeDbProxyEndpointsResponse describeDBProxyEndpointsResponse = clientProxy.injectCredentialsAndInvokeV2(describeDBProxyEndpointsRequest, rdsClient::describeDBProxyEndpoints);
        return describeDBProxyEndpointsResponse.dbProxyEndpoints()
                .stream()
                .findFirst()
                .orElse(DBProxyEndpoint.builder().build());
    }

    /**
     * Validate the model in the first run.
     * @return null if validation passed.
     *         ProgressEvent if validation failed
     */
    private ProgressEvent<ResourceModel, CallbackContext> validateModels(ResourceModel oldModel, ResourceModel newModel) {
        if (StringUtils.isNotEmpty(newModel.getDBProxyName()) && !newModel.getDBProxyName().equals(oldModel.getDBProxyName())){
            return ProgressEvent.defaultFailureHandler(
                    new CfnInvalidRequestException(DB_PROXY_NAME_CREATE_ONLY_MESSAGE),
                    HandlerErrorCode.NotUpdatable);
        }

        if (StringUtils.isNotEmpty(newModel.getDBProxyEndpointName())
                && !newModel.getDBProxyEndpointName().equals(oldModel.getDBProxyEndpointName())){
            return ProgressEvent.defaultFailureHandler(
                    new CfnInvalidRequestException(DB_PROXY_ENDPOINT_NAME_CREATE_ONLY_MESSAGE),
                    HandlerErrorCode.NotUpdatable);
        }

        if (newModel.getVpcSubnetIds() != null && !newModel.getVpcSubnetIds().isEmpty()
                && !listEqualsIgnoreOrder(newModel.getVpcSubnetIds(), oldModel.getVpcSubnetIds())){
            return ProgressEvent.defaultFailureHandler(
                    new CfnInvalidRequestException(VPC_SUBNET_ID_CREATE_ONLY_MESSAGE),
                    HandlerErrorCode.NotUpdatable);
        }

        if (StringUtils.isNotEmpty(newModel.getTargetRole())
                && !newModel.getTargetRole().equals(oldModel.getTargetRole())){
            return ProgressEvent.defaultFailureHandler(
                    new CfnInvalidRequestException(TARGET_ROLE_CREATE_ONLY_MESSAGE),
                    HandlerErrorCode.NotUpdatable);
        }

        return null;
    }

}
