package software.amazon.rds.dbproxyendpoint;

import static software.amazon.rds.dbproxyendpoint.Utility.listEqualsIgnoreOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.AddTagsToResourceRequest;
import com.amazonaws.services.rds.model.DBProxyEndpoint;
import com.amazonaws.services.rds.model.DBProxyEndpointNotFoundException;
import com.amazonaws.services.rds.model.DescribeDBProxyEndpointsRequest;
import com.amazonaws.services.rds.model.DescribeDBProxyEndpointsResult;
import com.amazonaws.services.rds.model.ModifyDBProxyEndpointRequest;
import com.amazonaws.services.rds.model.ModifyDBProxyEndpointResult;
import com.amazonaws.services.rds.model.RemoveTagsFromResourceRequest;
import com.amazonaws.services.rds.model.Tag;
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
    private AmazonRDS rdsClient;
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
        rdsClient = AmazonRDSClientBuilder.defaultClient();
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
            } catch (DBProxyEndpointNotFoundException e) {
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

        if (proxyEndpointStateSoFar.getStatus().equals(Constants.AVAILABLE_ENDPOINT_STATE)) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModel(newModel)
                    .status(OperationStatus.SUCCESS)
                    .build();
        } else if (Constants.TERMINAL_FAILURE_STATES.contains(proxyEndpointStateSoFar.getStatus())) {
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

            DBProxyEndpoint proxyEndpoint = updatedProxyEndpointProgress(proxyEndpointStateSoFar.getDBProxyEndpointName());
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
        if (list1.size() > 0 && list2.size() > 0) {
            list1.removeAll(list2);
        }
        return list1;
    }

    private List<Tag> toRDSTags(List<TagFormat> tagList) {
        return tagList.stream().map(t -> new Tag().withKey(t.getKey()).withValue(t.getValue())).collect(Collectors.toList());
    }

    private boolean deregisterOldTags(ResourceModel oldModel, ResourceModel newModel, DBProxyEndpoint proxyEndpoint) {
        List<TagFormat> oldTags = getTagKeys(oldModel);
        List<TagFormat> newTags = getTagKeys(newModel);
        List<TagFormat> tagsToRemove = listNewTags(oldTags, newTags);
        List<String> tagKeyList = tagsToRemove.stream().map(t -> t.getKey()).collect(Collectors.toList());

        if (tagKeyList.size() > 0) {
            RemoveTagsFromResourceRequest removeTagsRequest = new RemoveTagsFromResourceRequest()
                    .withResourceName(proxyEndpoint.getDBProxyEndpointArn())
                    .withTagKeys(tagKeyList);
            clientProxy.injectCredentialsAndInvoke(removeTagsRequest, rdsClient::removeTagsFromResource);
        }
        return true;
    }

    private boolean registerNewTags(ResourceModel oldModel, ResourceModel newModel, DBProxyEndpoint proxyEndpoint) {
        List<TagFormat> oldTags = getTagKeys(oldModel);
        List<TagFormat> newTags = getTagKeys(newModel);
        List<TagFormat> tagsToAdd = listNewTags(newTags, oldTags);
        if (tagsToAdd.size() > 0 ) {
            AddTagsToResourceRequest addTagsRequest = new AddTagsToResourceRequest()
                    .withResourceName(proxyEndpoint.getDBProxyEndpointArn())
                    .withTags(toRDSTags(tagsToAdd));
            clientProxy.injectCredentialsAndInvoke(addTagsRequest, rdsClient::addTagsToResource);
        }
        return true;
    }

    private DBProxyEndpoint updateProxyEndpointSettings(ResourceModel oldModel, ResourceModel newModel) {
        ModifyDBProxyEndpointRequest request = new ModifyDBProxyEndpointRequest()
                .withDBProxyEndpointName(oldModel.getDBProxyEndpointName())
                .withVpcSecurityGroupIds(newModel.getVpcSecurityGroupIds());

        ModifyDBProxyEndpointResult result = clientProxy.injectCredentialsAndInvoke(request, rdsClient::modifyDBProxyEndpoint);
        if (result != null) {
            return result.getDBProxyEndpoint();
        } else {
            return null;
        }
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
