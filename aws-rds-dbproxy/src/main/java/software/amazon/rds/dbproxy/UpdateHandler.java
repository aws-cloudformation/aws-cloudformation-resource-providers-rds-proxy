package software.amazon.rds.dbproxy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.DBProxy;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbProxyNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbProxiesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxiesResponse;
import software.amazon.awssdk.services.rds.model.ModifyDbProxyRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbProxyResponse;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.awssdk.services.rds.model.UserAuthConfig;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandler<CallbackContext> {
    private AmazonWebServicesClientProxy clientProxy;
    private RdsClient rdsClient;
    private Logger log;

    private static final String TIMED_OUT_MESSAGE = "Timed out waiting for proxy to finish modification.";
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

        // This Lambda will continually be re-invoked with the current state of the proxy, finally succeeding when deleted.
        return updateProxyAndUpdateProgress(newModel, oldModel, currentContext);
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateProxyAndUpdateProgress(ResourceModel newModel,
                                                                                       ResourceModel oldModel,
                                                                                       CallbackContext callbackContext) {
        // This Lambda will continually be re-invoked with the current state of the proxy, finally succeeding when state stabilizes.
        final DBProxy proxyStateSoFar = callbackContext.getProxy();

        if (callbackContext.getStabilizationRetriesRemaining() == 0) {
            throw new RuntimeException(TIMED_OUT_MESSAGE);
        }

        // Update proxy settings
        if (proxyStateSoFar == null) {
            try {
                return ProgressEvent.<ResourceModel, CallbackContext>builder()
                        .resourceModel(newModel)
                        .status(OperationStatus.IN_PROGRESS)
                        .callbackContext(CallbackContext.builder()
                                .proxy(updateProxySettings(oldModel, newModel))
                                .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                .build())
                        .build();
            } catch (DbProxyNotFoundException e) {
                return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.NotFound);
            }
        }

        // Update tags
        if (!callbackContext.isTagsDeregistered()) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                           .resourceModel(newModel)
                           .status(OperationStatus.IN_PROGRESS)
                           .callbackContext(CallbackContext.builder()
                                                           .proxy(proxyStateSoFar)
                                                           .tagsDeregistered(deregisterOldTags(oldModel, newModel, proxyStateSoFar))
                                                           .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                           .build())
                           .build();
        }

        if (!callbackContext.isTagsRegistered()) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                           .resourceModel(newModel)
                           .status(OperationStatus.IN_PROGRESS)
                           .callbackContext(CallbackContext.builder()
                                                           .proxy(proxyStateSoFar)
                                                           .tagsDeregistered(callbackContext.isTagsDeregistered())
                                                           .tagsRegistered(registerNewTags(oldModel, newModel, proxyStateSoFar))
                                                           .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                           .build())
                           .build();
        }

        if (proxyStateSoFar.status().toString().equals(Constants.AVAILABLE_PROXY_STATE)) {
            newModel.setVpcId(proxyStateSoFar.vpcId());
            newModel.setDebugLogging(proxyStateSoFar.debugLogging());
            newModel.setIdleClientTimeout(proxyStateSoFar.idleClientTimeout());
            newModel.setRequireTLS(proxyStateSoFar.requireTLS());

            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                           .resourceModel(newModel)
                           .status(OperationStatus.SUCCESS)
                           .build();
        } else if (Constants.TERMINAL_FAILURE_STATES.contains(proxyStateSoFar.status().toString())) {
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

            DBProxy proxy = updatedProxyProgress(proxyStateSoFar.dbProxyName());
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                           .resourceModel(newModel)
                           .status(OperationStatus.IN_PROGRESS)
                           .callbackContext(CallbackContext.builder()
                                                           .tagsDeregistered(callbackContext.isTagsDeregistered())
                                                           .tagsRegistered(callbackContext.isTagsRegistered())
                                                           .proxy(proxy)
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
        return tagList.stream().map(t -> Tag.builder().key(t.getKey()).value(t.getValue()).build()).collect(Collectors.toList());
    }

    private boolean deregisterOldTags(ResourceModel oldModel, ResourceModel newModel, DBProxy proxy) {
        List<TagFormat> oldTags = getTagKeys(oldModel);
        List<TagFormat> newTags = getTagKeys(newModel);
        List<TagFormat> tagsToRemove = listNewTags(oldTags, newTags);
        List<String> tagKeyList = tagsToRemove.stream().map(t -> t.getKey()).collect(Collectors.toList());

        if (tagKeyList.size() > 0) {
            RemoveTagsFromResourceRequest removeTagsRequest = RemoveTagsFromResourceRequest.builder()
                                                                      .resourceName(proxy.dbProxyArn())
                                                                      .tagKeys(tagKeyList)
                                                                      .build();
            clientProxy.injectCredentialsAndInvokeV2(removeTagsRequest, rdsClient::removeTagsFromResource);
        }
        return true;
    }

    private boolean registerNewTags(ResourceModel oldModel, ResourceModel newModel, DBProxy proxy) {
        List<TagFormat> oldTags = getTagKeys(oldModel);
        List<TagFormat> newTags = getTagKeys(newModel);
        List<TagFormat> tagsToAdd = listNewTags(newTags, oldTags);
        if (tagsToAdd.size() > 0 ) {
            AddTagsToResourceRequest addTagsRequest = AddTagsToResourceRequest.builder()
                                                              .resourceName(proxy.dbProxyArn())
                                                              .tags(toRDSTags(tagsToAdd))
                                                              .build();
            clientProxy.injectCredentialsAndInvokeV2(addTagsRequest, rdsClient::addTagsToResource);
        }
        return true;
    }

    private DBProxy updateProxySettings(ResourceModel oldModel, ResourceModel newModel) {
        List<UserAuthConfig> userAuthConfig = Utility.getUserAuthConfigs(newModel);
        ModifyDbProxyRequest request = ModifyDbProxyRequest.builder()
                                               .auth(userAuthConfig)
                                               .dbProxyName(oldModel.getDBProxyName())
                                               .debugLogging(newModel.getDebugLogging())
                                               .idleClientTimeout(newModel.getIdleClientTimeout())
                                               .requireTLS(newModel.getRequireTLS())
                                               .roleArn(newModel.getRoleArn())
                                               .securityGroups(newModel.getVpcSecurityGroupIds())
                                               .build();

        ModifyDbProxyResponse response = clientProxy.injectCredentialsAndInvokeV2(request, rdsClient::modifyDBProxy);
        return response.dbProxy();
    }

    private DBProxy updatedProxyProgress(String proxyName) {
        DescribeDbProxiesRequest describeDBProxiesRequest = DescribeDbProxiesRequest.builder()
                .dbProxyName(proxyName)
                .build();

        DescribeDbProxiesResponse describeDbProxiesResponse = clientProxy.injectCredentialsAndInvokeV2(describeDBProxiesRequest, rdsClient::describeDBProxies);
        return describeDbProxiesResponse.dbProxies()
                                      .stream()
                                      .findFirst()
                                      .orElse(DBProxy.builder().build());
    }
}
