package software.amazon.rds.dbproxy;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.AddTagsToResourceRequest;
import com.amazonaws.services.rds.model.DBProxy;
import com.amazonaws.services.rds.model.DBProxyNotFoundException;
import com.amazonaws.services.rds.model.DescribeDBProxiesRequest;
import com.amazonaws.services.rds.model.DescribeDBProxiesResult;
import com.amazonaws.services.rds.model.ModifyDBProxyRequest;
import com.amazonaws.services.rds.model.ModifyDBProxyResult;
import com.amazonaws.services.rds.model.RemoveTagsFromResourceRequest;
import com.amazonaws.services.rds.model.Tag;
import com.amazonaws.services.rds.model.UserAuthConfig;
import com.google.common.collect.ImmutableList;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandler<CallbackContext> {
    private AmazonWebServicesClientProxy clientProxy;
    private AmazonRDS rdsClient;
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
        rdsClient = AmazonRDSClientBuilder.defaultClient();
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
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                           .resourceModel(newModel)
                           .status(OperationStatus.IN_PROGRESS)
                           .callbackContext(CallbackContext.builder()
                                                           .proxy(updateProxySettings(oldModel, newModel))
                                                           .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                           .build())
                           .build();
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

        if (proxyStateSoFar.getStatus().equals(Constants.AVAILABLE_PROXY_STATE)) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                           .resourceModel(newModel)
                           .status(OperationStatus.SUCCESS)
                           .build();
        } else if (Constants.TERMINAL_FAILURE_STATES.contains(proxyStateSoFar.getStatus())) {
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

            DBProxy proxy = updatedProxyProgress(proxyStateSoFar.getDBProxyName());
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
        return tagList.stream().map(t -> new Tag().withKey(t.getKey()).withValue(t.getValue())).collect(Collectors.toList());
    }

    private boolean deregisterOldTags(ResourceModel oldModel, ResourceModel newModel, DBProxy proxy) {
        List<TagFormat> oldTags = getTagKeys(oldModel);
        List<TagFormat> newTags = getTagKeys(newModel);
        List<TagFormat> tagsToRemove = listNewTags(oldTags, newTags);
        List<String> tagKeyList = tagsToRemove.stream().map(t -> t.getKey()).collect(Collectors.toList());

        if (tagKeyList.size() > 0) {
            RemoveTagsFromResourceRequest removeTagsRequest = new RemoveTagsFromResourceRequest()
                                                                      .withResourceName(proxy.getDBProxyArn())
                                                                      .withTagKeys(tagKeyList);
            clientProxy.injectCredentialsAndInvoke(removeTagsRequest, rdsClient::removeTagsFromResource);
        }
        return true;
    }

    private boolean registerNewTags(ResourceModel oldModel, ResourceModel newModel, DBProxy proxy) {
        List<TagFormat> oldTags = getTagKeys(oldModel);
        List<TagFormat> newTags = getTagKeys(newModel);
        List<TagFormat> tagsToAdd = listNewTags(newTags, oldTags);
        if (tagsToAdd.size() > 0 ) {
            AddTagsToResourceRequest addTagsRequest = new AddTagsToResourceRequest()
                                                              .withResourceName(proxy.getDBProxyArn())
                                                              .withTags(toRDSTags(tagsToAdd));
            clientProxy.injectCredentialsAndInvoke(addTagsRequest, rdsClient::addTagsToResource);
        }
        return true;
    }

    private DBProxy updateProxySettings(ResourceModel oldModel, ResourceModel newModel) {
        List<UserAuthConfig> userAuthConfig = Utility.getUserAuthConfigs(newModel);
        ModifyDBProxyRequest request = new ModifyDBProxyRequest()
                                               .withAuth(userAuthConfig)
                                               .withDBProxyName(oldModel.getDBProxyName())
                                               .withDebugLogging(newModel.getDebugLogging())
                                               .withIdleClientTimeout(newModel.getIdleClientTimeout())
                                               .withRequireTLS(newModel.getRequireTLS())
                                               .withRoleArn(newModel.getRoleArn())
                                               .withSecurityGroups(newModel.getVpcSecurityGroupIds());

        try {
            ModifyDBProxyResult result = clientProxy.injectCredentialsAndInvoke(request, rdsClient::modifyDBProxy);
            if (result != null) {
                return result.getDBProxy();
            } else {
                return null;
            }
        } catch (DBProxyNotFoundException e) {
            throw new software.amazon.cloudformation.exceptions.ResourceNotFoundException(ResourceModel.TYPE_NAME,
                                                                                          Objects.toString(oldModel.getDBProxyName()));
        }
    }

    private DBProxy updatedProxyProgress(String proxyName) {
        DescribeDBProxiesRequest describeDBProxiesRequest;
        DescribeDBProxiesResult describeDBProxiesResult;

        describeDBProxiesRequest = new DescribeDBProxiesRequest().withDBProxyName(proxyName);
        describeDBProxiesResult = clientProxy.injectCredentialsAndInvoke(describeDBProxiesRequest, rdsClient::describeDBProxies);
        return describeDBProxiesResult.getDBProxies()
                                      .stream()
                                      .findFirst()
                                      .orElse(new DBProxy());
    }
}
