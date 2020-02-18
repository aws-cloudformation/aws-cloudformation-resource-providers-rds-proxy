package software.amazon.rds.dbproxy;

import java.util.List;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.DBProxy;
import com.amazonaws.services.rds.model.ModifyDBProxyRequest;
import com.amazonaws.services.rds.model.ModifyDBProxyResult;
import com.amazonaws.services.rds.model.UserAuthConfig;
import com.google.common.collect.ImmutableList;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandler<CallbackContext> {
    private AmazonWebServicesClientProxy clientProxy;
    private AmazonRDS rdsClient;

    private static final int NUMBER_OF_STATE_POLL_RETRIES = 60;

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

        final CallbackContext currentContext = callbackContext == null ?
                                               CallbackContext.builder().stabilizationRetriesRemaining(NUMBER_OF_STATE_POLL_RETRIES).build() :
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
                           .resourceModels(ImmutableList.of(oldModel, newModel))
                           .status(OperationStatus.IN_PROGRESS)
                           .callbackContext(CallbackContext.builder()
                                                           .proxy(updateProxySettings(oldModel, newModel))
                                                           .stabilizationRetriesRemaining(NUMBER_OF_STATE_POLL_RETRIES)
                                                           .build())
                           .build();
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                       .resourceModel(newModel)
                       .status(OperationStatus.SUCCESS)
                       .build();
    }

    private DBProxy updateProxySettings(ResourceModel oldModel, ResourceModel newModel) {
        List<UserAuthConfig> userAuthConfig = Utility.getUserAuthConfigs(newModel);

        ModifyDBProxyRequest request = new ModifyDBProxyRequest()
                                               .withAuth(userAuthConfig)
                                               .withDBProxyName(oldModel.getDbProxyName())
                                               .withDebugLogging(newModel.getDebugLogging())
                                               .withIdleClientTimeout(newModel.getIdleClientTimeout())
                                               .withNewDBProxyName(newModel.getDbProxyName())
                                               .withRequireTLS(newModel.getRequireTLS())
                                               .withRoleArn(newModel.getRoleArn())
                                               .withSecurityGroups(newModel.getVpcSecurityGroupIds());

        ModifyDBProxyResult result = clientProxy.injectCredentialsAndInvoke(request, rdsClient::modifyDBProxy);
        if (result != null) {
            return result.getDBProxy();
        } else {
            return null;
        }
    }
}
