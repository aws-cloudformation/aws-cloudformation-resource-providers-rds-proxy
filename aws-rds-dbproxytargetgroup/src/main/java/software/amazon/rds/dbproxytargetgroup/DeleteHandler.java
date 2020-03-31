package software.amazon.rds.dbproxytargetgroup;

import java.util.List;
import java.util.Optional;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.AmazonRDSException;
import com.amazonaws.services.rds.model.DeregisterDBProxyTargetsRequest;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetGroupsRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandler<CallbackContext> {

    private AmazonWebServicesClientProxy clientProxy;
    private AmazonRDS rdsClient;
    private Logger logger;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        clientProxy = proxy;
        rdsClient = AmazonRDSClientBuilder.defaultClient();
        this.logger = logger;

        final CallbackContext currentContext = Optional.ofNullable(callbackContext)
                                                       .orElse(CallbackContext.builder()
                                                                              .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                                              .build());

        // This Lambda will continually be re-invoked with the current state of the proxy, finally succeeding when deleted.
        return deleteProxyTargetGroup(model, currentContext);
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteProxyTargetGroup(ResourceModel model,
                                                                                 CallbackContext callbackContext) {

        if (!callbackContext.isTargetsDeregistered()) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                           .resourceModel(model)
                           .status(OperationStatus.IN_PROGRESS)
                           .callbackContext(CallbackContext.builder()
                                                           .targetsDeregistered(deregisterOldTargets(model))
                                                           .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                           .build())
                           .build();
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                       .resourceModel(model)
                       .status(OperationStatus.SUCCESS)
                       .build();
    }

    private boolean deregisterOldTargets(ResourceModel model) {
        List<String> oldClusters = Utility.getClusters(model);
        List<String> oldInstances = Utility.getInstances(model);

        if (oldClusters.size() ==0 && oldInstances.size() == 0) {
            return true;
        }

        DeregisterDBProxyTargetsRequest deregisterRequest = new DeregisterDBProxyTargetsRequest()
                                                                    .withDBProxyName(model.getDbProxyName())
                                                                    .withDBClusterIdentifiers(oldClusters)
                                                                    .withDBInstanceIdentifiers(oldInstances);

        try {
            clientProxy.injectCredentialsAndInvoke(deregisterRequest, rdsClient::deregisterDBProxyTargets);
        } catch (AmazonRDSException e) {
            logger.log("Caught exception when deregistering, proceeding anyway");
        }
        return true;
    }
}
