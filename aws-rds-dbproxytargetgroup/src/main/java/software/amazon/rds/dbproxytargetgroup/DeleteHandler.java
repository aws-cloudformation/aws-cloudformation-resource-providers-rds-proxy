package software.amazon.rds.dbproxytargetgroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.AmazonRDSException;
import com.amazonaws.services.rds.model.DBProxyNotFoundException;
import com.amazonaws.services.rds.model.DBProxyTarget;
import com.amazonaws.services.rds.model.DeregisterDBProxyTargetsRequest;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetGroupsRequest;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetsRequest;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetsResult;
import com.amazonaws.services.rds.model.InvalidDBProxyStateException;
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
            boolean deregistered = deregisterOldTargetsHelper(model);
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                           .resourceModel(model)
                           .status(OperationStatus.IN_PROGRESS)
                           .callbackContext(CallbackContext.builder()
                                                           .targetsDeregistered(deregistered)
                                                           .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                           .build())
                           .build();
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                       .resourceModel(model)
                       .status(OperationStatus.SUCCESS)
                       .build();
    }

    private boolean deregisterOldTargetsHelper(ResourceModel model) {
        try {
            return deregisterOldTargets(model);
        } catch (DBProxyNotFoundException e) {
            // Proxy is already deleted, no need to deregister
            return true;
        } catch (InvalidDBProxyStateException e) {
            // Proxy is deleting, no need to deregister
            return true;
        }
    }

    private boolean deregisterOldTargets(ResourceModel model) {
        DescribeDBProxyTargetsRequest describeDBProxyTargetsRequest = new DescribeDBProxyTargetsRequest()
                                                                              .withDBProxyName(model.getDBProxyName())
                                                                              .withTargetGroupName(model.getTargetGroupName());

        DescribeDBProxyTargetsResult describeResult = clientProxy.injectCredentialsAndInvoke(describeDBProxyTargetsRequest, rdsClient::describeDBProxyTargets);

        List<String> dbClusters = new ArrayList<>();
        List<String> dbInstances = new ArrayList<>();
        for (DBProxyTarget target: describeResult.getTargets()) {
            if (target.getType().equals("TRACKED_CLUSTER")) {
                dbClusters.add(target.getRdsResourceId());
            } else {
                dbInstances.add(target.getRdsResourceId());
            }
        }

        if (dbClusters.size() > 0) {
            DeregisterDBProxyTargetsRequest deregisterRequest = new DeregisterDBProxyTargetsRequest()
                                                                        .withDBProxyName(model.getDBProxyName())
                                                                        .withDBClusterIdentifiers(dbClusters);
            clientProxy.injectCredentialsAndInvoke(deregisterRequest, rdsClient::deregisterDBProxyTargets);
        } else if (dbInstances.size() > 0){
            DeregisterDBProxyTargetsRequest deregisterRequest = new DeregisterDBProxyTargetsRequest()
                                                                        .withDBProxyName(model.getDBProxyName())
                                                                        .withDBInstanceIdentifiers(dbInstances);
            clientProxy.injectCredentialsAndInvoke(deregisterRequest, rdsClient::deregisterDBProxyTargets);
        }

        return true;
    }
}
