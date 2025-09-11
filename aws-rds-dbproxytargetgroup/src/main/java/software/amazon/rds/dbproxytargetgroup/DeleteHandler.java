package software.amazon.rds.dbproxytargetgroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBProxyTarget;
import software.amazon.awssdk.services.rds.model.DbProxyNotFoundException;
import software.amazon.awssdk.services.rds.model.DeregisterDbProxyTargetsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetsResponse;
import software.amazon.awssdk.services.rds.model.InvalidDbProxyStateException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandler<CallbackContext> {

    private AmazonWebServicesClientProxy clientProxy;
    private RdsClient rdsClient;
    private Logger logger;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        clientProxy = proxy;
        rdsClient = RdsClient.create();
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
                       .status(OperationStatus.SUCCESS)
                       .build();
    }

    private boolean deregisterOldTargetsHelper(ResourceModel model) {
        try {
            return deregisterOldTargets(model);
        } catch (DbProxyNotFoundException e) {
            // Proxy is already deleted, no need to deregister
            return true;
        } catch (InvalidDbProxyStateException e) {
            if (e.getMessage().contains("DELETING")) {
                // Proxy is deleting, no need to deregister
                return true;
            }
            throw e;
        }
    }

    private boolean deregisterOldTargets(ResourceModel model) {
        String proxyName = model.getDBProxyName();
        String targetGroupName = Optional.ofNullable(model.getTargetGroupName()).orElse("default");

        DescribeDbProxyTargetsRequest describeDBProxyTargetsRequest = DescribeDbProxyTargetsRequest.builder()
                                                                              .dbProxyName(proxyName)
                                                                              .targetGroupName(targetGroupName)
                                                                              .build();

        DescribeDbProxyTargetsResponse describeResponse = clientProxy.injectCredentialsAndInvokeV2(describeDBProxyTargetsRequest, rdsClient::describeDBProxyTargets);

        List<String> dbClusters = new ArrayList<>();
        List<String> dbInstances = new ArrayList<>();
        for (DBProxyTarget target: describeResponse.targets()) {
            if (target.type().toString().equals("TRACKED_CLUSTER")) {
                dbClusters.add(target.rdsResourceId());
            } else {
                dbInstances.add(target.rdsResourceId());
            }
        }

        if (!dbClusters.isEmpty()) {
            DeregisterDbProxyTargetsRequest deregisterRequest = DeregisterDbProxyTargetsRequest.builder()
                                                                        .dbProxyName(model.getDBProxyName())
                                                                        .dbClusterIdentifiers(dbClusters)
                                                                        .build();
            clientProxy.injectCredentialsAndInvokeV2(deregisterRequest, rdsClient::deregisterDBProxyTargets);
        } else if (!dbInstances.isEmpty()){
            DeregisterDbProxyTargetsRequest deregisterRequest = DeregisterDbProxyTargetsRequest.builder()
                                                                        .dbProxyName(model.getDBProxyName())
                                                                        .dbInstanceIdentifiers(dbInstances)
                                                                        .build();
            clientProxy.injectCredentialsAndInvokeV2(deregisterRequest, rdsClient::deregisterDBProxyTargets);
        }

        return true;
    }
}
