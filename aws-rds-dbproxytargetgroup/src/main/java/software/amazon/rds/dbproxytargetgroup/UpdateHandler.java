package software.amazon.rds.dbproxytargetgroup;

import static software.amazon.rds.dbproxytargetgroup.Utility.validateHealth;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.ConnectionPoolConfiguration;
import software.amazon.awssdk.services.rds.model.DBProxyTarget;
import software.amazon.awssdk.services.rds.model.DBProxyTargetGroup;
import software.amazon.awssdk.services.rds.model.DbProxyTargetGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DeregisterDbProxyTargetsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetsResponse;
import software.amazon.awssdk.services.rds.model.ModifyDbProxyTargetGroupRequest;
import software.amazon.awssdk.services.rds.model.RegisterDbProxyTargetsRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandler<CallbackContext> {
    private AmazonWebServicesClientProxy clientProxy;
    private RdsClient rdsClient;

    private static final String TIMED_OUT_MESSAGE = "Timed out waiting for ProxyTargetGroup to finish modification.";

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

        final CallbackContext currentContext = Optional.ofNullable(callbackContext)
                                                       .orElse(CallbackContext.builder()
                                                                              .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                                              .build());

        // This Lambda will continually be re-invoked with the current state of the proxy, finally succeeding when deleted.
        return updateProxyAndUpdateProgress(newModel, oldModel, currentContext);
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateProxyAndUpdateProgress(ResourceModel newModel,
                                                                                       ResourceModel oldModel,
                                                                                       CallbackContext callbackContext) {
        // This Lambda will continually be re-invoked with the current state of the proxy, finally succeeding when state stabilizes.
        if (callbackContext.getStabilizationRetriesRemaining() == 0) {
            throw new RuntimeException(TIMED_OUT_MESSAGE);
        }

        // Update target-group settings
        if (callbackContext.getTargetGroupStatus() == null) {
            try {
                return ProgressEvent.<ResourceModel, CallbackContext>builder()
                        .resourceModel(newModel)
                        .status(OperationStatus.IN_PROGRESS)
                        .callbackContext(CallbackContext.builder()
                                .targetGroupStatus(modifyProxyTargetGroup(oldModel, newModel))
                                .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                .build())
                        .build();
            } catch (DbProxyTargetGroupNotFoundException e) {
                return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.NotFound);
            }
        }

        // Update registered databases
        if  (!callbackContext.isTargetsDeregistered()) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                           .resourceModel(newModel)
                           .status(OperationStatus.IN_PROGRESS)
                           .callbackContext(CallbackContext.builder()
                                                           .targetGroupStatus(callbackContext.getTargetGroupStatus())
                                                           .targetsDeregistered(deregisterOldTargets(oldModel, newModel))
                                                           .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                           .build())
                           .build();
        }

        if (callbackContext.getTargets() == null) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                           .resourceModel(newModel)
                           .status(OperationStatus.IN_PROGRESS)
                           .callbackContext(CallbackContext.builder()
                                                           .targetGroupStatus(callbackContext.getTargetGroupStatus())
                                                           .targetsDeregistered(callbackContext.isTargetsDeregistered())
                                                           .targets(registerNewTargets(oldModel, newModel))
                                                           .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                           .build())
                           .build();
        }

        if (!callbackContext.isAllTargetsHealthy()) {
            boolean allTargetsHealthy = checkTargetHealth(newModel);

            try {
                Thread.sleep(Constants.POLL_RETRY_DELAY_IN_MS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                           .resourceModel(newModel)
                           .status(OperationStatus.IN_PROGRESS)
                           .callbackContext(CallbackContext.builder()
                               .targetGroupStatus(callbackContext.getTargetGroupStatus())
                               .targetsDeregistered(callbackContext.isTargetsDeregistered())
                               .targets(callbackContext.getTargets())
                               .stabilizationRetriesRemaining(callbackContext.getStabilizationRetriesRemaining() - 1)
                               .allTargetsHealthy(allTargetsHealthy)
                               .build())
                           .build();
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                       .resourceModel(newModel)
                       .status(OperationStatus.SUCCESS)
                       .build();
    }

    private DBProxyTargetGroup modifyProxyTargetGroup(ResourceModel oldModel, ResourceModel newModel) {
        if (oldModel.equals(newModel)) {
            return DBProxyTargetGroup.builder().build();
        }

        ConnectionPoolConfigurationInfoFormat modelConnectionPoolConfig = newModel.getConnectionPoolConfigurationInfo();
        if (modelConnectionPoolConfig == null) {
            DescribeDbProxyTargetGroupsRequest describeRequest = DescribeDbProxyTargetGroupsRequest.builder()
                                                                         .dbProxyName(newModel.getDBProxyName())
                                                                         .targetGroupName(newModel.getTargetGroupName())
                                                                         .build();
            return clientProxy.injectCredentialsAndInvokeV2(describeRequest, rdsClient::describeDBProxyTargetGroups).targetGroups().get(0);
        }

        ConnectionPoolConfiguration connectionPoolConfiguration =
                ConnectionPoolConfiguration.builder()
                        .maxConnectionsPercent(modelConnectionPoolConfig.getMaxConnectionsPercent())
                        .maxIdleConnectionsPercent(modelConnectionPoolConfig.getMaxIdleConnectionsPercent())
                        .connectionBorrowTimeout(modelConnectionPoolConfig.getConnectionBorrowTimeout())
                        .sessionPinningFilters(modelConnectionPoolConfig.getSessionPinningFilters())
                        .initQuery(modelConnectionPoolConfig.getInitQuery())
                        .build();

        ModifyDbProxyTargetGroupRequest request = ModifyDbProxyTargetGroupRequest.builder()
                                                          .dbProxyName(newModel.getDBProxyName())
                                                          .targetGroupName(newModel.getTargetGroupName())
                                                          .connectionPoolConfig(connectionPoolConfiguration)
                                                          .build();

        return clientProxy.injectCredentialsAndInvokeV2(request, rdsClient::modifyDBProxyTargetGroup).dbProxyTargetGroup();
    }

    private List<DBProxyTarget> registerNewTargets(ResourceModel oldModel, ResourceModel newModel) {
        List<String> oldClusters = Utility.getClusters(oldModel);
        List<String> newClusters = Utility.getClusters(newModel);
        List<String> clustersToAdd =  listNewObjects(newClusters, oldClusters);

        List<String> oldInstances = Utility.getInstances(oldModel);
        List<String> newInstances = Utility.getInstances(newModel);
        List<String> instancesToAdd =  listNewObjects(newInstances, oldInstances);

        if (clustersToAdd.size() == 0 && instancesToAdd.size() == 0) {
            return new ArrayList<>();
        }

        RegisterDbProxyTargetsRequest registerRequest = RegisterDbProxyTargetsRequest.builder()
                                                                .dbProxyName(newModel.getDBProxyName())
                                                                .targetGroupName(newModel.getTargetGroupName())
                                                                .dbClusterIdentifiers(clustersToAdd)
                                                                .dbInstanceIdentifiers(instancesToAdd)
                                                                .build();
        return clientProxy.injectCredentialsAndInvokeV2(registerRequest, rdsClient::registerDBProxyTargets).dbProxyTargets();
    }

    private boolean deregisterOldTargets(ResourceModel oldModel, ResourceModel newModel) {
        List<String> oldClusters = Utility.getClusters(oldModel);
        List<String> newClusters = Utility.getClusters(newModel);
        List<String> clustersToRemove =  listNewObjects(oldClusters, newClusters);

        List<String> oldInstances = Utility.getInstances(oldModel);
        List<String> newInstances = Utility.getInstances(newModel);
        List<String> instancesToRemove =  listNewObjects(oldInstances, newInstances);

        if (clustersToRemove.size() == 0 && instancesToRemove.size() == 0) {
            return true;
        }

        DeregisterDbProxyTargetsRequest deregisterRequest = DeregisterDbProxyTargetsRequest.builder()
                                                                    .dbProxyName(newModel.getDBProxyName())
                                                                    .targetGroupName(newModel.getTargetGroupName())
                                                                    .dbClusterIdentifiers(clustersToRemove)
                                                                    .dbInstanceIdentifiers(instancesToRemove)
                                                                    .build();
        clientProxy.injectCredentialsAndInvokeV2(deregisterRequest, rdsClient::deregisterDBProxyTargets);
        return true;
    }

    private List<String> listNewObjects(List<String> list1, List<String> list2) {
        if (!list1.isEmpty() && !list2.isEmpty()) {
            list1.removeAll(list2);
        }
        return list1;
    }

    private boolean checkTargetHealth(ResourceModel model) {
        String proxyName = model.getDBProxyName();
        String targetGroupName = Optional.ofNullable(model.getTargetGroupName()).orElse("default");

        DescribeDbProxyTargetsRequest describeDBProxyTargetsRequest = DescribeDbProxyTargetsRequest.builder()
                                                                              .dbProxyName(proxyName)
                                                                              .targetGroupName(targetGroupName)
                                                                              .build();

        DescribeDbProxyTargetsResponse targetsResponse = clientProxy.injectCredentialsAndInvokeV2(describeDBProxyTargetsRequest, rdsClient::describeDBProxyTargets);
        return validateHealth(targetsResponse);
    }
}
