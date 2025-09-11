package software.amazon.rds.dbproxytargetgroup;

import static software.amazon.rds.dbproxytargetgroup.Utility.validateHealth;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.ConnectionPoolConfiguration;
import software.amazon.awssdk.services.rds.model.DBProxy;
import software.amazon.awssdk.services.rds.model.DBProxyTarget;
import software.amazon.awssdk.services.rds.model.DBProxyTargetGroup;
import software.amazon.awssdk.services.rds.model.DbProxyNotFoundException;
import software.amazon.awssdk.services.rds.model.DbProxyTargetGroupNotFoundException;
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

public class CreateHandler extends BaseHandler<CallbackContext> {
    public static final String TIMED_OUT_MESSAGE = "Timed out waiting for target group to become available.";

    private AmazonWebServicesClientProxy clientProxy;
    private RdsClient rdsClient;

    private Logger log;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {
        final ResourceModel model = request.getDesiredResourceState();

        clientProxy = proxy;
        rdsClient = RdsClient.create();
        log = logger;

        final CallbackContext currentContext = Optional.ofNullable(callbackContext)
                                                       .orElse(CallbackContext.builder()
                                                                              .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                                              .build());

        // This Lambda will continually be re-invoked with the current state of the proxy, finally succeeding when state stabilizes.
        try {
            return createTargetGroupAndUpdateProgress(model, currentContext);
        } catch (DbProxyNotFoundException | DbProxyTargetGroupNotFoundException e) {
            return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.NotFound);
        }
    }

    private ProgressEvent<ResourceModel, CallbackContext> createTargetGroupAndUpdateProgress(ResourceModel model,
                                                                                             CallbackContext callbackContext) {
        // This Lambda will continually be re-invoked with the current state of the proxy, finally succeeding when state stabilizes.
        final DBProxy proxyStateSoFar = callbackContext.getProxy();

        if (callbackContext.getStabilizationRetriesRemaining() == 0) {
            throw new RuntimeException(TIMED_OUT_MESSAGE);
        }

        if (callbackContext.getTargetGroupStatus() == null) {
            DBProxyTargetGroup targetGroupSettings = modifyProxyTargetGroup(model);
            model.setTargetGroupArn(targetGroupSettings.targetGroupArn());
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                           .resourceModel(model)
                           .status(OperationStatus.IN_PROGRESS)
                           .callbackContext(CallbackContext.builder()
                                                           .proxy(proxyStateSoFar)
                                                           .targetGroupStatus(targetGroupSettings)
                                                           .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                           .build())
                           .build();

        } else {
            if (callbackContext.getTargets() == null) {
                //If targets have not been setup, register them
                List<DBProxyTarget> targets = registerDefaultTarget(model);
                return ProgressEvent.<ResourceModel, CallbackContext>builder()
                               .resourceModel(model)
                               .status(OperationStatus.IN_PROGRESS)
                               .callbackContext(CallbackContext.builder()
                                                               .proxy(proxyStateSoFar)
                                                               .targetGroupStatus(callbackContext.getTargetGroupStatus())
                                                               .targets(targets)
                                                               .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                               .build())
                               .build();
            } else {
                if (!callbackContext.isAllTargetsHealthy()) {
                    boolean allTargetsHealthy = checkTargetHealth(model);

                    try {
                        Thread.sleep(Constants.POLL_RETRY_DELAY_IN_MS);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    return ProgressEvent.<ResourceModel, CallbackContext>builder()
                                   .resourceModel(model)
                                   .status(OperationStatus.IN_PROGRESS)
                                   .callbackContext(CallbackContext.builder()
                                       .proxy(proxyStateSoFar)
                                       .targetGroupStatus(callbackContext.getTargetGroupStatus())
                                       .targets(callbackContext.getTargets())
                                       .stabilizationRetriesRemaining(callbackContext.getStabilizationRetriesRemaining() - 1)
                                       .allTargetsHealthy(allTargetsHealthy)
                                       .build())
                                   .build();
                } else {
                    if (!callbackContext.isAllTargetsHealthy()) {
                        boolean allTargetsHealthy = checkTargetHealth(model);

                        try {
                            Thread.sleep(Constants.POLL_RETRY_DELAY_IN_MS);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }

                        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                                       .resourceModel(model)
                                       .status(OperationStatus.IN_PROGRESS)
                                       .callbackContext(CallbackContext.builder()
                                           .proxy(proxyStateSoFar)
                                           .targetGroupStatus(callbackContext.getTargetGroupStatus())
                                           .targets(callbackContext.getTargets())
                                           .stabilizationRetriesRemaining(callbackContext.getStabilizationRetriesRemaining() - 1)
                                           .allTargetsHealthy(allTargetsHealthy)
                                           .build())
                                       .build();
                    } else {
                        //All setup has been completed
                        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                                       .resourceModel(model)
                                       .status(OperationStatus.SUCCESS)
                                       .build();
                    }
                }
            }
        }

    }

    private DBProxyTargetGroup modifyProxyTargetGroup(ResourceModel model) {
        ConnectionPoolConfigurationInfoFormat modelConnectionPoolConfig = model.getConnectionPoolConfigurationInfo();

        if (modelConnectionPoolConfig == null) {
            DescribeDbProxyTargetGroupsRequest describeRequest = DescribeDbProxyTargetGroupsRequest.builder()
                                                                         .dbProxyName(model.getDBProxyName())
                                                                         .targetGroupName(model.getTargetGroupName())
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
                                                          .dbProxyName(model.getDBProxyName())
                                                          .targetGroupName(model.getTargetGroupName())
                                                          .connectionPoolConfig(connectionPoolConfiguration)
                                                          .build();

        return clientProxy.injectCredentialsAndInvokeV2(request, rdsClient::modifyDBProxyTargetGroup).dbProxyTargetGroup();
    }

    private List<DBProxyTarget> registerDefaultTarget(ResourceModel model) {
        List<String> newClusters = Utility.getClusters(model);
        List<String> newInstances = Utility.getInstances(model);

        if (newClusters.isEmpty() && newInstances.isEmpty()) {
            return new ArrayList<>();
        }

        RegisterDbProxyTargetsRequest registerRequest = RegisterDbProxyTargetsRequest.builder()
                                                                .dbProxyName(model.getDBProxyName())
                                                                .targetGroupName(model.getTargetGroupName())
                                                                .dbClusterIdentifiers(newClusters)
                                                                .dbInstanceIdentifiers(newInstances)
                                                                .build();

        return clientProxy.injectCredentialsAndInvokeV2(registerRequest, rdsClient::registerDBProxyTargets).dbProxyTargets();
    }

    private boolean checkTargetHealth(ResourceModel model) {
        DescribeDbProxyTargetsRequest describeDBProxyTargetsRequest = DescribeDbProxyTargetsRequest.builder()
                                                                              .dbProxyName(model.getDBProxyName())
                                                                              .targetGroupName(model.getTargetGroupName())
                                                                              .build();

        DescribeDbProxyTargetsResponse describeResponse = clientProxy.injectCredentialsAndInvokeV2(describeDBProxyTargetsRequest, rdsClient::describeDBProxyTargets);
        return validateHealth(describeResponse);
    }
}
