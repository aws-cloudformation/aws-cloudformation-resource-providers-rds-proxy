package software.amazon.rds.dbproxytargetgroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBProxyTarget;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetGroupsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetsResponse;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ReadHandler extends BaseHandler<CallbackContext> {
    private AmazonWebServicesClientProxy clientProxy;
    private RdsClient rdsClient;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        final ResourceModel desiredResource = request.getDesiredResourceState();

        String proxyName = desiredResource.getDBProxyName();
        String targetGroupName = Optional.ofNullable(desiredResource.getTargetGroupName()).orElse("default");

        clientProxy = proxy;
        rdsClient = RdsClient.create();

        final ResourceModel model = describeDBProxyTargetGroup(proxyName, targetGroupName);

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                       .resourceModel(model)
                       .status(OperationStatus.SUCCESS)
                       .build();
    }

    private ResourceModel describeDBProxyTargetGroup(final String proxyName,
                                                     final String targetGroupName) {
        DescribeDbProxyTargetGroupsRequest request = DescribeDbProxyTargetGroupsRequest.builder()
                                                             .dbProxyName(proxyName)
                                                             .targetGroupName(targetGroupName)
                                                             .build();

        final DescribeDbProxyTargetGroupsResponse response = clientProxy.injectCredentialsAndInvokeV2(request, rdsClient::describeDBProxyTargetGroups);

        if (response != null && response.targetGroups() != null && response.targetGroups().size() == 1) {
            ResourceModel resourceModel = Utility.resultToModel(response.targetGroups().get(0));

            DescribeDbProxyTargetsRequest targetsRequest = DescribeDbProxyTargetsRequest.builder()
                                                                         .dbProxyName(proxyName)
                                                                         .targetGroupName(targetGroupName)
                                                                         .build();
            final DescribeDbProxyTargetsResponse targetsResponse = clientProxy.injectCredentialsAndInvokeV2(targetsRequest, rdsClient::describeDBProxyTargets);
            if (targetsResponse != null) {
                List<String> dbClusters = new ArrayList<>();
                List<String> dbInstances = new ArrayList<>();
                for (DBProxyTarget target: targetsResponse.targets()) {
                    if (target.type().toString().equals("TRACKED_CLUSTER")) {
                        dbClusters.add(target.rdsResourceId());
                    } else {
                        dbInstances.add(target.rdsResourceId());
                    }
                }

                if (dbClusters.size() > 0) {
                    resourceModel.setDBClusterIdentifiers(dbClusters);
                } else if (dbInstances.size() > 0){
                    resourceModel.setDBInstanceIdentifiers(dbInstances);
                }
            }
            return resourceModel;
        } else {
            String name = String.format("%s:%s", proxyName, targetGroupName);
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, name);
        }
    }
}
