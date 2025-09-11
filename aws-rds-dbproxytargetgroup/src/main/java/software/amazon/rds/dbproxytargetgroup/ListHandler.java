package software.amazon.rds.dbproxytargetgroup;

import java.util.List;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetGroupsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ListHandler extends BaseHandler<CallbackContext> {
    private static final int MAX_RESULTS = 100;
    private AmazonWebServicesClientProxy clientProxy;
    private RdsClient rdsClient;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        clientProxy = proxy;
        rdsClient = RdsClient.create();

        final List<ResourceModel> models = listProxyTargetGroups(
                request.getDesiredResourceState().getDBProxyName(),
                request.getNextToken());

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                       .resourceModels(models)
                       .status(OperationStatus.SUCCESS)
                       .build();
    }

    private List<ResourceModel> listProxyTargetGroups(String dbProxyName, String nextToken) {
        DescribeDbProxyTargetGroupsRequest request = DescribeDbProxyTargetGroupsRequest.builder()
                .dbProxyName(dbProxyName)
                .maxRecords(MAX_RESULTS)
                .marker(nextToken)
                .build();

        DescribeDbProxyTargetGroupsResponse response = clientProxy.injectCredentialsAndInvokeV2(request, rdsClient::describeDBProxyTargetGroups);

        List<ResourceModel> models = response.targetGroups().stream().map(r -> Utility.resultToModel(r)).collect(Collectors.toList());

        return models;
    }
}
