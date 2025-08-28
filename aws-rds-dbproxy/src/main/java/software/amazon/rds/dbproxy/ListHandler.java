package software.amazon.rds.dbproxy;

import java.util.List;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbProxiesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxiesResponse;
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

        final List<ResourceModel> models = listProxies(request.getNextToken());

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                       .resourceModels(models)
                       .status(OperationStatus.SUCCESS)
                       .build();
    }

    private List<ResourceModel> listProxies(String nextToken) {
        DescribeDbProxiesRequest request = DescribeDbProxiesRequest.builder()
                .maxRecords(MAX_RESULTS)
                .marker(nextToken)
                .build();

        DescribeDbProxiesResponse response = clientProxy.injectCredentialsAndInvokeV2(request, rdsClient::describeDBProxies);

        return response.dbProxies().stream().map(r -> Utility.resultToModel(r)).collect(Collectors.toList());
    }
}
