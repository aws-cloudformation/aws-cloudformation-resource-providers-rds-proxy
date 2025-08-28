package software.amazon.rds.dbproxyendpoint;

import java.util.List;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyEndpointsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyEndpointsResponse;
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

        final List<ResourceModel> models = listProxyEndpoints(request.getNextToken());

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(models)
                .status(OperationStatus.SUCCESS)
                .build();
    }

    private List<ResourceModel> listProxyEndpoints(String nextToken) {
        DescribeDbProxyEndpointsRequest request = DescribeDbProxyEndpointsRequest.builder().maxRecords(MAX_RESULTS).marker(nextToken).build();

        DescribeDbProxyEndpointsResponse response = clientProxy.injectCredentialsAndInvokeV2(request, rdsClient::describeDBProxyEndpoints);

        return response.dbProxyEndpoints().stream().map(Utility::resultToModel).collect(Collectors.toList());
    }
}
