package software.amazon.rds.dbproxy;

import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.DescribeDBProxiesRequest;
import com.amazonaws.services.rds.model.DescribeDBProxiesResult;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ListHandler extends BaseHandler<CallbackContext> {

    private static final int MAX_RESULTS = 100;

    private AmazonWebServicesClientProxy clientProxy;
    private AmazonRDS rdsClient;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        clientProxy = proxy;
        rdsClient = AmazonRDSClientBuilder.defaultClient();

        final List<ResourceModel> models = listProxies(request.getNextToken());

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                       .resourceModels(models)
                       .status(OperationStatus.SUCCESS)
                       .build();
    }

    private List<ResourceModel> listProxies(String nextToken) {
        DescribeDBProxiesRequest request = new DescribeDBProxiesRequest().withMaxRecords(MAX_RESULTS).withMarker(nextToken);

        DescribeDBProxiesResult result = clientProxy.injectCredentialsAndInvoke(request, rdsClient::describeDBProxies);

        List<ResourceModel> models = result.getDBProxies().stream().map(r -> Utility.resultToModel(r)).collect(Collectors.toList());

        return models;
    }
}
