package software.amazon.rds.dbproxytargetgroup;

import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetGroupsRequest;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetGroupsResult;
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

        final List<ResourceModel> models = listProxyTargetGroups(request.getNextToken());

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                       .resourceModels(models)
                       .status(OperationStatus.SUCCESS)
                       .build();
    }

    private List<ResourceModel> listProxyTargetGroups(String nextToken) {
        DescribeDBProxyTargetGroupsRequest request = new DescribeDBProxyTargetGroupsRequest().withMaxRecords(MAX_RESULTS).withMarker(nextToken);

        DescribeDBProxyTargetGroupsResult result = clientProxy.injectCredentialsAndInvoke(request, rdsClient::describeDBProxyTargetGroups);

        List<ResourceModel> models = result.getTargetGroups().stream().map(r -> Utility.resultToModel(r)).collect(Collectors.toList());

        return models;
    }
}
