package software.amazon.rds.dbproxy;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.DescribeDBProxiesRequest;
import com.amazonaws.services.rds.model.DescribeDBProxiesResult;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ReadHandler extends BaseHandler<CallbackContext> {
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

        final ResourceModel model = describeDBProxy(request.getDesiredResourceState().getDbProxyName());

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                       .resourceModel(model)
                       .status(OperationStatus.SUCCESS)
                       .build();
    }

    private ResourceModel describeDBProxy(final String proxyName) {
        DescribeDBProxiesRequest request = new DescribeDBProxiesRequest().withDBProxyName(proxyName);

        final DescribeDBProxiesResult result = clientProxy.injectCredentialsAndInvoke(request, rdsClient::describeDBProxies);
        if (result != null && result.getDBProxies() != null && result.getDBProxies().size() == 1) {
            return Utility.resultToModel(result.getDBProxies().get(0));
        } else {
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, proxyName);
        }
    }
}
