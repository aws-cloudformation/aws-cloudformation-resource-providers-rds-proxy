package software.amazon.rds.dbproxytargetgroup;

import java.util.Optional;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetGroupsRequest;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetGroupsResult;
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

        final ResourceModel desiredResource = request.getDesiredResourceState();

        String proxyName = desiredResource.getDbProxyName();
        String targetGroupName = Optional.ofNullable(desiredResource.getTargetGroupName()).orElse("default");

        clientProxy = proxy;
        rdsClient = AmazonRDSClientBuilder.defaultClient();

        final ResourceModel model = describeDBProxyTargetGroup(proxyName, targetGroupName);

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                       .resourceModel(model)
                       .status(OperationStatus.SUCCESS)
                       .build();
    }

    private ResourceModel describeDBProxyTargetGroup(final String proxyName,
                                                     final String targetGroupName) {
        DescribeDBProxyTargetGroupsRequest request = new DescribeDBProxyTargetGroupsRequest()
                                                             .withDBProxyName(proxyName)
                                                             .withTargetGroupName(targetGroupName);

        final DescribeDBProxyTargetGroupsResult result = clientProxy.injectCredentialsAndInvoke(request, rdsClient::describeDBProxyTargetGroups);

        if (result != null && result.getTargetGroups() != null && result.getTargetGroups().size() == 1) {
            return Utility.resultToModel(result.getTargetGroups().get(0));
        } else {
            String name = String.format("%s:%s", proxyName, targetGroupName);
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, name);
        }
    }
}
