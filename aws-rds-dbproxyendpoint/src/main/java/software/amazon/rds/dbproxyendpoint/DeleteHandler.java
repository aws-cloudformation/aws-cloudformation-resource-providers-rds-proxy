package software.amazon.rds.dbproxyendpoint;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.DBProxyEndpoint;
import com.amazonaws.services.rds.model.DBProxyEndpointNotFoundException;
import com.amazonaws.services.rds.model.DeleteDBProxyEndpointRequest;
import com.amazonaws.services.rds.model.DeleteDBProxyEndpointResult;
import com.amazonaws.services.rds.model.DescribeDBProxyEndpointsRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandler<CallbackContext> {
    private static final String TIMED_OUT_MESSAGE = "Timed out waiting for proxyEndpoint to terminate.";

    private AmazonWebServicesClientProxy clientProxy;
    private AmazonRDS rdsClient;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        clientProxy = proxy;
        rdsClient = AmazonRDSClientBuilder.defaultClient();

        final CallbackContext currentContext = callbackContext == null ?
                CallbackContext.builder().stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES).build() :
                callbackContext;

        // This Lambda will continually be re-invoked with the current state of the proxyEndpoint, finally succeeding when deleted.
        return deleteProxyEndpointAndUpdateProgress(model, currentContext);
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteProxyEndpointAndUpdateProgress(ResourceModel model,
                                                                                       CallbackContext callbackContext) {
        if (callbackContext.getStabilizationRetriesRemaining() == 0) {
            throw new RuntimeException(TIMED_OUT_MESSAGE);
        }

        if (callbackContext.getProxyEndpoint() == null) {
            try {
                return ProgressEvent.<ResourceModel, CallbackContext>builder()
                        .resourceModel(model)
                        .status(OperationStatus.IN_PROGRESS)
                        .callbackContext(CallbackContext.builder()
                                .proxyEndpoint(deleteProxyEndpoint(model.getDBProxyEndpointName()))
                                .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                .build())
                        .build();
            } catch (DBProxyEndpointNotFoundException e) {
                return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.NotFound);
            }
        } else if (callbackContext.isDeleted()) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .status(OperationStatus.SUCCESS)
                    .build();
        } else {
            try {
                Thread.sleep(Constants.POLL_RETRY_DELAY_IN_MS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModel(model)
                    .status(OperationStatus.IN_PROGRESS)
                    .callbackContext(CallbackContext.builder()
                            .proxyEndpoint(callbackContext.getProxyEndpoint())
                            .deleted(!doesProxyEndpointExist(model.getDBProxyEndpointName()))
                            .stabilizationRetriesRemaining(callbackContext.getStabilizationRetriesRemaining() - 1)
                            .build())
                    .build();
        }
    }

    private DBProxyEndpoint deleteProxyEndpoint(String proxyEndpointName) {
        DeleteDBProxyEndpointRequest request = new DeleteDBProxyEndpointRequest().withDBProxyEndpointName(proxyEndpointName);

        DeleteDBProxyEndpointResult result = clientProxy.injectCredentialsAndInvoke(request, rdsClient::deleteDBProxyEndpoint);
        return result.getDBProxyEndpoint();
    }

    private boolean doesProxyEndpointExist(String proxyEndpointName) {
        DescribeDBProxyEndpointsRequest describeDBProxyEndpointsRequest;

        describeDBProxyEndpointsRequest = new DescribeDBProxyEndpointsRequest().withDBProxyEndpointName(proxyEndpointName);
        try {
            clientProxy.injectCredentialsAndInvoke(describeDBProxyEndpointsRequest, rdsClient::describeDBProxyEndpoints);
            return true;
        } catch (DBProxyEndpointNotFoundException e) {
            return false;
        }
    }
}
