package software.amazon.rds.dbproxyendpoint;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBProxyEndpoint;
import software.amazon.awssdk.services.rds.model.DbProxyEndpointNotFoundException;
import software.amazon.awssdk.services.rds.model.DeleteDbProxyEndpointRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbProxyEndpointResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyEndpointsRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandler<CallbackContext> {
    private static final String TIMED_OUT_MESSAGE = "Timed out waiting for proxyEndpoint to terminate.";

    private AmazonWebServicesClientProxy clientProxy;
    private RdsClient rdsClient;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        clientProxy = proxy;
        rdsClient = RdsClient.create();

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
            } catch (DbProxyEndpointNotFoundException e) {
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
        DeleteDbProxyEndpointRequest request = DeleteDbProxyEndpointRequest.builder().dbProxyEndpointName(proxyEndpointName).build();

        DeleteDbProxyEndpointResponse response = clientProxy.injectCredentialsAndInvokeV2(request, rdsClient::deleteDBProxyEndpoint);
        return response.dbProxyEndpoint();
    }

    private boolean doesProxyEndpointExist(String proxyEndpointName) {

        DescribeDbProxyEndpointsRequest describeDBProxyEndpointsRequest = DescribeDbProxyEndpointsRequest.builder().dbProxyEndpointName(proxyEndpointName).build();
        try {
            clientProxy.injectCredentialsAndInvokeV2(describeDBProxyEndpointsRequest, rdsClient::describeDBProxyEndpoints);
            return true;
        } catch (DbProxyEndpointNotFoundException e) {
            return false;
        }
    }
}
