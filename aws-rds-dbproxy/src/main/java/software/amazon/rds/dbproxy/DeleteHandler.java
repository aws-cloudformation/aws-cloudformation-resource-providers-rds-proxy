package software.amazon.rds.dbproxy;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBProxy;
import software.amazon.awssdk.services.rds.model.DbProxyNotFoundException;
import software.amazon.awssdk.services.rds.model.DeleteDbProxyRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbProxyResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbProxiesRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandler<CallbackContext> {
    private static final String TIMED_OUT_MESSAGE = "Timed out waiting for proxy to terminate.";

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

        // This Lambda will continually be re-invoked with the current state of the proxy, finally succeeding when deleted.
        return deleteProxyAndUpdateProgress(model, currentContext);
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteProxyAndUpdateProgress(ResourceModel model,
                                                                                       CallbackContext callbackContext) {
        if (callbackContext.getStabilizationRetriesRemaining() == 0) {
            throw new RuntimeException(TIMED_OUT_MESSAGE);
        }

        if (callbackContext.getProxy() == null) {
            try {
                return ProgressEvent.<ResourceModel, CallbackContext>builder()
                           .resourceModel(model)
                           .status(OperationStatus.IN_PROGRESS)
                           .callbackContext(CallbackContext.builder()
                                                           .proxy(deleteProxy(model.getDBProxyName()))
                                                           .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                           .build())
                           .build();
            } catch (DbProxyNotFoundException e) {
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
                                                           .proxy(callbackContext.getProxy())
                                                           .deleted(!doesProxyExist(model.getDBProxyName()))
                                                           .stabilizationRetriesRemaining(callbackContext.getStabilizationRetriesRemaining() - 1)
                                                           .build())
                           .build();
        }
    }

    private DBProxy deleteProxy(String proxyName) {
        DeleteDbProxyRequest request = DeleteDbProxyRequest.builder().dbProxyName(proxyName).build();

        DeleteDbProxyResponse response = clientProxy.injectCredentialsAndInvokeV2(request, rdsClient::deleteDBProxy);
        return response.dbProxy();
    }

    private boolean doesProxyExist(String proxyName) {
        DescribeDbProxiesRequest describeDBProxiesRequest = DescribeDbProxiesRequest.builder().dbProxyName(proxyName).build();
        try {
            clientProxy.injectCredentialsAndInvokeV2(describeDBProxiesRequest, rdsClient::describeDBProxies);
            return true;
        } catch (DbProxyNotFoundException e) {
            return false;
        }
    }
}
