package software.amazon.rds.dbproxy;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbProxyRequest;
import software.amazon.awssdk.services.rds.model.CreateDbProxyResponse;
import software.amazon.awssdk.services.rds.model.DBProxy;
import software.amazon.awssdk.services.rds.model.DbProxyAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DescribeDbProxiesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxiesResponse;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.awssdk.services.rds.model.UserAuthConfig;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class CreateHandler extends BaseHandler<CallbackContext> {
    public static final String TIMED_OUT_MESSAGE = "Timed out waiting for proxy to become available.";

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

        // This Lambda will continually be re-invoked with the current state of the proxy, finally succeeding when state stabilizes.
        return createProxyAndUpdateProgress(model, currentContext);
    }

    private ProgressEvent<ResourceModel, CallbackContext> createProxyAndUpdateProgress(ResourceModel model,
                                                                                       CallbackContext callbackContext) {
        // This Lambda will continually be re-invoked with the current state of the proxy, finally succeeding when state stabilizes.
        final DBProxy proxyStateSoFar = callbackContext.getProxy();

        if (callbackContext.getStabilizationRetriesRemaining() == 0) {
            throw new RuntimeException(TIMED_OUT_MESSAGE);
        }

        if (proxyStateSoFar == null) {
            try {
                return ProgressEvent.<ResourceModel, CallbackContext>builder()
                        .resourceModel(model)
                        .status(OperationStatus.IN_PROGRESS)
                        .callbackContext(CallbackContext.builder()
                                .proxy(createProxy(model))
                                .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                .build())
                        .build();
            } catch (DbProxyAlreadyExistsException e) {
                return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.AlreadyExists);
            }
        } else if (proxyStateSoFar.status().toString().equals(Constants.AVAILABLE_PROXY_STATE)) {
            model.setDBProxyArn(proxyStateSoFar.dbProxyArn());
            model.setEndpoint(proxyStateSoFar.endpoint());
            model.setVpcId(proxyStateSoFar.vpcId());
            model.setDebugLogging(proxyStateSoFar.debugLogging());
            model.setIdleClientTimeout(proxyStateSoFar.idleClientTimeout());
            model.setRequireTLS(proxyStateSoFar.requireTLS());

            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                           .resourceModel(model)
                           .status(OperationStatus.SUCCESS)
                           .build();
        } else if (Constants.TERMINAL_FAILURE_STATES.contains(proxyStateSoFar.status().toString())) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                           .status(OperationStatus.FAILED)
                           .errorCode(HandlerErrorCode.NotFound)
                           .build();
        } else {
            model.setDBProxyArn(proxyStateSoFar.dbProxyArn());
            model.setEndpoint(proxyStateSoFar.endpoint());
            model.setVpcId(proxyStateSoFar.vpcId());
            model.setDebugLogging(proxyStateSoFar.debugLogging());
            model.setIdleClientTimeout(proxyStateSoFar.idleClientTimeout());
            model.setRequireTLS(proxyStateSoFar.requireTLS());

            try {
                Thread.sleep(Constants.POLL_RETRY_DELAY_IN_MS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            DBProxy proxy = updatedProxyProgress(proxyStateSoFar.dbProxyName());
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                           .resourceModel(model)
                           .status(OperationStatus.IN_PROGRESS)
                           .callbackContext(CallbackContext.builder()
                                                           .proxy(proxy)
                                                           .stabilizationRetriesRemaining(callbackContext.getStabilizationRetriesRemaining() - 1)
                                                           .build())
                           .build();
        }
    }

    private DBProxy createProxy(ResourceModel model) {
        List<UserAuthConfig> userAuthConfig = Utility.getUserAuthConfigs(model);
        List<Tag> tags = getTags(model);

        CreateDbProxyRequest request = CreateDbProxyRequest.builder()
                .auth(userAuthConfig)
                .dbProxyName(model.getDBProxyName())
                .debugLogging(model.getDebugLogging())
                .engineFamily(model.getEngineFamily())
                .idleClientTimeout(model.getIdleClientTimeout())
                .requireTLS(model.getRequireTLS())
                .roleArn(model.getRoleArn())
                .vpcSecurityGroupIds(model.getVpcSecurityGroupIds())
                .vpcSubnetIds(model.getVpcSubnetIds())
                .tags(tags)
                .build();

        CreateDbProxyResponse response = clientProxy.injectCredentialsAndInvokeV2(request, rdsClient::createDBProxy);
        return response.dbProxy();
    }

    private List<Tag> getTags(ResourceModel model) {
        List<Tag> tags = new ArrayList<>();

        if (model.getTags() == null) {
            return tags;
        }

        for(TagFormat tag : model.getTags()){
            Tag t = Tag.builder()
                    .key(tag.getKey())
                    .value(tag.getValue())
                    .build();
            tags.add(t);
        }
        return tags;
    }

    private DBProxy updatedProxyProgress(String proxyName) {
        DescribeDbProxiesRequest describeDBProxiesRequest = DescribeDbProxiesRequest.builder()
                .dbProxyName(proxyName)
                .build();

        DescribeDbProxiesResponse describeDbProxiesResponse = clientProxy.injectCredentialsAndInvokeV2(describeDBProxiesRequest, rdsClient::describeDBProxies);
        return describeDbProxiesResponse.dbProxies()
                                      .stream()
                                      .findFirst()
                                      .orElse(DBProxy.builder().build());
    }
}
