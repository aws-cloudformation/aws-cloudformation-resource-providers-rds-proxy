package software.amazon.rds.dbproxy;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.CreateDBProxyRequest;
import com.amazonaws.services.rds.model.CreateDBProxyResult;
import com.amazonaws.services.rds.model.DBProxy;
import com.amazonaws.services.rds.model.DBProxyAlreadyExistsException;
import com.amazonaws.services.rds.model.DescribeDBProxiesRequest;
import com.amazonaws.services.rds.model.DescribeDBProxiesResult;
import com.amazonaws.services.rds.model.Tag;
import com.amazonaws.services.rds.model.UserAuthConfig;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class CreateHandler extends BaseHandler<CallbackContext> {
    public static final String TIMED_OUT_MESSAGE = "Timed out waiting for proxy to become available.";

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
            } catch (DBProxyAlreadyExistsException e) {
                return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.AlreadyExists);
            }
        } else if (proxyStateSoFar.getStatus().equals(Constants.AVAILABLE_PROXY_STATE)) {
            model.setDBProxyArn(proxyStateSoFar.getDBProxyArn());
            model.setEndpoint(proxyStateSoFar.getEndpoint());
            model.setVpcId(proxyStateSoFar.getVpcId());
            model.setDebugLogging(proxyStateSoFar.getDebugLogging());
            model.setIdleClientTimeout(proxyStateSoFar.getIdleClientTimeout());
            model.setRequireTLS(proxyStateSoFar.getRequireTLS());

            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                           .resourceModel(model)
                           .status(OperationStatus.SUCCESS)
                           .build();
        } else if (Constants.TERMINAL_FAILURE_STATES.contains(proxyStateSoFar.getStatus())) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                           .status(OperationStatus.FAILED)
                           .errorCode(HandlerErrorCode.NotFound)
                           .build();
        } else {
            model.setDBProxyArn(proxyStateSoFar.getDBProxyArn());
            model.setEndpoint(proxyStateSoFar.getEndpoint());
            model.setVpcId(proxyStateSoFar.getVpcId());
            model.setDebugLogging(proxyStateSoFar.getDebugLogging());
            model.setIdleClientTimeout(proxyStateSoFar.getIdleClientTimeout());
            model.setRequireTLS(proxyStateSoFar.getRequireTLS());

            try {
                Thread.sleep(Constants.POLL_RETRY_DELAY_IN_MS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            DBProxy proxy = updatedProxyProgress(proxyStateSoFar.getDBProxyName());
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

        CreateDBProxyRequest request = new CreateDBProxyRequest()
                                               .withAuth(userAuthConfig)
                                               .withDBProxyName(model.getDBProxyName())
                                               .withDebugLogging(model.getDebugLogging())
                                               .withEngineFamily(model.getEngineFamily())
                                               .withIdleClientTimeout(model.getIdleClientTimeout())
                                               .withRequireTLS(model.getRequireTLS())
                                               .withRoleArn(model.getRoleArn())
                                               .withVpcSecurityGroupIds(model.getVpcSecurityGroupIds())
                                               .withVpcSubnetIds(model.getVpcSubnetIds())
                                               .withTags(tags);

        CreateDBProxyResult result = clientProxy.injectCredentialsAndInvoke(request, rdsClient::createDBProxy);
        return result.getDBProxy();
    }

    private List<Tag> getTags(ResourceModel model) {
        List<Tag> tags = new ArrayList<>();

        if (model.getTags() == null) {
            return tags;
        }

        for(TagFormat tag : model.getTags()){
            Tag t = new Tag().withKey(tag.getKey()).withValue(tag.getValue());
            tags.add(t);
        }
        return tags;
    }

    private DBProxy updatedProxyProgress(String proxyName) {
        DescribeDBProxiesRequest describeDBProxiesRequest;
        DescribeDBProxiesResult describeDBProxiesResult;

        describeDBProxiesRequest = new DescribeDBProxiesRequest().withDBProxyName(proxyName);
        describeDBProxiesResult = clientProxy.injectCredentialsAndInvoke(describeDBProxiesRequest, rdsClient::describeDBProxies);
        return describeDBProxiesResult.getDBProxies()
                                      .stream()
                                      .findFirst()
                                      .orElse(new DBProxy());
    }
}
