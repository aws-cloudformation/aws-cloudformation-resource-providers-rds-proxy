package software.amazon.rds.dbproxytargetgroup;

import com.amazonaws.services.rds.model.ConnectionPoolConfigurationInfo;
import com.amazonaws.services.rds.model.DBProxy;
import com.amazonaws.services.rds.model.DBProxyTarget;
import com.amazonaws.services.rds.model.DBProxyTargetGroup;
import com.amazonaws.services.rds.model.DescribeDBProxiesRequest;
import com.amazonaws.services.rds.model.DescribeDBProxiesResult;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetGroupsRequest;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetGroupsResult;
import com.amazonaws.services.rds.model.ModifyDBProxyTargetGroupRequest;
import com.amazonaws.services.rds.model.ModifyDBProxyTargetGroupResult;
import com.amazonaws.services.rds.model.RegisterDBProxyTargetsRequest;
import com.amazonaws.services.rds.model.RegisterDBProxyTargetsResult;
import com.google.common.collect.ImmutableList;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private Logger logger;

    @BeforeEach
    public void setup() {
        proxy = mock(AmazonWebServicesClientProxy.class);
        logger = mock(Logger.class);
    }


    @Test
    public void handleRequest_SimpleSuccess() {
        DBProxy dbProxy = new DBProxy().withStatus("available");
        DBProxyTargetGroup dbProxyTargetGroup = new DBProxyTargetGroup();
        List<DBProxyTarget> proxyTargets = new ArrayList<>();

        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        final CallbackContext context = CallbackContext.builder()
                                                       .proxy(dbProxy)
                                                       .targetGroupStatus(dbProxyTargetGroup)
                                                       .targets(proxyTargets)
                                                       .stabilizationRetriesRemaining(1)
                                                       .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testModifyNoConnectionPoolConfig() {
        DBProxy dbProxy = new DBProxy().withStatus("available");
        ImmutableList<String> clusterId = ImmutableList.of("clusterId");
        DBProxyTargetGroup dbProxyTargetGroup = new DBProxyTargetGroup();
        doReturn(new DescribeDBProxyTargetGroupsResult().withTargetGroups(dbProxyTargetGroup)).when(proxy).injectCredentialsAndInvoke(any(DescribeDBProxyTargetGroupsRequest.class), any());
        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder().clusterIdentifiers(clusterId).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        final CallbackContext context = CallbackContext.builder()
                                                       .proxy(dbProxy)
                                                       .stabilizationRetriesRemaining(1)
                                                       .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                                    .proxy(dbProxy)
                                                                    .targetGroupStatus(dbProxyTargetGroup)
                                                                    .build();
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testModifyConnectionConfigProxy() {
        int connectionBorrowTimeout = 1;
        int maxConnectionsPercent = 25;
        int maxIdleConnectionsPercent = 50;
        String initQuery = "initQuery";
        String sessionPinningFilters = "sessionPinningFilters";

        DBProxy dbProxy = new DBProxy().withStatus("available");
        ConnectionPoolConfigurationInfo connectionPoolConfigurationInfo = new ConnectionPoolConfigurationInfo()
                                                                                  .withConnectionBorrowTimeout(connectionBorrowTimeout)
                                                                                  .withMaxConnectionsPercent(maxConnectionsPercent)
                                                                                  .withMaxIdleConnectionsPercent(maxIdleConnectionsPercent)
                                                                                  .withInitQuery(initQuery)
                                                                                  .withSessionPinningFilters(sessionPinningFilters);

        DBProxyTargetGroup dbProxyTargetGroup = new DBProxyTargetGroup().withConnectionPoolConfig(connectionPoolConfigurationInfo);
        doReturn(new ModifyDBProxyTargetGroupResult().withDBProxyTargetGroup(dbProxyTargetGroup)).when(proxy).injectCredentialsAndInvoke(any(ModifyDBProxyTargetGroupRequest.class), any());
        final CreateHandler handler = new CreateHandler();

        ConnectionPoolConfigurationInfoFormat connectionPoolConfigurationInfo1 =
                ConnectionPoolConfigurationInfoFormat
                        .builder()
                        .maxConnectionsPercent(maxConnectionsPercent)
                        .maxIdleConnectionsPercent(maxIdleConnectionsPercent)
                        .connectionBorrowTimeout(connectionBorrowTimeout)
                        .sessionPinningFilters(ImmutableList.of(sessionPinningFilters))
                        .initQuery(initQuery)
                        .build();

        final ResourceModel model = ResourceModel.builder().connectionPoolConfigurationInfo(connectionPoolConfigurationInfo1).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        final CallbackContext context = CallbackContext.builder()
                                                       .proxy(dbProxy)
                                                       .stabilizationRetriesRemaining(1)
                                                       .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                                    .proxy(dbProxy)
                                                                    .targetGroupStatus(dbProxyTargetGroup)
                                                                    .build();
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testRegistration(){
        DBProxy dbProxy = new DBProxy().withStatus("available");

        DBProxyTargetGroup dbProxyTargetGroup = new DBProxyTargetGroup();
        DBProxyTarget dbProxyTarget = new DBProxyTarget();
        doReturn(new RegisterDBProxyTargetsResult().withDBProxyTargets(dbProxyTarget)).when(proxy).injectCredentialsAndInvoke(any(RegisterDBProxyTargetsRequest.class), any());
        final CreateHandler handler = new CreateHandler();

        ImmutableList<String> clusterId = ImmutableList.of("clusterId");

        final ResourceModel model = ResourceModel.builder().clusterIdentifiers(clusterId).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        final CallbackContext context = CallbackContext.builder()
                                                       .proxy(dbProxy)
                                                       .targetGroupStatus(dbProxyTargetGroup)
                                                       .stabilizationRetriesRemaining(1)
                                                       .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                                    .proxy(dbProxy)
                                                                    .targetGroupStatus(dbProxyTargetGroup)
                                                                    .targets(ImmutableList.of(dbProxyTarget))
                                                                    .build();
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testProxyState() {
        DBProxy dbProxy = new DBProxy().withStatus("available");
        doReturn(new DescribeDBProxiesResult().withDBProxies(dbProxy)).when(proxy).injectCredentialsAndInvoke(any(DescribeDBProxiesRequest.class), any());

        final CreateHandler handler = new CreateHandler();
        ImmutableList<String> clusterId = ImmutableList.of("clusterId");
        final ResourceModel model = ResourceModel.builder().clusterIdentifiers(clusterId).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES - 1)
                                                                    .proxy(dbProxy)
                                                                    .build();

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testStabilizationTimeout() {
        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        final CallbackContext context = CallbackContext.builder()
                                                       .stabilizationRetriesRemaining(0)
                                                       .proxy(new DBProxy().withStatus("creating"))
                                                       .build();

        try {
            handler.handleRequest(proxy, request, context, logger);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).isEqualTo(CreateHandler.TIMED_OUT_MESSAGE);
        }
    }
}