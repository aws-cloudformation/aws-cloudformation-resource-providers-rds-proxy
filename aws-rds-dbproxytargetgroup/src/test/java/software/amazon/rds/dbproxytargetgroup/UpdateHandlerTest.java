package software.amazon.rds.dbproxytargetgroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.amazonaws.services.rds.model.ConnectionPoolConfigurationInfo;
import com.amazonaws.services.rds.model.DBProxy;
import com.amazonaws.services.rds.model.DBProxyTarget;
import com.amazonaws.services.rds.model.DBProxyTargetGroup;
import com.amazonaws.services.rds.model.DeregisterDBProxyTargetsRequest;
import com.amazonaws.services.rds.model.DeregisterDBProxyTargetsResult;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetGroupsRequest;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetGroupsResult;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetsRequest;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetsResult;
import com.amazonaws.services.rds.model.ModifyDBProxyTargetGroupRequest;
import com.amazonaws.services.rds.model.ModifyDBProxyTargetGroupResult;
import com.amazonaws.services.rds.model.RegisterDBProxyTargetsRequest;
import com.amazonaws.services.rds.model.RegisterDBProxyTargetsResult;
import com.amazonaws.services.rds.model.TargetHealth;
import com.google.common.collect.ImmutableList;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest {

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
        DBProxyTargetGroup defaultTargetGroup = new DBProxyTargetGroup();
        List<DBProxyTarget> targetList = new ArrayList<>();
        final CallbackContext context = CallbackContext.builder()
                                                       .targetGroupStatus(defaultTargetGroup)
                                                       .targets(targetList)
                                                       .targetsDeregistered(true)
                                                       .stabilizationRetriesRemaining(1)
                                                       .allTargetsHealthy(true)
                                                       .build();

        final UpdateHandler handler = new UpdateHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .previousResourceState(model)
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
    @SuppressWarnings("unchecked")
    public void testModifyTargetGroup() {
        int connectionBorrowTimeout = 1;
        int maxConnectionsPercent = 25;
        int maxIdleConnectionsPercent = 50;
        int newMaxIdleConnectionsPercent = 40;
        String initQuery = "initQuery";
        String sessionPinningFilters = "sessionPinningFilters";

        DBProxy dbProxy = new DBProxy().withStatus("available");
        com.amazonaws.services.rds.model.ConnectionPoolConfigurationInfo connectionPoolConfigurationInfo = new ConnectionPoolConfigurationInfo()
                                                                                                                   .withConnectionBorrowTimeout(connectionBorrowTimeout)
                                                                                                                   .withMaxConnectionsPercent(maxConnectionsPercent)
                                                                                                                   .withMaxIdleConnectionsPercent(newMaxIdleConnectionsPercent)
                                                                                                                   .withInitQuery(initQuery)
                                                                                                                   .withSessionPinningFilters(sessionPinningFilters);

        DBProxyTargetGroup dbProxyTargetGroup = new DBProxyTargetGroup().withConnectionPoolConfig(connectionPoolConfigurationInfo);
        doReturn(new ModifyDBProxyTargetGroupResult().withDBProxyTargetGroup(dbProxyTargetGroup)).when(proxy)
                .injectCredentialsAndInvoke(any(ModifyDBProxyTargetGroupRequest.class), any(Function.class));


        final CallbackContext context = CallbackContext.builder()
                                                       .stabilizationRetriesRemaining(1)
                                                       .proxy(dbProxy)
                                                       .build();

        final UpdateHandler handler = new UpdateHandler();

        ConnectionPoolConfigurationInfoFormat connectionPoolConfigurationInfo1 =
                ConnectionPoolConfigurationInfoFormat
                        .builder()
                        .maxConnectionsPercent(maxConnectionsPercent)
                        .maxIdleConnectionsPercent(maxIdleConnectionsPercent)
                        .connectionBorrowTimeout(connectionBorrowTimeout)
                        .sessionPinningFilters(ImmutableList.of(sessionPinningFilters))
                        .initQuery(initQuery)
                        .build();

        ConnectionPoolConfigurationInfoFormat connectionPoolConfigurationInfo2 =
                ConnectionPoolConfigurationInfoFormat
                        .builder()
                        .maxConnectionsPercent(maxConnectionsPercent)
                        .maxIdleConnectionsPercent(newMaxIdleConnectionsPercent)
                        .connectionBorrowTimeout(connectionBorrowTimeout)
                        .sessionPinningFilters(ImmutableList.of(sessionPinningFilters))
                        .initQuery(initQuery)
                        .build();


        final ResourceModel oldModel = ResourceModel.builder().connectionPoolConfigurationInfo(connectionPoolConfigurationInfo1).build();
        final ResourceModel desiredModel = ResourceModel.builder().connectionPoolConfigurationInfo(connectionPoolConfigurationInfo2).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(desiredModel)
                                                                      .previousResourceState(oldModel)
                                                                      .build();

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                                    .targetGroupStatus(dbProxyTargetGroup)
                                                                    .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testModifyNoConnectionPoolConfig() {
        DBProxy dbProxy = new DBProxy().withStatus("available");
        ImmutableList<String> clusterId = ImmutableList.of("clusterId");
        DBProxyTargetGroup dbProxyTargetGroup = new DBProxyTargetGroup();
        doReturn(new DescribeDBProxyTargetGroupsResult().withTargetGroups(dbProxyTargetGroup)).when(proxy)
                .injectCredentialsAndInvoke(any(DescribeDBProxyTargetGroupsRequest.class), any(Function.class));
        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder().dBClusterIdentifiers(clusterId).build();

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
    public void testModifyTargetGroup_noChanges() {
        int connectionBorrowTimeout = 1;
        int maxConnectionsPercent = 25;
        int maxIdleConnectionsPercent = 50;
        String initQuery = "initQuery";
        String sessionPinningFilters = "sessionPinningFilters";

        DBProxy dbProxy = new DBProxy().withStatus("available");

        final CallbackContext context = CallbackContext.builder()
                                                       .stabilizationRetriesRemaining(1)
                                                       .proxy(dbProxy)
                                                       .build();

        final UpdateHandler handler = new UpdateHandler();

        ConnectionPoolConfigurationInfoFormat connectionPoolConfigurationInfo1 =
                ConnectionPoolConfigurationInfoFormat
                        .builder()
                        .maxConnectionsPercent(maxConnectionsPercent)
                        .maxIdleConnectionsPercent(maxIdleConnectionsPercent)
                        .connectionBorrowTimeout(connectionBorrowTimeout)
                        .sessionPinningFilters(ImmutableList.of(sessionPinningFilters))
                        .initQuery(initQuery)
                        .build();

        final ResourceModel oldModel = ResourceModel.builder().connectionPoolConfigurationInfo(connectionPoolConfigurationInfo1).build();
        final ResourceModel desiredModel = ResourceModel.builder().connectionPoolConfigurationInfo(connectionPoolConfigurationInfo1).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(desiredModel)
                                                                      .previousResourceState(oldModel)
                                                                      .build();

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                                    .targetGroupStatus(new DBProxyTargetGroup())
                                                                    .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRegister() {
        DBProxy dbProxy = new DBProxy().withStatus("available");
        DBProxyTargetGroup dbProxyTargetGroup = new DBProxyTargetGroup();
        DBProxyTarget dbProxyTarget = new DBProxyTarget();
        doReturn(new RegisterDBProxyTargetsResult().withDBProxyTargets(dbProxyTarget)).when(proxy)
                .injectCredentialsAndInvoke(any(RegisterDBProxyTargetsRequest.class), any(Function.class));
        final UpdateHandler handler = new UpdateHandler();

        ImmutableList<String> clusterId = ImmutableList.of("clusterId");

        final ResourceModel desiredModel = ResourceModel.builder().dBClusterIdentifiers(clusterId).build();
        final ResourceModel oldModel = ResourceModel.builder().build();


        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(desiredModel)
                                                                      .previousResourceState(oldModel)
                                                                      .build();

        final CallbackContext context = CallbackContext.builder()
                                                       .proxy(dbProxy)
                                                       .targetGroupStatus(dbProxyTargetGroup)
                                                       .targetsDeregistered(true)
                                                       .stabilizationRetriesRemaining(1)
                                                       .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                                    .targetGroupStatus(dbProxyTargetGroup)
                                                                    .targetsDeregistered(true)
                                                                    .targets(ImmutableList.of(dbProxyTarget))
                                                                    .build();
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDeregister() {
        DBProxyTargetGroup dbProxyTargetGroup = new DBProxyTargetGroup();
        doReturn(new DeregisterDBProxyTargetsResult()).when(proxy).injectCredentialsAndInvoke(any(DeregisterDBProxyTargetsRequest.class), any(Function.class));
        final UpdateHandler handler = new UpdateHandler();

        ImmutableList<String> instanceId = ImmutableList.of("instanceId");

        final ResourceModel desiredModel = ResourceModel.builder().build();
        final ResourceModel oldModel = ResourceModel.builder().dBInstanceIdentifiers(instanceId).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(desiredModel)
                                                                      .previousResourceState(oldModel)
                                                                      .build();

        final CallbackContext context = CallbackContext.builder()
                                                       .targetGroupStatus(dbProxyTargetGroup)
                                                       .stabilizationRetriesRemaining(1)
                                                       .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                                    .targetGroupStatus(dbProxyTargetGroup)
                                                                    .targetsDeregistered(true)
                                                                    .build();
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTargetHealth() {
        DBProxyTargetGroup defaultTargetGroup = new DBProxyTargetGroup();
        DBProxyTarget dbProxyTarget = new DBProxyTarget().withRdsResourceId("resourceId");
        List<DBProxyTarget> targetList = ImmutableList.of(dbProxyTarget);

        DBProxyTarget target = new DBProxyTarget()
                                       .withRdsResourceId("resourceId")
                                       .withType("RDS_INSTANCE")
                                       .withTargetHealth(new TargetHealth().withState(Constants.AVAILABLE_STATE));

        doReturn(new DescribeDBProxyTargetsResult().withTargets(target)).when(proxy).injectCredentialsAndInvoke(any(DescribeDBProxyTargetsRequest.class), any(Function.class));
        final CallbackContext context = CallbackContext.builder()
                                                       .targetGroupStatus(defaultTargetGroup)
                                                       .targets(targetList)
                                                       .targetsDeregistered(true)
                                                       .stabilizationRetriesRemaining(1)
                                                       .build();

        final UpdateHandler handler = new UpdateHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .previousResourceState(model)
                                                                      .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(0)
                                                                    .targetGroupStatus(defaultTargetGroup)
                                                                    .targetsDeregistered(true)
                                                                    .targets(ImmutableList.of(dbProxyTarget))
                                                                    .allTargetsHealthy(true)
                                                                    .build();
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }
}
