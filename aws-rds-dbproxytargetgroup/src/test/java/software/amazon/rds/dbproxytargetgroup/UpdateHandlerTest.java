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
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.rds.model.ConnectionPoolConfigurationInfo;
import software.amazon.awssdk.services.rds.model.DBProxy;
import software.amazon.awssdk.services.rds.model.DBProxyTarget;
import software.amazon.awssdk.services.rds.model.DBProxyTargetGroup;
import software.amazon.awssdk.services.rds.model.DeregisterDbProxyTargetsRequest;
import software.amazon.awssdk.services.rds.model.DeregisterDbProxyTargetsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetGroupsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetsResponse;
import software.amazon.awssdk.services.rds.model.ModifyDbProxyTargetGroupRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbProxyTargetGroupResponse;
import software.amazon.awssdk.services.rds.model.RegisterDbProxyTargetsRequest;
import software.amazon.awssdk.services.rds.model.RegisterDbProxyTargetsResponse;
import software.amazon.awssdk.services.rds.model.TargetHealth;
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
        DBProxyTargetGroup defaultTargetGroup = DBProxyTargetGroup.builder().build();
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
    public void testModifyTargetGroup() {
        int connectionBorrowTimeout = 1;
        int maxConnectionsPercent = 25;
        int maxIdleConnectionsPercent = 50;
        int newMaxIdleConnectionsPercent = 40;
        String initQuery = "initQuery";
        String sessionPinningFilters = "sessionPinningFilters";

        DBProxy dbProxy = DBProxy.builder().status("available").build();
        ConnectionPoolConfigurationInfo connectionPoolConfigurationInfo = ConnectionPoolConfigurationInfo.builder()
                                                                                                                   .connectionBorrowTimeout(connectionBorrowTimeout)
                                                                                                                   .maxConnectionsPercent(maxConnectionsPercent)
                                                                                                                   .maxIdleConnectionsPercent(newMaxIdleConnectionsPercent)
                                                                                                                   .initQuery(initQuery)
                                                                                                                   .sessionPinningFilters(sessionPinningFilters)
                .build();

        DBProxyTargetGroup dbProxyTargetGroup = DBProxyTargetGroup.builder().connectionPoolConfig(connectionPoolConfigurationInfo).build();
        doReturn(ModifyDbProxyTargetGroupResponse.builder().dbProxyTargetGroup(dbProxyTargetGroup).build()).when(proxy)
                .injectCredentialsAndInvokeV2(any(ModifyDbProxyTargetGroupRequest.class),
                        ArgumentMatchers.<Function<ModifyDbProxyTargetGroupRequest, ModifyDbProxyTargetGroupResponse>>any());


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
    public void testModifyNoConnectionPoolConfig() {
        DBProxy dbProxy = DBProxy.builder().status("available").build();
        ImmutableList<String> clusterId = ImmutableList.of("clusterId");
        DBProxyTargetGroup dbProxyTargetGroup = DBProxyTargetGroup.builder().build();
        doReturn(DescribeDbProxyTargetGroupsResponse.builder().targetGroups(dbProxyTargetGroup).build()).when(proxy)
                .injectCredentialsAndInvokeV2(any(DescribeDbProxyTargetGroupsRequest.class),
                        ArgumentMatchers.<Function<DescribeDbProxyTargetGroupsRequest, DescribeDbProxyTargetGroupsResponse>>any());
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

        DBProxy dbProxy = DBProxy.builder().status("available").build();

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
                                                                    .targetGroupStatus(DBProxyTargetGroup.builder().build())
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
    public void testRegister() {
        DBProxy dbProxy = DBProxy.builder().status("available").build();
        DBProxyTargetGroup dbProxyTargetGroup = DBProxyTargetGroup.builder().build();
        DBProxyTarget dbProxyTarget = DBProxyTarget.builder().build();
        doReturn(RegisterDbProxyTargetsResponse.builder().dbProxyTargets(dbProxyTarget).build()).when(proxy)
                .injectCredentialsAndInvokeV2(any(RegisterDbProxyTargetsRequest.class),
                        ArgumentMatchers.<Function<RegisterDbProxyTargetsRequest, RegisterDbProxyTargetsResponse>>any());
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
    public void testDeregister() {
        DBProxyTargetGroup dbProxyTargetGroup = DBProxyTargetGroup.builder().build();
        doReturn(DeregisterDbProxyTargetsResponse.builder().build()).when(proxy).injectCredentialsAndInvokeV2(any(DeregisterDbProxyTargetsRequest.class),
                ArgumentMatchers.<Function<DeregisterDbProxyTargetsRequest, DeregisterDbProxyTargetsResponse>>any());
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
    public void testTargetHealth() {
        DBProxyTargetGroup defaultTargetGroup = DBProxyTargetGroup.builder().build();
        DBProxyTarget dbProxyTarget = DBProxyTarget.builder().rdsResourceId("resourceId").build();
        List<DBProxyTarget> targetList = ImmutableList.of(dbProxyTarget);

        DBProxyTarget target = DBProxyTarget.builder()
                                       .rdsResourceId("resourceId")
                                       .type("RDS_INSTANCE")
                                       .targetHealth(TargetHealth.builder().state(Constants.AVAILABLE_STATE).build())
                .build();

        doReturn(DescribeDbProxyTargetsResponse.builder().targets(target).build()).when(proxy).injectCredentialsAndInvokeV2(any(DescribeDbProxyTargetsRequest.class),
                ArgumentMatchers.<Function<DescribeDbProxyTargetsRequest, DescribeDbProxyTargetsResponse>>any());
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
