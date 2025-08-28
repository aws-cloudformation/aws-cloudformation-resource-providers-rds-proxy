package software.amazon.rds.dbproxytargetgroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
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
import software.amazon.awssdk.services.rds.model.DbProxyNotFoundException;
import software.amazon.awssdk.services.rds.model.DbProxyTargetGroupNotFoundException;
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
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

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
        DBProxy dbProxy = DBProxy.builder().status("available").build();
        DBProxyTargetGroup dbProxyTargetGroup = DBProxyTargetGroup.builder().build();
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
                                                       .allTargetsHealthy(true)
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
    public void testModifyConnectionConfigProxy() {
        int connectionBorrowTimeout = 1;
        int maxConnectionsPercent = 25;
        int maxIdleConnectionsPercent = 50;
        String initQuery = "initQuery";
        String sessionPinningFilters = "sessionPinningFilters";

        DBProxy dbProxy = DBProxy.builder().status("available").build();
        ConnectionPoolConfigurationInfo connectionPoolConfigurationInfo = ConnectionPoolConfigurationInfo.builder()
                                                                                  .connectionBorrowTimeout(connectionBorrowTimeout)
                                                                                  .maxConnectionsPercent(maxConnectionsPercent)
                                                                                  .maxIdleConnectionsPercent(maxIdleConnectionsPercent)
                                                                                  .initQuery(initQuery)
                                                                                  .sessionPinningFilters(sessionPinningFilters)
                .build();

        DBProxyTargetGroup dbProxyTargetGroup = DBProxyTargetGroup.builder().connectionPoolConfig(connectionPoolConfigurationInfo).build();
        doReturn(ModifyDbProxyTargetGroupResponse.builder().dbProxyTargetGroup(dbProxyTargetGroup).build()).when(proxy)
                .injectCredentialsAndInvokeV2(any(ModifyDbProxyTargetGroupRequest.class),
                        ArgumentMatchers.<Function<ModifyDbProxyTargetGroupRequest, ModifyDbProxyTargetGroupResponse>>any());
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
    public void testCheckTargetHealth() {
        DBProxy dbProxy = DBProxy.builder().status("available").build();
        DBProxyTargetGroup dbProxyTargetGroup = DBProxyTargetGroup.builder().build();
        List<DBProxyTarget> proxyTargets = new ArrayList<>();
        DBProxyTarget target = DBProxyTarget.builder()
                                       .rdsResourceId("resourceId")
                                       .type("RDS_INSTANCE")
                                       .targetHealth(TargetHealth.builder().state(Constants.AVAILABLE_STATE).build())
                .build();

        doReturn(DescribeDbProxyTargetsResponse.builder().targets(target).build()).when(proxy).injectCredentialsAndInvokeV2(any(DescribeDbProxyTargetsRequest.class),
                ArgumentMatchers.<Function<DescribeDbProxyTargetsRequest, DescribeDbProxyTargetsResponse>>any());


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


        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(0)
                                                                    .proxy(dbProxy)
                                                                    .targetGroupStatus(dbProxyTargetGroup)
                                                                    .targets(proxyTargets)
                                                                    .allTargetsHealthy(true)
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
    public void testCheckTargetHealth_nullInstanceHealth() {
        DBProxy dbProxy = DBProxy.builder().status("available").build();
        DBProxyTargetGroup dbProxyTargetGroup = DBProxyTargetGroup.builder().build();
        List<DBProxyTarget> proxyTargets = new ArrayList<>();
        DBProxyTarget target = DBProxyTarget.builder()
                                       .rdsResourceId("resourceId")
                                       .type("RDS_INSTANCE")
                                       .build();

        doReturn(DescribeDbProxyTargetsResponse.builder().targets(target).build()).when(proxy).injectCredentialsAndInvokeV2(any(DescribeDbProxyTargetsRequest.class),
                ArgumentMatchers.<Function<DescribeDbProxyTargetsRequest, DescribeDbProxyTargetsResponse>>any());


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


        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(0)
                                                                    .proxy(dbProxy)
                                                                    .targetGroupStatus(dbProxyTargetGroup)
                                                                    .targets(proxyTargets)
                                                                    .allTargetsHealthy(false)
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
    public void testCheckTargetHealth_unhealthy() {
        DBProxy dbProxy = DBProxy.builder().status("available").build();
        DBProxyTargetGroup dbProxyTargetGroup = DBProxyTargetGroup.builder().build();
        List<DBProxyTarget> proxyTargets = new ArrayList<>();
        DBProxyTarget target = DBProxyTarget.builder()
                                       .rdsResourceId("resourceId")
                                       .type("RDS_INSTANCE")
                                       .targetHealth(TargetHealth.builder().state("unhealthy").build())
                .build();

        doReturn(DescribeDbProxyTargetsResponse.builder().targets(target).build()).when(proxy).injectCredentialsAndInvokeV2(any(DescribeDbProxyTargetsRequest.class),
                ArgumentMatchers.<Function<DescribeDbProxyTargetsRequest, DescribeDbProxyTargetsResponse>>any());

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


        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(0)
                                                                    .proxy(dbProxy)
                                                                    .targetGroupStatus(dbProxyTargetGroup)
                                                                    .targets(proxyTargets)
                                                                    .allTargetsHealthy(false)
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
    public void testCheckTargetHealth_cluster() {
        DBProxy dbProxy = DBProxy.builder().status("available").build();
        DBProxyTargetGroup dbProxyTargetGroup = DBProxyTargetGroup.builder().build();
        List<DBProxyTarget> proxyTargets = new ArrayList<>();
        DBProxyTarget target = DBProxyTarget.builder()
                                       .rdsResourceId("resourceId-c")
                                       .type("TRACKED_CLUSTER")
                .build();

        DBProxyTarget targetCluster = DBProxyTarget.builder()
                                       .rdsResourceId("resourceId-i")
                                       .type("RDS_INSTANCE")
                                       .targetHealth(TargetHealth.builder().state(Constants.AVAILABLE_STATE).build())
                .build();

        doReturn(DescribeDbProxyTargetsResponse.builder().targets(target, targetCluster).build()).when(proxy).injectCredentialsAndInvokeV2(any(DescribeDbProxyTargetsRequest.class),
                ArgumentMatchers.<Function<DescribeDbProxyTargetsRequest, DescribeDbProxyTargetsResponse>>any());
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


        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(0)
                                                                    .proxy(dbProxy)
                                                                    .targetGroupStatus(dbProxyTargetGroup)
                                                                    .targets(proxyTargets)
                                                                    .allTargetsHealthy(true)
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
    public void testCheckTargetHealth_noTargets() {
        DBProxy dbProxy = DBProxy.builder().status("available").build();
        DBProxyTargetGroup dbProxyTargetGroup = DBProxyTargetGroup.builder().build();
        List<DBProxyTarget> proxyTargets = new ArrayList<>();
        doReturn(DescribeDbProxyTargetsResponse.builder().build()).when(proxy).injectCredentialsAndInvokeV2(any(DescribeDbProxyTargetsRequest.class),
                ArgumentMatchers.<Function<DescribeDbProxyTargetsRequest, DescribeDbProxyTargetsResponse>>any());
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


        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(0)
                                                                    .proxy(dbProxy)
                                                                    .targetGroupStatus(dbProxyTargetGroup)
                                                                    .targets(proxyTargets)
                                                                    .allTargetsHealthy(true)
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
    public void testRegistration_Available(){
        DBProxy dbProxy = DBProxy.builder().status("available").build();
        DBProxyTargetGroup dbProxyTargetGroup = DBProxyTargetGroup.builder().build();
        DBProxyTarget dbProxyTarget = DBProxyTarget.builder().build();
        doReturn(RegisterDbProxyTargetsResponse.builder().dbProxyTargets(dbProxyTarget).build()).when(proxy)
                .injectCredentialsAndInvokeV2(any(RegisterDbProxyTargetsRequest.class),
                        ArgumentMatchers.<Function<RegisterDbProxyTargetsRequest, RegisterDbProxyTargetsResponse>>any());
        final CreateHandler handler = new CreateHandler();

        ImmutableList<String> clusterId = ImmutableList.of("clusterId");

        final ResourceModel model = ResourceModel.builder().dBClusterIdentifiers(clusterId).build();

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
    public void testRegistration_Creating(){
        DBProxy dbProxy = DBProxy.builder().status("creating").build();

        DBProxyTargetGroup dbProxyTargetGroup = DBProxyTargetGroup.builder().build();
        DBProxyTarget dbProxyTarget = DBProxyTarget.builder().build();
        doReturn(RegisterDbProxyTargetsResponse.builder().dbProxyTargets(dbProxyTarget).build()).when(proxy)
                .injectCredentialsAndInvokeV2(any(RegisterDbProxyTargetsRequest.class),
                        ArgumentMatchers.<Function<RegisterDbProxyTargetsRequest, RegisterDbProxyTargetsResponse>>any());
        final CreateHandler handler = new CreateHandler();

        ImmutableList<String> clusterId = ImmutableList.of("clusterId");

        final ResourceModel model = ResourceModel.builder().dBClusterIdentifiers(clusterId).build();

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
    public void testProxyDoesNotExist() {
        doThrow(DbProxyNotFoundException.builder().message("").build())
                .when(proxy).injectCredentialsAndInvokeV2(any(DescribeDbProxyTargetGroupsRequest.class),
                ArgumentMatchers.<Function<DescribeDbProxyTargetGroupsRequest, DescribeDbProxyTargetGroupsResponse>>any());
        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        final CallbackContext context = CallbackContext.builder()
                                                       .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                       .build();

        ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, context, logger);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);
    }

    @Test
    public void testProxyTargetGroupDoesNotExist() {
        doThrow(DbProxyTargetGroupNotFoundException.builder().message("").build()).when(proxy)
                .injectCredentialsAndInvokeV2(any(DescribeDbProxyTargetGroupsRequest.class),
                ArgumentMatchers.<Function<DescribeDbProxyTargetGroupsRequest, DescribeDbProxyTargetGroupsResponse>>any());
        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                .build();

        ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, context, logger);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);
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
                                                       .proxy(DBProxy.builder().status("creating").build())
                                                       .build();

        try {
            handler.handleRequest(proxy, request, context, logger);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).isEqualTo(CreateHandler.TIMED_OUT_MESSAGE);
        }
    }
}
