package software.amazon.rds.dbproxytargetgroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static software.amazon.rds.dbproxytargetgroup.Matchers.assertThatModelsAreEqual;

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
import software.amazon.awssdk.services.rds.model.DBProxyTarget;
import software.amazon.awssdk.services.rds.model.DBProxyTargetGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetGroupsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetsResponse;
import software.amazon.awssdk.services.rds.model.TargetType;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private Logger logger;

    private static final String PROXY_NAME = "proxy1";
    private static final String DEFAULT_NAME = "default";

    @BeforeEach
    public void setup() {
        proxy = mock(AmazonWebServicesClientProxy.class);
        logger = mock(Logger.class);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final ReadHandler handler = new ReadHandler();
        ConnectionPoolConfigurationInfo connectionPoolConfigurationInfo = ConnectionPoolConfigurationInfo.builder().build();

        DBProxyTargetGroup dbProxyTargetGroup1 = DBProxyTargetGroup.builder().connectionPoolConfig(connectionPoolConfigurationInfo)
                                                                         .dbProxyName(PROXY_NAME)
                                                                         .targetGroupName(DEFAULT_NAME)
                .build();
        final List<DBProxyTargetGroup> existingProxies = ImmutableList.of(dbProxyTargetGroup1);


        DescribeDbProxyTargetGroupsRequest describeRequest = DescribeDbProxyTargetGroupsRequest.builder()
                                                                     .dbProxyName(PROXY_NAME)
                                                                     .targetGroupName(DEFAULT_NAME)
                .build();
        doReturn(DescribeDbProxyTargetGroupsResponse.builder().targetGroups(existingProxies).build())
                .when(proxy)
                .injectCredentialsAndInvokeV2(eq(describeRequest),
                        ArgumentMatchers.<Function<DescribeDbProxyTargetGroupsRequest, DescribeDbProxyTargetGroupsResponse>>any());

        DescribeDbProxyTargetsRequest targetRequest = DescribeDbProxyTargetsRequest.builder()
                                                              .dbProxyName(PROXY_NAME)
                                                              .targetGroupName(DEFAULT_NAME)
                .build();
        doReturn(DescribeDbProxyTargetsResponse.builder().build()).when(proxy).injectCredentialsAndInvokeV2(eq(targetRequest),
                ArgumentMatchers.<Function<DescribeDbProxyTargetsRequest, DescribeDbProxyTargetsResponse>>any());

        final ResourceModel model = ResourceModel.builder().dBProxyName(PROXY_NAME).targetGroupName(DEFAULT_NAME).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThatModelsAreEqual(response.getResourceModel(), dbProxyTargetGroup1);
        assertThat(response.getResourceModel().getDBClusterIdentifiers()).isNull();
        assertThat(response.getResourceModel().getDBInstanceIdentifiers()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WithRegisteredCluster() {
        final ReadHandler handler = new ReadHandler();
        ConnectionPoolConfigurationInfo connectionPoolConfigurationInfo = ConnectionPoolConfigurationInfo.builder().build();

        DBProxyTargetGroup dbProxyTargetGroup1 = DBProxyTargetGroup.builder().connectionPoolConfig(connectionPoolConfigurationInfo)
                                                                         .dbProxyName(PROXY_NAME)
                                                                         .targetGroupName(DEFAULT_NAME)
                .build();
        final List<DBProxyTargetGroup> existingProxies = ImmutableList.of(dbProxyTargetGroup1);


        DescribeDbProxyTargetGroupsRequest describeRequest = DescribeDbProxyTargetGroupsRequest.builder()
                                                                     .dbProxyName(PROXY_NAME)
                                                                     .targetGroupName(DEFAULT_NAME)
                .build();
        doReturn(DescribeDbProxyTargetGroupsResponse.builder().targetGroups(existingProxies).build())
                .when(proxy)
                .injectCredentialsAndInvokeV2(eq(describeRequest),
                        ArgumentMatchers.<Function<DescribeDbProxyTargetGroupsRequest, DescribeDbProxyTargetGroupsResponse>>any());

        DescribeDbProxyTargetsRequest targetRequest = DescribeDbProxyTargetsRequest.builder()
                                                                    .dbProxyName(PROXY_NAME)
                                                                    .targetGroupName(DEFAULT_NAME)
                                                                    .build();
        String clusterId = "clusterId";
        doReturn(DescribeDbProxyTargetsResponse.builder()
                         .targets(DBProxyTarget.builder().type(TargetType.TRACKED_CLUSTER).rdsResourceId(clusterId).build(),
                                  DBProxyTarget.builder().type(TargetType.RDS_INSTANCE).rdsResourceId("instanceId").build())
                .build())
                .when(proxy).injectCredentialsAndInvokeV2(eq(targetRequest), ArgumentMatchers.<Function<DescribeDbProxyTargetsRequest, DescribeDbProxyTargetsResponse>>any());

        final ResourceModel model = ResourceModel.builder().dBProxyName(PROXY_NAME).targetGroupName(DEFAULT_NAME).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThatModelsAreEqual(response.getResourceModel(), dbProxyTargetGroup1);
        assertThat(response.getResourceModel().getDBClusterIdentifiers()).isNotNull();
        assertThat(response.getResourceModel().getDBClusterIdentifiers()).isEqualTo(ImmutableList.of(clusterId));
        assertThat(response.getResourceModel().getDBInstanceIdentifiers()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_defaultName() {
        final ReadHandler handler = new ReadHandler();

        ConnectionPoolConfigurationInfo connectionPoolConfigurationInfo = ConnectionPoolConfigurationInfo.builder().build();

        DBProxyTargetGroup dbProxyTargetGroup1 = DBProxyTargetGroup.builder()
                .connectionPoolConfig(connectionPoolConfigurationInfo)
                .dbProxyName(PROXY_NAME)
                .targetGroupName(DEFAULT_NAME)
                .build();
        final List<DBProxyTargetGroup> existingProxies = ImmutableList.of(dbProxyTargetGroup1);


        DescribeDbProxyTargetGroupsRequest describeRequest = DescribeDbProxyTargetGroupsRequest.builder()
                                                                     .dbProxyName(PROXY_NAME)
                                                                     .targetGroupName(DEFAULT_NAME)
                .build();
        doReturn(DescribeDbProxyTargetGroupsResponse.builder().targetGroups(existingProxies).build())
                .when(proxy)
                .injectCredentialsAndInvokeV2(eq(describeRequest),
                        ArgumentMatchers.<Function<DescribeDbProxyTargetGroupsRequest, DescribeDbProxyTargetGroupsResponse>>any());


        final ResourceModel model = ResourceModel.builder().dBProxyName(PROXY_NAME).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThatModelsAreEqual(response.getResourceModel(), dbProxyTargetGroup1);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_ResourceNotFound() {
        final ReadHandler handler = new ReadHandler();

        doReturn(DescribeDbProxyTargetGroupsResponse.builder().build())
                .when(proxy).injectCredentialsAndInvokeV2(any(DescribeDbProxyTargetGroupsRequest.class),
                ArgumentMatchers.<Function<DescribeDbProxyTargetGroupsRequest, DescribeDbProxyTargetGroupsResponse>>any());

        final ResourceModel model = ResourceModel.builder().dBProxyName("proxy1").build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        assertThrows(CfnNotFoundException.class, () -> {
            handler.handleRequest(proxy, request, null, logger);
        });
    }
}
