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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.amazonaws.services.rds.model.ConnectionPoolConfigurationInfo;
import com.amazonaws.services.rds.model.DBProxyTarget;
import com.amazonaws.services.rds.model.DBProxyTargetGroup;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetGroupsRequest;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetGroupsResult;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetsRequest;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetsResult;
import com.amazonaws.services.rds.model.TargetType;
import com.google.common.collect.ImmutableList;
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
    @SuppressWarnings("unchecked")
    public void handleRequest_SimpleSuccess() {
        final ReadHandler handler = new ReadHandler();
        ConnectionPoolConfigurationInfo connectionPoolConfigurationInfo = new ConnectionPoolConfigurationInfo();

        DBProxyTargetGroup dbProxyTargetGroup1 = new DBProxyTargetGroup().withConnectionPoolConfig(connectionPoolConfigurationInfo)
                                                                         .withDBProxyName(PROXY_NAME)
                                                                         .withTargetGroupName(DEFAULT_NAME);
        final List<DBProxyTargetGroup> existingProxies = ImmutableList.of(dbProxyTargetGroup1);


        DescribeDBProxyTargetGroupsRequest describeRequest = new DescribeDBProxyTargetGroupsRequest()
                                                                     .withDBProxyName(PROXY_NAME)
                                                                     .withTargetGroupName(DEFAULT_NAME);
        doReturn(new DescribeDBProxyTargetGroupsResult().withTargetGroups(existingProxies))
                .when(proxy)
                .injectCredentialsAndInvoke(eq(describeRequest), any(Function.class));

        DescribeDBProxyTargetsRequest targetRequest = new DescribeDBProxyTargetsRequest()
                                                              .withDBProxyName(PROXY_NAME)
                                                              .withTargetGroupName(DEFAULT_NAME);
        doReturn(new DescribeDBProxyTargetsResult()).when(proxy).injectCredentialsAndInvoke(eq(targetRequest), any(Function.class));

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
    @SuppressWarnings("unchecked")
    public void handleRequest_WithRegisteredCluster() {
        final ReadHandler handler = new ReadHandler();
        ConnectionPoolConfigurationInfo connectionPoolConfigurationInfo = new ConnectionPoolConfigurationInfo();

        DBProxyTargetGroup dbProxyTargetGroup1 = new DBProxyTargetGroup().withConnectionPoolConfig(connectionPoolConfigurationInfo)
                                                                         .withDBProxyName(PROXY_NAME)
                                                                         .withTargetGroupName(DEFAULT_NAME);
        final List<DBProxyTargetGroup> existingProxies = ImmutableList.of(dbProxyTargetGroup1);


        DescribeDBProxyTargetGroupsRequest describeRequest = new DescribeDBProxyTargetGroupsRequest()
                                                                     .withDBProxyName(PROXY_NAME)
                                                                     .withTargetGroupName(DEFAULT_NAME);
        doReturn(new DescribeDBProxyTargetGroupsResult().withTargetGroups(existingProxies))
                .when(proxy)
                .injectCredentialsAndInvoke(eq(describeRequest), any(Function.class));

        DescribeDBProxyTargetsRequest targetRequest = new DescribeDBProxyTargetsRequest()
                                                                   .withDBProxyName(PROXY_NAME)
                                                                   .withTargetGroupName(DEFAULT_NAME);
        String clusterId = "clusterId";
        doReturn(new DescribeDBProxyTargetsResult()
                         .withTargets(new DBProxyTarget().withType(TargetType.TRACKED_CLUSTER).withRdsResourceId(clusterId),
                                      new DBProxyTarget().withType(TargetType.RDS_INSTANCE).withRdsResourceId("instanceId")))
                .when(proxy).injectCredentialsAndInvoke(eq(targetRequest), any(Function.class));

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
    @SuppressWarnings("unchecked")
    public void handleRequest_defaultName() {
        final ReadHandler handler = new ReadHandler();

        ConnectionPoolConfigurationInfo connectionPoolConfigurationInfo = new ConnectionPoolConfigurationInfo();

        DBProxyTargetGroup dbProxyTargetGroup1 = new DBProxyTargetGroup().withConnectionPoolConfig(connectionPoolConfigurationInfo)
                                                                         .withDBProxyName(PROXY_NAME)
                                                                         .withTargetGroupName(DEFAULT_NAME);
        final List<DBProxyTargetGroup> existingProxies = ImmutableList.of(dbProxyTargetGroup1);


        DescribeDBProxyTargetGroupsRequest describeRequest = new DescribeDBProxyTargetGroupsRequest()
                                                                     .withDBProxyName(PROXY_NAME)
                                                                     .withTargetGroupName(DEFAULT_NAME);
        doReturn(new DescribeDBProxyTargetGroupsResult().withTargetGroups(existingProxies))
                .when(proxy)
                .injectCredentialsAndInvoke(eq(describeRequest), any(Function.class));


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
    @SuppressWarnings("unchecked")
    public void handleRequest_ResourceNotFound() {
        final ReadHandler handler = new ReadHandler();

        doReturn(new DescribeDBProxyTargetGroupsResult())
                .when(proxy).injectCredentialsAndInvoke(any(DescribeDBProxyTargetGroupsRequest.class), any(Function.class));

        final ResourceModel model = ResourceModel.builder().dBProxyName("proxy1").build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        assertThrows(CfnNotFoundException.class, () -> {
            handler.handleRequest(proxy, request, null, logger);
        });
    }
}
