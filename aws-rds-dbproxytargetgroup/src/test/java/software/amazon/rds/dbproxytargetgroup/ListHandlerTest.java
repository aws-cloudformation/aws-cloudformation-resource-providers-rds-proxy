package software.amazon.rds.dbproxytargetgroup;

import com.amazonaws.services.rds.model.ConnectionPoolConfigurationInfo;
import com.amazonaws.services.rds.model.DBProxyTargetGroup;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetGroupsRequest;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetGroupsResult;
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
import static software.amazon.rds.dbproxytargetgroup.Matchers.assertThatModelsAreEqual;

import java.util.List;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest {

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
        final ListHandler handler = new ListHandler();

        final ResourceModel model = ResourceModel.builder().build();

        ConnectionPoolConfigurationInfo connectionPoolConfigurationInfo = new ConnectionPoolConfigurationInfo();

        DBProxyTargetGroup dbProxyTargetGroup1 = new DBProxyTargetGroup().withConnectionPoolConfig(connectionPoolConfigurationInfo)
                                                                         .withDBProxyName("proxy1")
                                                                         .withTargetGroupName("default");
        DBProxyTargetGroup dbProxyTargetGroup2 = new DBProxyTargetGroup().withConnectionPoolConfig(connectionPoolConfigurationInfo)
                                                                         .withDBProxyName("proxy2")
                                                                         .withTargetGroupName("default");
        final List<DBProxyTargetGroup> existingProxies = ImmutableList.of(dbProxyTargetGroup1, dbProxyTargetGroup2);

        doReturn(new DescribeDBProxyTargetGroupsResult().withTargetGroups(existingProxies))
                .when(proxy).injectCredentialsAndInvoke(any(DescribeDBProxyTargetGroupsRequest.class), any());


        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
                handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels().size()).isEqualTo(2);
        assertThatModelsAreEqual(response.getResourceModels().get(0), dbProxyTargetGroup1);
        assertThatModelsAreEqual(response.getResourceModels().get(1), dbProxyTargetGroup2);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }
}
