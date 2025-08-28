package software.amazon.rds.dbproxyendpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static software.amazon.rds.dbproxyendpoint.Matchers.assertThatModelsAreEqual;

import java.util.List;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.rds.model.DBProxyEndpoint;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyEndpointsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyEndpointsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

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

        DBProxyEndpoint proxyEndpoint1 = DBProxyEndpoint.builder().dbProxyEndpointName("proxyEndpoint1").build();
        DBProxyEndpoint proxyEndpoint2 = DBProxyEndpoint.builder().dbProxyEndpointName("proxyEndpoint2").build();
        final List<DBProxyEndpoint> existingProxyEndpoints = ImmutableList.of(proxyEndpoint1, proxyEndpoint2);

        doReturn(DescribeDbProxyEndpointsResponse.builder().dbProxyEndpoints(existingProxyEndpoints).build())
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(DescribeDbProxyEndpointsRequest.class),
                        ArgumentMatchers.<Function<DescribeDbProxyEndpointsRequest, DescribeDbProxyEndpointsResponse>>any());


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
        assertThatModelsAreEqual(response.getResourceModels().get(0), proxyEndpoint1);
        assertThatModelsAreEqual(response.getResourceModels().get(1), proxyEndpoint2);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }
}
