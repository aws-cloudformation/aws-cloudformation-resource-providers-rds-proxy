package software.amazon.rds.dbproxyendpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
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

import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.rds.model.DBProxyEndpoint;
import com.amazonaws.services.rds.model.DBProxyEndpointNotFoundException;
import com.amazonaws.services.rds.model.DescribeDBProxyEndpointsRequest;
import com.amazonaws.services.rds.model.DescribeDBProxyEndpointsResult;
import com.amazonaws.services.rds.model.ListTagsForResourceRequest;
import com.amazonaws.services.rds.model.ListTagsForResourceResult;
import com.amazonaws.services.rds.model.Tag;
import com.google.common.collect.ImmutableList;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
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

    @BeforeEach
    public void setup() {
        proxy = mock(AmazonWebServicesClientProxy.class);
        logger = mock(Logger.class);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final ReadHandler handler = new ReadHandler();

        DBProxyEndpoint proxyEndpoint1 = new DBProxyEndpoint()
                .withDBProxyEndpointName("proxyEndpoint1")
                .withDBProxyEndpointArn("arn")
                .withDBProxyName("proxy1")
                .withVpcSubnetIds("vpcsubnet1", "vpcsubnet2")
                .withVpcSecurityGroupIds("sg1", "sg2")
                .withEndpoint("endpoint1");
        final List<DBProxyEndpoint> existingProxyEndpoints = ImmutableList.of(proxyEndpoint1);

        doReturn(new DescribeDBProxyEndpointsResult().withDBProxyEndpoints(existingProxyEndpoints))
                .when(proxy)
                .injectCredentialsAndInvoke(any(DescribeDBProxyEndpointsRequest.class),
                        ArgumentMatchers.<Function<DescribeDBProxyEndpointsRequest, AmazonWebServiceResult<ResponseMetadata>>>any());

        doReturn(new ListTagsForResourceResult()).when(proxy)
                .injectCredentialsAndInvoke(any(ListTagsForResourceRequest.class),
                        ArgumentMatchers.<Function<ListTagsForResourceRequest, AmazonWebServiceResult<ResponseMetadata>>>any());

        final ResourceModel model = ResourceModel.builder().dBProxyEndpointName("proxyEndpoint1").build();

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
        assertThatModelsAreEqual(response.getResourceModel(), proxyEndpoint1);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WithTags() {
        final ReadHandler handler = new ReadHandler();

        DBProxyEndpoint proxyEndpoint1 = new DBProxyEndpoint()
                .withDBProxyEndpointName("proxyEndpoint1")
                .withDBProxyEndpointArn("arn")
                .withDBProxyName("proxy1")
                .withVpcSubnetIds("vpcsubnet1", "vpcsubnet2")
                .withVpcSecurityGroupIds("sg1", "sg2")
                .withEndpoint("endpoint1");
        final List<DBProxyEndpoint> existingProxyEndpoints = ImmutableList.of(proxyEndpoint1);

        doReturn(new DescribeDBProxyEndpointsResult().withDBProxyEndpoints(existingProxyEndpoints))
                .when(proxy)
                .injectCredentialsAndInvoke(any(DescribeDBProxyEndpointsRequest.class),
                        ArgumentMatchers.<Function<DescribeDBProxyEndpointsRequest, AmazonWebServiceResult<ResponseMetadata>>>any());

        String tagKey = "tagKey";
        String tagValue = "tagValue";
        String tagKey2 = "tagKey2";
        String tagValue2 = "tagValue2";
        List<Tag> tagList = ImmutableList.of(new Tag().withKey(tagKey).withValue(tagValue),
                new Tag().withKey(tagKey2).withValue(tagValue2));
        doReturn(new ListTagsForResourceResult().withTagList(tagList))
                .when(proxy)
                .injectCredentialsAndInvoke(any(ListTagsForResourceRequest.class),
                        ArgumentMatchers.<Function<ListTagsForResourceRequest, AmazonWebServiceResult<ResponseMetadata>>>any());

        final ResourceModel model = ResourceModel.builder().dBProxyEndpointName("proxyEndpoint1").build();

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
        assertThatModelsAreEqual(response.getResourceModel(), proxyEndpoint1);
        assertThat(response.getResourceModel().getTags().size()).isEqualTo(2);
        assertThat(response.getResourceModel().getTags().get(0).getKey()).isEqualTo(tagKey);
        assertThat(response.getResourceModel().getTags().get(0).getValue()).isEqualTo(tagValue);
        assertThat(response.getResourceModel().getTags().get(1).getKey()).isEqualTo(tagKey2);
        assertThat(response.getResourceModel().getTags().get(1).getValue()).isEqualTo(tagValue2);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_ResourceNotFound() {
        final ReadHandler handler = new ReadHandler();

        doReturn(new DescribeDBProxyEndpointsResult())
                .when(proxy)
                .injectCredentialsAndInvoke(any(DescribeDBProxyEndpointsRequest.class),
                        ArgumentMatchers.<Function<DescribeDBProxyEndpointsRequest, AmazonWebServiceResult<ResponseMetadata>>>any());

        final ResourceModel model = ResourceModel.builder().dBProxyEndpointName("proxyEndpoint1").build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);
    }

    @Test
    public void handleRequest_ResourceNotFoundExceptionFromRDS() {
        final ReadHandler handler = new ReadHandler();

        DBProxyEndpointNotFoundException exception = new DBProxyEndpointNotFoundException(TestConstants.NOT_FOUND_ERROR_MESSAGE);
        doThrow(exception)
                .when(proxy)
                .injectCredentialsAndInvoke(any(DescribeDBProxyEndpointsRequest.class),
                        ArgumentMatchers.<Function<DescribeDBProxyEndpointsRequest, AmazonWebServiceResult<ResponseMetadata>>>any());

        final ResourceModel model = ResourceModel.builder().dBProxyEndpointName("proxyEndpoint1").build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);
    }
}
