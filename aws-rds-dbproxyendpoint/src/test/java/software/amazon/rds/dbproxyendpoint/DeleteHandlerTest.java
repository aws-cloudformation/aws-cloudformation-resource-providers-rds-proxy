package software.amazon.rds.dbproxyendpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

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
import com.amazonaws.services.rds.model.DeleteDBProxyEndpointRequest;
import com.amazonaws.services.rds.model.DeleteDBProxyEndpointResult;
import com.amazonaws.services.rds.model.DescribeDBProxyEndpointsRequest;
import com.amazonaws.services.rds.model.DescribeDBProxyEndpointsResult;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest {

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
        DBProxyEndpoint dbProxyEndpoint = new DBProxyEndpoint().withStatus("deleting");

        final DeleteHandler handler = new DeleteHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext context = CallbackContext.builder()
                .proxyEndpoint(dbProxyEndpoint)
                .deleted(true)
                .stabilizationRetriesRemaining(1)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_alreadyDeletedTest() {
        DBProxyEndpointNotFoundException exception = new DBProxyEndpointNotFoundException(TestConstants.NOT_FOUND_ERROR_MESSAGE);
        doThrow(exception).when(proxy).injectCredentialsAndInvoke(any(DeleteDBProxyEndpointRequest.class),
                ArgumentMatchers.<Function<DeleteDBProxyEndpointRequest, AmazonWebServiceResult<ResponseMetadata>>>any());

        final DeleteHandler handler = new DeleteHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(1)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                .proxyEndpoint(new DBProxyEndpoint())
                .build();
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isEqualTo(exception.getMessage());
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);
    }

    @Test
    public void handleRequest_deleteTest() {
        DBProxyEndpoint dbProxyEndpoint = new DBProxyEndpoint().withStatus("deleting");
        doReturn(new DeleteDBProxyEndpointResult().withDBProxyEndpoint(dbProxyEndpoint)).when(proxy)
                .injectCredentialsAndInvoke(any(DeleteDBProxyEndpointRequest.class),
                        ArgumentMatchers.<Function<DeleteDBProxyEndpointRequest, AmazonWebServiceResult<ResponseMetadata>>>any());

        final DeleteHandler handler = new DeleteHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(1)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                .proxyEndpoint(dbProxyEndpoint)
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
    public void handleRequest_deletingTest() {
        DBProxyEndpoint dbProxyEndpoint = new DBProxyEndpoint().withStatus("deleting");
        doReturn(new DescribeDBProxyEndpointsResult().withDBProxyEndpoints(dbProxyEndpoint)).when(proxy)
                .injectCredentialsAndInvoke(any(DescribeDBProxyEndpointsRequest.class),
                        ArgumentMatchers.<Function<DescribeDBProxyEndpointsRequest, AmazonWebServiceResult<ResponseMetadata>>>any());

        final DeleteHandler handler = new DeleteHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(60)
                .proxyEndpoint(dbProxyEndpoint)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                .stabilizationRetriesRemaining(59)
                .proxyEndpoint(dbProxyEndpoint)
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
    public void handleRequest_deletedTest() {
        DBProxyEndpoint dbProxyEndpoint = new DBProxyEndpoint().withStatus("deleting");
        doThrow(new DBProxyEndpointNotFoundException("")).when(proxy)
                .injectCredentialsAndInvoke(any(DescribeDBProxyEndpointsRequest.class),
                        ArgumentMatchers.<Function<DescribeDBProxyEndpointsRequest, AmazonWebServiceResult<ResponseMetadata>>>any());

        final DeleteHandler handler = new DeleteHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(60)
                .proxyEndpoint(dbProxyEndpoint)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                .stabilizationRetriesRemaining(59)
                .proxyEndpoint(dbProxyEndpoint)
                .deleted(true)
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
}
