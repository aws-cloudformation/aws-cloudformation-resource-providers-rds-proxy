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

import software.amazon.awssdk.services.rds.model.CreateDbProxyEndpointRequest;
import software.amazon.awssdk.services.rds.model.CreateDbProxyEndpointResponse;
import software.amazon.awssdk.services.rds.model.DBProxyEndpoint;
import software.amazon.awssdk.services.rds.model.DbProxyEndpointAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyEndpointsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyEndpointsResponse;
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
    public void handleRequest_InitialRunCreateProxyEndpoint() {
        DBProxyEndpoint dbProxyEndpoint = DBProxyEndpoint.builder().status("creating").build();
        doReturn(CreateDbProxyEndpointResponse.builder().dbProxyEndpoint(dbProxyEndpoint).build()).when(proxy)
                .injectCredentialsAndInvokeV2(any(CreateDbProxyEndpointRequest.class),
                        ArgumentMatchers.<Function<CreateDbProxyEndpointRequest, CreateDbProxyEndpointResponse>>any());

        final CreateHandler handler = new CreateHandler();

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
    public void handleRequest_CreateProxyEndpointAlreadyExists() {
        DbProxyEndpointAlreadyExistsException exception = DbProxyEndpointAlreadyExistsException.builder().message(TestConstants.ALREADY_EXIST_ERROR_MESSAGE).build();
        doThrow(exception).when(proxy).injectCredentialsAndInvokeV2(any(CreateDbProxyEndpointRequest.class),
                ArgumentMatchers.<Function<CreateDbProxyEndpointRequest, CreateDbProxyEndpointResponse>>any());

        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(1)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isEqualTo(exception.getMessage());
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.AlreadyExists);
    }

    @Test
    public void handleRequest_CreateProxyEndpointWithEndpointProperty() {
        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder().endpoint("test-endpoint").build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(1)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).contains(CreateHandler.ENDPOINT_READ_ONLY_MESSAGE);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
    }

    @Test
    public void handleRequest_CreateProxyEndpointWithDBProxyEndpointArnProperty() {
        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder().dBProxyEndpointArn("test-endpoint-arn").build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(1)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).contains(CreateHandler.DB_PROXY_ENDPOINT_ARN_READ_ONLY_MESSAGE);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
    }

    @Test
    public void handleRequest_CreateProxyEndpointWithVpcIdProperty() {
        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder().vpcId("test-vpc-id").build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(1)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).contains(CreateHandler.VPC_ID_READ_ONLY_MESSAGE);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
    }

    @Test
    public void handleRequest_CreateProxyEndpointWithIsDefaultProperty() {
        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder().isDefault(true).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(1)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).contains(CreateHandler.IS_DEFAULT_READ_ONLY_MESSAGE);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
    }

    @Test
    public void handleRequest_Creating() {
        DBProxyEndpoint dbProxyEndpoint = DBProxyEndpoint.builder().status("creating").build();
        doReturn(DescribeDbProxyEndpointsResponse.builder().dbProxyEndpoints(dbProxyEndpoint).build()).when(proxy)
                .injectCredentialsAndInvokeV2(any(DescribeDbProxyEndpointsRequest.class),
                        ArgumentMatchers.<Function<DescribeDbProxyEndpointsRequest, DescribeDbProxyEndpointsResponse>>any());

        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext context = CallbackContext.builder()
                .proxyEndpoint(dbProxyEndpoint)
                .stabilizationRetriesRemaining(1)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_Available() {
        DBProxyEndpoint dbProxyEndpoint = DBProxyEndpoint.builder().status("available").build();

        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext context = CallbackContext.builder()
                .proxyEndpoint(dbProxyEndpoint)
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
    public void handleRequest_terminalState() {
        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(60)
                .proxyEndpoint(DBProxyEndpoint.builder().status("incompatible-network").build())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
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
                .proxyEndpoint(DBProxyEndpoint.builder().status("creating").build())
                .build();

        try {
            handler.handleRequest(proxy, request, context, logger);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).isEqualTo(CreateHandler.TIMED_OUT_MESSAGE);
        }
    }
}
