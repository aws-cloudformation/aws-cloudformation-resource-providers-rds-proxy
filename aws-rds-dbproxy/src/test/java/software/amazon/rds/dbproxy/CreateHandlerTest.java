package software.amazon.rds.dbproxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.amazonaws.services.rds.model.CreateDBProxyRequest;
import com.amazonaws.services.rds.model.CreateDBProxyResult;
import com.amazonaws.services.rds.model.DBProxy;
import com.amazonaws.services.rds.model.DescribeDBProxiesRequest;
import com.amazonaws.services.rds.model.DescribeDBProxiesResult;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
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
    public void handleRequest_InitialRunCreateProxy() {
        DBProxy dbProxy = new DBProxy().withStatus("creating");
        doReturn(new CreateDBProxyResult().withDBProxy(dbProxy)).when(proxy).injectCredentialsAndInvoke(any(CreateDBProxyRequest.class), any());

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
                                                                    .stabilizationRetriesRemaining(60)
                                                                    .proxy(dbProxy)
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
    public void handleRequest_Creating() {
        DBProxy dbProxy = new DBProxy().withStatus("creating");
        doReturn(new DescribeDBProxiesResult().withDBProxies(dbProxy)).when(proxy).injectCredentialsAndInvoke(any(DescribeDBProxiesRequest.class), any());

        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        final CallbackContext context = CallbackContext.builder()
                                                       .proxy(dbProxy)
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
        DBProxy dbProxy = new DBProxy().withStatus("available");

        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        final CallbackContext context = CallbackContext.builder()
                                                       .proxy(dbProxy)
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
    public void testStabilizationTimeout() {
        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        final CallbackContext context = CallbackContext.builder()
                                                       .stabilizationRetriesRemaining(0)
                                                       .proxy(new DBProxy().withStatus("creating"))
                                                       .build();

        try {
            handler.handleRequest(proxy, request, context, logger);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).isEqualTo(CreateHandler.TIMED_OUT_MESSAGE);
        }
    }
}
