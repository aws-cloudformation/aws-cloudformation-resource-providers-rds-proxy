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

import com.amazonaws.services.rds.model.DBProxy;
import com.amazonaws.services.rds.model.ModifyDBProxyRequest;
import com.amazonaws.services.rds.model.ModifyDBProxyResult;
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
        DBProxy dbProxy = new DBProxy().withStatus("available");
        final CallbackContext context = CallbackContext.builder()
                                                       .proxy(dbProxy)
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
    public void testModifyProxy() {
        DBProxy dbProxy = new DBProxy().withStatus("available");
        doReturn(new ModifyDBProxyResult().withDBProxy(dbProxy)).when(proxy).injectCredentialsAndInvoke(any(ModifyDBProxyRequest.class), any());

        final CallbackContext context = CallbackContext.builder()
                                                       .stabilizationRetriesRemaining(1)
                                                       .build();

        final UpdateHandler handler = new UpdateHandler();

        final ResourceModel desiredModel = ResourceModel.builder().build();
        final ResourceModel oldModel = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(desiredModel)
                                                                      .previousResourceState(oldModel)
                                                                      .build();

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(60)
                                                                    .proxy(dbProxy)
                                                                    .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).containsOnly(request.getDesiredResourceState(), request.getPreviousResourceState());
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }
}
