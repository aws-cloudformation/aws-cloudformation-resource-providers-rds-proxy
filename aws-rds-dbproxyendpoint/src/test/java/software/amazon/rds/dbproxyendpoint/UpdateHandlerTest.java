package software.amazon.rds.dbproxyendpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static software.amazon.rds.dbproxyendpoint.Constants.AVAILABLE_ENDPOINT_STATE;

import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.DBProxyEndpoint;
import software.amazon.awssdk.services.rds.model.DbProxyEndpointNotFoundException;
import software.amazon.awssdk.services.rds.model.ModifyDbProxyEndpointRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbProxyEndpointResponse;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceResponse;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
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
        DBProxyEndpoint dbProxyEndpoint = DBProxyEndpoint.builder().status(AVAILABLE_ENDPOINT_STATE).build();
        final CallbackContext context = CallbackContext.builder()
                .proxyEndpoint(dbProxyEndpoint)
                .stabilizationRetriesRemaining(1)
                .tagsRegistered(true)
                .tagsDeregistered(true)
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
    public void testModifyNonExistProxyEndpoint() {
        DbProxyEndpointNotFoundException exception = DbProxyEndpointNotFoundException.builder()
                .message(TestConstants.NOT_FOUND_ERROR_MESSAGE)
                .build();
        doThrow(exception).when(proxy).injectCredentialsAndInvokeV2(any(ModifyDbProxyEndpointRequest.class),
                ArgumentMatchers.<Function<ModifyDbProxyEndpointRequest, ModifyDbProxyEndpointResponse>>any());

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

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

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
    public void testModifyDBProxyNameCreateOnlyProperty() {
        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(1)
                .build();

        final UpdateHandler handler = new UpdateHandler();

        final ResourceModel desiredModel = ResourceModel.builder().dBProxyName("new-proxy-name").build();
        final ResourceModel oldModel = ResourceModel.builder().dBProxyName("old-proxy-name").build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .previousResourceState(oldModel)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).contains(UpdateHandler.DB_PROXY_NAME_CREATE_ONLY_MESSAGE);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotUpdatable);
    }

    @Test
    public void testModifyDBProxyEndpointNameCreateOnlyProperty() {
        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(1)
                .build();

        final UpdateHandler handler = new UpdateHandler();

        final ResourceModel desiredModel = ResourceModel.builder().dBProxyEndpointName("new-proxy-endpoint-name").build();
        final ResourceModel oldModel = ResourceModel.builder().dBProxyEndpointName("old-proxy-endpoint-name").build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .previousResourceState(oldModel)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).contains(UpdateHandler.DB_PROXY_ENDPOINT_NAME_CREATE_ONLY_MESSAGE);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotUpdatable);
    }

    @Test
    public void testModifyVpcSubnetIdsCreateOnlyProperty() {
        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(1)
                .build();

        final UpdateHandler handler = new UpdateHandler();

        final ResourceModel desiredModel = ResourceModel.builder().vpcSubnetIds(ImmutableList.of("subnet-1")).build();
        final ResourceModel oldModel = ResourceModel.builder().vpcSubnetIds(ImmutableList.of("subnet-1", "subnet-2")).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .previousResourceState(oldModel)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).contains(UpdateHandler.VPC_SUBNET_ID_CREATE_ONLY_MESSAGE);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotUpdatable);
    }

    @Test
    public void testModifyTargetRoleCreateOnlyProperty() {
        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(1)
                .build();

        final UpdateHandler handler = new UpdateHandler();

        final ResourceModel desiredModel = ResourceModel.builder().targetRole("READ_ONLY").build();
        final ResourceModel oldModel = ResourceModel.builder().targetRole("READ_WRITE").build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .previousResourceState(oldModel)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).contains(UpdateHandler.TARGET_ROLE_CREATE_ONLY_MESSAGE);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotUpdatable);
    }

    @Test
    public void testModifyProxyEndpoint() {
        DBProxyEndpoint dbProxyEndpoint = DBProxyEndpoint.builder().status(AVAILABLE_ENDPOINT_STATE).build();
        doReturn(ModifyDbProxyEndpointResponse.builder().dbProxyEndpoint(dbProxyEndpoint).build())
                .when(proxy).injectCredentialsAndInvokeV2(any(ModifyDbProxyEndpointRequest.class),
                        ArgumentMatchers.<Function<ModifyDbProxyEndpointRequest, ModifyDbProxyEndpointResponse>>any());

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
                .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                .proxyEndpoint(dbProxyEndpoint)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testDeregisterTags() {
        DBProxyEndpoint dbProxyEndpoint = DBProxyEndpoint.builder().status(AVAILABLE_ENDPOINT_STATE).build();
        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(1)
                .proxyEndpoint(dbProxyEndpoint)
                .build();

        final UpdateHandler handler = new UpdateHandler();

        TagFormat tag1 = new TagFormat();
        tag1.setKey("key1");
        tag1.setValue("value1");
        TagFormat tag2 = new TagFormat();
        tag2.setKey("key2");
        tag2.setValue("value2");

        final ResourceModel desiredModel = ResourceModel.builder().build();
        final ResourceModel oldModel = ResourceModel.builder().tags(ImmutableList.of(tag1, tag2)).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .previousResourceState(oldModel)
                .build();

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                .proxyEndpoint(dbProxyEndpoint)
                .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                .tagsDeregistered(true)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        ArgumentCaptor<RemoveTagsFromResourceRequest> captor = ArgumentCaptor.forClass(RemoveTagsFromResourceRequest.class);
        verify(proxy).injectCredentialsAndInvokeV2(captor.capture(),
                ArgumentMatchers.<Function<RemoveTagsFromResourceRequest, RemoveTagsFromResourceResponse>>any());
        RemoveTagsFromResourceRequest removeTagsRequest = captor.getValue();
        assertThat(removeTagsRequest.tagKeys().size()).isEqualTo(2);
    }

    @Test
    public void testRegisterTags() {
        DBProxyEndpoint dbProxyEndpoint = DBProxyEndpoint.builder().status(AVAILABLE_ENDPOINT_STATE).build();
        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(1)
                .tagsDeregistered(true)
                .proxyEndpoint(dbProxyEndpoint)
                .build();

        final UpdateHandler handler = new UpdateHandler();

        TagFormat tag1 = new TagFormat();
        tag1.setKey("key1");
        tag1.setValue("value1");
        TagFormat tag2 = new TagFormat();
        tag2.setKey("key2");
        tag2.setValue("value2");

        final ResourceModel desiredModel = ResourceModel.builder().tags(ImmutableList.of(tag1, tag2)).build();
        final ResourceModel oldModel = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .previousResourceState(oldModel)
                .build();

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                .tagsDeregistered(true)
                .proxyEndpoint(dbProxyEndpoint)
                .tagsRegistered(true)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        ArgumentCaptor<AddTagsToResourceRequest> captor = ArgumentCaptor.forClass(AddTagsToResourceRequest.class);
        verify(proxy).injectCredentialsAndInvokeV2(captor.capture(),
                ArgumentMatchers.<Function<AddTagsToResourceRequest, AddTagsToResourceResponse>>any());
        AddTagsToResourceRequest removeTagsRequest = captor.getValue();
        assertThat(removeTagsRequest.tags().size()).isEqualTo(2);
    }

    @Test
    public void testChangedTagValue_deregister() {
        DBProxyEndpoint dbProxyEndpoint = DBProxyEndpoint.builder().status(AVAILABLE_ENDPOINT_STATE).build();
        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(1)
                .proxyEndpoint(dbProxyEndpoint)
                .build();

        final UpdateHandler handler = new UpdateHandler();

        String sharedKey = "key1";
        TagFormat tag1 = new TagFormat();
        tag1.setKey(sharedKey);
        tag1.setValue("value1");
        TagFormat tag2 = new TagFormat();
        tag2.setKey("key2");
        tag2.setValue("value2");
        TagFormat tag3 = new TagFormat();
        tag3.setKey(sharedKey);
        tag3.setValue("value3");

        final ResourceModel desiredModel = ResourceModel.builder().tags(ImmutableList.of(tag3, tag2)).build();
        final ResourceModel oldModel = ResourceModel.builder().tags(ImmutableList.of(tag1, tag2)).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .previousResourceState(oldModel)
                .build();

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                .tagsDeregistered(true)
                .proxyEndpoint(dbProxyEndpoint)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        ArgumentCaptor<RemoveTagsFromResourceRequest> captor = ArgumentCaptor.forClass(RemoveTagsFromResourceRequest.class);
        verify(proxy).injectCredentialsAndInvokeV2(captor.capture(),
                ArgumentMatchers.<Function<RemoveTagsFromResourceRequest, RemoveTagsFromResourceResponse>>any());
        RemoveTagsFromResourceRequest removeTagsRequest = captor.getValue();
        assertThat(removeTagsRequest.tagKeys().size()).isEqualTo(1);
        assertThat(removeTagsRequest.tagKeys().get(0)).isEqualTo(sharedKey);
    }

    @Test
    public void testChangedTagValue_Register() {
        DBProxyEndpoint dbProxyEndpoint = DBProxyEndpoint.builder().status(AVAILABLE_ENDPOINT_STATE).build();
        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(1)
                .tagsDeregistered(true)
                .proxyEndpoint(dbProxyEndpoint)
                .build();

        final UpdateHandler handler = new UpdateHandler();

        String sharedKey = "key1";
        String newValue = "value3";
        TagFormat tag1 = new TagFormat();
        tag1.setKey(sharedKey);
        tag1.setValue("value1");
        TagFormat tag2 = new TagFormat();
        tag2.setKey("key2");
        tag2.setValue("value2");
        TagFormat tag3 = new TagFormat();
        tag3.setKey(sharedKey);
        tag3.setValue(newValue);

        final ResourceModel desiredModel = ResourceModel.builder().tags(ImmutableList.of(tag3, tag2)).build();
        final ResourceModel oldModel = ResourceModel.builder().tags(ImmutableList.of(tag1, tag2)).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .previousResourceState(oldModel)
                .build();

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                .tagsDeregistered(true)
                .tagsRegistered(true)
                .proxyEndpoint(dbProxyEndpoint)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        ArgumentCaptor<AddTagsToResourceRequest> captor = ArgumentCaptor.forClass(AddTagsToResourceRequest.class);
        verify(proxy).injectCredentialsAndInvokeV2(captor.capture(),
                ArgumentMatchers.<Function<AddTagsToResourceRequest, AddTagsToResourceResponse>>any());
        AddTagsToResourceRequest addTagRequest = captor.getValue();
        assertThat(addTagRequest.tags().size()).isEqualTo(1);
        Tag addedTag = addTagRequest.tags().get(0);
        assertThat(addedTag.key()).isEqualTo(sharedKey);
        assertThat(addedTag.value()).isEqualTo(newValue);
    }
}
