package software.amazon.rds.dbproxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static software.amazon.rds.dbproxy.Constants.AVAILABLE_PROXY_STATE;

import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.amazonaws.services.rds.model.AddTagsToResourceRequest;
import com.amazonaws.services.rds.model.DBProxy;
import com.amazonaws.services.rds.model.ModifyDBProxyRequest;
import com.amazonaws.services.rds.model.ModifyDBProxyResult;
import com.amazonaws.services.rds.model.RemoveTagsFromResourceRequest;
import com.amazonaws.services.rds.model.Tag;
import com.google.common.collect.ImmutableList;
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
        DBProxy dbProxy = new DBProxy().withStatus(AVAILABLE_PROXY_STATE);
        final CallbackContext context = CallbackContext.builder()
                                                       .proxy(dbProxy)
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
    @SuppressWarnings("unchecked")
    public void testModifyProxy() {
        DBProxy dbProxy = new DBProxy().withStatus(AVAILABLE_PROXY_STATE);
        doReturn(new ModifyDBProxyResult().withDBProxy(dbProxy)).when(proxy).injectCredentialsAndInvoke(any(ModifyDBProxyRequest.class),
                any(Function.class));

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
                                                                    .proxy(dbProxy)
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
    @SuppressWarnings("unchecked")
    public void testDeregisterTags() {
        DBProxy dbProxy = new DBProxy().withStatus(AVAILABLE_PROXY_STATE);
        final CallbackContext context = CallbackContext.builder()
                                                       .stabilizationRetriesRemaining(1)
                                                       .proxy(dbProxy)
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
                                                                    .proxy(dbProxy)
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
        verify(proxy).injectCredentialsAndInvoke(captor.capture(), any(Function.class));
        RemoveTagsFromResourceRequest removeTagsRequest = captor.getValue();
        assertThat(removeTagsRequest.getTagKeys().size()).isEqualTo(2);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRegisterTags() {
        DBProxy dbProxy = new DBProxy().withStatus(AVAILABLE_PROXY_STATE);
        final CallbackContext context = CallbackContext.builder()
                                                       .stabilizationRetriesRemaining(1)
                                                       .tagsDeregistered(true)
                                                       .proxy(dbProxy)
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
                                                                    .proxy(dbProxy)
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
        verify(proxy).injectCredentialsAndInvoke(captor.capture(), any(Function.class));
        AddTagsToResourceRequest removeTagsRequest = captor.getValue();
        assertThat(removeTagsRequest.getTags().size()).isEqualTo(2);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testChangedTagValue_deregister() {
        DBProxy dbProxy = new DBProxy().withStatus(AVAILABLE_PROXY_STATE);
        final CallbackContext context = CallbackContext.builder()
                                                       .stabilizationRetriesRemaining(1)
                                                       .proxy(dbProxy)
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
                                                                    .proxy(dbProxy)
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
        verify(proxy).injectCredentialsAndInvoke(captor.capture(), any(Function.class));
        RemoveTagsFromResourceRequest removeTagsRequest = captor.getValue();
        assertThat(removeTagsRequest.getTagKeys().size()).isEqualTo(1);
        assertThat(removeTagsRequest.getTagKeys().get(0)).isEqualTo(sharedKey);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testChangedTagValue_Register() {
        DBProxy dbProxy = new DBProxy().withStatus(AVAILABLE_PROXY_STATE);
        final CallbackContext context = CallbackContext.builder()
                                                       .stabilizationRetriesRemaining(1)
                                                       .tagsDeregistered(true)
                                                       .proxy(dbProxy)
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
                                                                    .proxy(dbProxy)
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
        verify(proxy).injectCredentialsAndInvoke(captor.capture(), any(Function.class));
        AddTagsToResourceRequest addTagRequest = captor.getValue();
        assertThat(addTagRequest.getTags().size()).isEqualTo(1);
        Tag addedTag = addTagRequest.getTags().get(0);
        assertThat(addedTag.getKey()).isEqualTo(sharedKey);
        assertThat(addedTag.getValue()).isEqualTo(newValue);
    }
}
