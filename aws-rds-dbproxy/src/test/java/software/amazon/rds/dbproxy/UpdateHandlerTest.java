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
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.DBProxy;
import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.rds.model.ModifyDbProxyRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbProxyResponse;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceResponse;
import software.amazon.awssdk.services.rds.model.Tag;
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
        DBProxy dbProxy = DBProxy.builder().status(AVAILABLE_PROXY_STATE).build();
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
    public void testModifyProxy() {
        DBProxy dbProxy = DBProxy.builder().status(AVAILABLE_PROXY_STATE).build();
        doReturn(ModifyDbProxyResponse.builder().dbProxy(dbProxy).build()).when(proxy).injectCredentialsAndInvokeV2(any(ModifyDbProxyRequest.class),
                ArgumentMatchers.<Function<ModifyDbProxyRequest, ModifyDbProxyResponse>>any());

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
    public void testDeregisterTags() {
        DBProxy dbProxy = DBProxy.builder().status(AVAILABLE_PROXY_STATE).build();
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
        verify(proxy).injectCredentialsAndInvokeV2(captor.capture(),
                ArgumentMatchers.<Function<RemoveTagsFromResourceRequest, RemoveTagsFromResourceResponse>>any());
        RemoveTagsFromResourceRequest removeTagsRequest = captor.getValue();
        assertThat(removeTagsRequest.tagKeys().size()).isEqualTo(2);
    }

    @Test
    public void testRegisterTags() {
        DBProxy dbProxy = DBProxy.builder().status(AVAILABLE_PROXY_STATE).build();
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
        verify(proxy).injectCredentialsAndInvokeV2(captor.capture(),
                ArgumentMatchers.<Function<AddTagsToResourceRequest, AddTagsToResourceResponse>>any());
        AddTagsToResourceRequest removeTagsRequest = captor.getValue();
        assertThat(removeTagsRequest.tags().size()).isEqualTo(2);
    }

    @Test
    public void testChangedTagValue_deregister() {
        DBProxy dbProxy = DBProxy.builder().status(AVAILABLE_PROXY_STATE).build();
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
        verify(proxy).injectCredentialsAndInvokeV2(captor.capture(),
                ArgumentMatchers.<Function<RemoveTagsFromResourceRequest, RemoveTagsFromResourceResponse>>any());
        RemoveTagsFromResourceRequest removeTagsRequest = captor.getValue();
        assertThat(removeTagsRequest.tagKeys().size()).isEqualTo(1);
        assertThat(removeTagsRequest.tagKeys().get(0)).isEqualTo(sharedKey);
    }

    @Test
    public void testChangedTagValue_Register() {
        DBProxy dbProxy = DBProxy.builder().status(AVAILABLE_PROXY_STATE).build();
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
        verify(proxy).injectCredentialsAndInvokeV2(captor.capture(),
                ArgumentMatchers.<Function<AddTagsToResourceRequest, AddTagsToResourceResponse>>any());
        AddTagsToResourceRequest addTagRequest = captor.getValue();
        assertThat(addTagRequest.tags().size()).isEqualTo(1);
        Tag addedTag = addTagRequest.tags().get(0);
        assertThat(addedTag.key()).isEqualTo(sharedKey);
        assertThat(addedTag.value()).isEqualTo(newValue);
    }
}
