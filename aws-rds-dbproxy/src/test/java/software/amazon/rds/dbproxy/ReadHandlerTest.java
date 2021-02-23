package software.amazon.rds.dbproxy;

import com.amazonaws.services.rds.model.DBProxy;
import com.amazonaws.services.rds.model.DescribeDBProxiesRequest;
import com.amazonaws.services.rds.model.DescribeDBProxiesResult;
import com.amazonaws.services.rds.model.ListTagsForResourceRequest;
import com.amazonaws.services.rds.model.ListTagsForResourceResult;
import com.amazonaws.services.rds.model.Tag;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static software.amazon.rds.dbproxy.Matchers.assertThatModelsAreEqual;

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
    @SuppressWarnings("unchecked")
    public void handleRequest_SimpleSuccess() {
        final ReadHandler handler = new ReadHandler();

        DBProxy proxy1 = new DBProxy().withDBProxyName("proxy1")
                                      .withDBProxyArn("arn")
                                      .withRoleArn("rolearn")
                                      .withVpcSubnetIds("vpcsubnet1", "vpcsubnet2")
                                      .withVpcSecurityGroupIds("sg1", "sg2");
        final List<DBProxy> existingProxies = ImmutableList.of(proxy1);

        doReturn(new DescribeDBProxiesResult().withDBProxies(existingProxies))
                .when(proxy)
                .injectCredentialsAndInvoke(any(DescribeDBProxiesRequest.class), any(Function.class));

        doReturn(new ListTagsForResourceResult()).when(proxy).injectCredentialsAndInvoke(any(ListTagsForResourceRequest.class), any(Function.class));

        final ResourceModel model = ResourceModel.builder().dBProxyName("proxy1").build();

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
        assertThatModelsAreEqual(response.getResourceModel(), proxy1);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void handleRequest_WithTags() {
        final ReadHandler handler = new ReadHandler();

        DBProxy proxy1 = new DBProxy().withDBProxyName("proxy1")
                                      .withDBProxyArn("arn")
                                      .withRoleArn("rolearn")
                                      .withVpcSubnetIds("vpcsubnet1", "vpcsubnet2")
                                      .withVpcSecurityGroupIds("sg1", "sg2");
        final List<DBProxy> existingProxies = ImmutableList.of(proxy1);

        doReturn(new DescribeDBProxiesResult().withDBProxies(existingProxies))
                .when(proxy)
                .injectCredentialsAndInvoke(any(DescribeDBProxiesRequest.class), any(Function.class));

        String tagKey = "tagKey";
        String tagValue = "tagValue";
        String tagKey2 = "tagKey2";
        String tagValue2 = "tagValue2";
        List<Tag> tagList = ImmutableList.of(new Tag().withKey(tagKey).withValue(tagValue),
                                             new Tag().withKey(tagKey2).withValue(tagValue2));
        doReturn(new ListTagsForResourceResult().withTagList(tagList))
                .when(proxy)
                .injectCredentialsAndInvoke(any(ListTagsForResourceRequest.class), any(Function.class));

        final ResourceModel model = ResourceModel.builder().dBProxyName("proxy1").build();

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
        assertThatModelsAreEqual(response.getResourceModel(), proxy1);
        assertThat(response.getResourceModel().getTags().size()).isEqualTo(2);
        assertThat(response.getResourceModel().getTags().get(0).getKey()).isEqualTo(tagKey);
        assertThat(response.getResourceModel().getTags().get(0).getValue()).isEqualTo(tagValue);
        assertThat(response.getResourceModel().getTags().get(1).getKey()).isEqualTo(tagKey2);
        assertThat(response.getResourceModel().getTags().get(1).getValue()).isEqualTo(tagValue2);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void handleRequest_ResourceNotFound() {
        final ReadHandler handler = new ReadHandler();

        doReturn(new DescribeDBProxiesResult())
                .when(proxy)
                .injectCredentialsAndInvoke(any(DescribeDBProxiesRequest.class), any(Function.class));

        final ResourceModel model = ResourceModel.builder().dBProxyName("proxy1").build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        assertThrows(CfnNotFoundException.class, () -> {
            handler.handleRequest(proxy, request, null, logger);
        });
    }
}
