package software.amazon.rds.dbproxytargetgroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.rds.model.DBProxyTarget;
import software.amazon.awssdk.services.rds.model.DbProxyNotFoundException;
import software.amazon.awssdk.services.rds.model.DeregisterDbProxyTargetsRequest;
import software.amazon.awssdk.services.rds.model.DeregisterDbProxyTargetsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetsResponse;
import software.amazon.awssdk.services.rds.model.InvalidDbProxyStateException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
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
        final DeleteHandler handler = new DeleteHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        final CallbackContext context = CallbackContext.builder()
                                                       .targets(new ArrayList<>())
                                                       .stabilizationRetriesRemaining(1)
                                                       .targetsDeregistered(true)
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
    public void handleRequest_DeregisterEmpty() {
        DescribeDbProxyTargetsResponse emptyResult = DescribeDbProxyTargetsResponse.builder().build();
        doReturn(emptyResult).when(proxy).injectCredentialsAndInvokeV2(any(DescribeDbProxyTargetsRequest.class),
                ArgumentMatchers.<Function<DescribeDbProxyTargetsRequest, DescribeDbProxyTargetsResponse>>any());

        final DeleteHandler handler = new DeleteHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        final CallbackContext context = CallbackContext.builder()
                                                       .stabilizationRetriesRemaining(1)
                                                       .build();

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                                    .targetsDeregistered(true)
                                                                    .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);
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
    public void handleRequest_DeregisterInstance() {
        String instanceId= "instanceID";
        DescribeDbProxyTargetsResponse describeResponse = DescribeDbProxyTargetsResponse.builder()
                                                              .targets(DBProxyTarget.builder()
                                                                                   .rdsResourceId(instanceId)
                                                                                   .type("RDS_INSTANCE")
                                                                      .build())
                .build();
        doReturn(describeResponse).when(proxy).injectCredentialsAndInvokeV2(any(DescribeDbProxyTargetsRequest.class),
                ArgumentMatchers.<Function<DescribeDbProxyTargetsRequest, DescribeDbProxyTargetsResponse>>any());
        final DeleteHandler handler = new DeleteHandler();

        final ResourceModel oldModel = ResourceModel.builder().dBInstanceIdentifiers(ImmutableList.of("db1")).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(oldModel)
                                                                      .build();

        final CallbackContext context = CallbackContext.builder()
                                                       .stabilizationRetriesRemaining(1)
                                                       .build();

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                                    .targetsDeregistered(true)
                                                                    .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        ArgumentCaptor<DeregisterDbProxyTargetsRequest> captor = ArgumentCaptor.forClass(DeregisterDbProxyTargetsRequest.class);
        verify(proxy).injectCredentialsAndInvokeV2(any(DeregisterDbProxyTargetsRequest.class), ArgumentMatchers.<Function<DeregisterDbProxyTargetsRequest, DeregisterDbProxyTargetsResponse>>any());
        verify(proxy).injectCredentialsAndInvokeV2(captor.capture(),
                ArgumentMatchers.<Function<DeregisterDbProxyTargetsRequest, DeregisterDbProxyTargetsResponse>>any());
        DeregisterDbProxyTargetsRequest deregisterDBProxyTargetsRequest = captor.getValue();
        assertThat(deregisterDBProxyTargetsRequest.dbInstanceIdentifiers()).isEqualTo(ImmutableList.of(instanceId));
        assertThat(deregisterDBProxyTargetsRequest.dbClusterIdentifiers().size()).isEqualTo(0);
    }

    @Test
    public void handleRequest_DeregisterCluster() {
        String instanceId= "instanceID";
        String clusterName = "clusterName";
        DBProxyTarget instance = DBProxyTarget.builder().rdsResourceId(instanceId).type("RDS_INSTANCE").build();
        DBProxyTarget cluster = DBProxyTarget.builder().rdsResourceId(clusterName).type("TRACKED_CLUSTER").build();
        DescribeDbProxyTargetsResponse describeResponse = DescribeDbProxyTargetsResponse.builder()
                                                              .targets(instance, cluster)
                .build();
        doReturn(describeResponse).when(proxy).injectCredentialsAndInvokeV2(any(DescribeDbProxyTargetsRequest.class),
                ArgumentMatchers.<Function<DescribeDbProxyTargetsRequest, DescribeDbProxyTargetsResponse>>any());
        final DeleteHandler handler = new DeleteHandler();

        final ResourceModel oldModel = ResourceModel.builder().dBInstanceIdentifiers(ImmutableList.of("db1")).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(oldModel)
                                                                      .build();

        final CallbackContext context = CallbackContext.builder()
                                                       .stabilizationRetriesRemaining(1)
                                                       .build();

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                                    .targetsDeregistered(true)
                                                                    .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        ArgumentCaptor<DeregisterDbProxyTargetsRequest> captor = ArgumentCaptor.forClass(DeregisterDbProxyTargetsRequest.class);
        verify(proxy).injectCredentialsAndInvokeV2(any(DescribeDbProxyTargetsRequest.class),
                ArgumentMatchers.<Function<DescribeDbProxyTargetsRequest, DescribeDbProxyTargetsResponse>>any());
        verify(proxy).injectCredentialsAndInvokeV2(captor.capture(),
                ArgumentMatchers.<Function<DeregisterDbProxyTargetsRequest, DeregisterDbProxyTargetsResponse>>any());
        DeregisterDbProxyTargetsRequest deregisterDBProxyTargetsRequest = captor.getValue();
        assertThat(deregisterDBProxyTargetsRequest.dbClusterIdentifiers()).isEqualTo(ImmutableList.of(clusterName));
        assertThat(deregisterDBProxyTargetsRequest.dbInstanceIdentifiers().size()).isEqualTo(0);
    }

    @Test
    public void handleRequest_DeregisterProxyDeleted() {
        doThrow(DbProxyNotFoundException.builder().message("").build()).when(proxy).injectCredentialsAndInvokeV2(any(DescribeDbProxyTargetsRequest.class),
                ArgumentMatchers.<Function<DescribeDbProxyTargetsRequest, DescribeDbProxyTargetsResponse>>any());

        final DeleteHandler handler = new DeleteHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        final CallbackContext context = CallbackContext.builder()
                                                       .stabilizationRetriesRemaining(1)
                                                       .build();

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                                    .targetsDeregistered(true)
                                                                    .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);
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
    public void handleRequest_DeregisterProxyDeleting() {
        doThrow(InvalidDbProxyStateException.builder().message("DB Proxy prx-123 is in an unsupported state - DELETING, needs to be in [ACTIVE]").build()).when(proxy)
                .injectCredentialsAndInvokeV2(any(DescribeDbProxyTargetsRequest.class),
                        ArgumentMatchers.<Function<DescribeDbProxyTargetsRequest, DescribeDbProxyTargetsResponse>>any());

        final DeleteHandler handler = new DeleteHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(model)
                                                                      .build();

        final CallbackContext context = CallbackContext.builder()
                                                       .stabilizationRetriesRemaining(1)
                                                       .build();

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                                                                    .stabilizationRetriesRemaining(Constants.NUMBER_OF_STATE_POLL_RETRIES)
                                                                    .targetsDeregistered(true)
                                                                    .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);
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
