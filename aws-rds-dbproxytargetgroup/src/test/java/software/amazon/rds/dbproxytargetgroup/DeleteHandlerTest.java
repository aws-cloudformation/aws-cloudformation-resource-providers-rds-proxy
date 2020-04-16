package software.amazon.rds.dbproxytargetgroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.amazonaws.services.rds.model.DBProxyTarget;
import com.amazonaws.services.rds.model.DeregisterDBProxyTargetsRequest;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetsRequest;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetsResult;
import com.google.common.collect.ImmutableList;
import jdk.nashorn.internal.ir.annotations.Immutable;
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
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_DeregisterEmpty() {
        DescribeDBProxyTargetsResult emptyResult = new DescribeDBProxyTargetsResult();
        doReturn(emptyResult).when(proxy).injectCredentialsAndInvoke(any(DescribeDBProxyTargetsRequest.class), any());

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
        DescribeDBProxyTargetsResult describeResult = new DescribeDBProxyTargetsResult()
                                                              .withTargets(new DBProxyTarget()
                                                                                   .withRdsResourceId(instanceId)
                                                                                   .withType("RDS_INSTANCE"));
        doReturn(describeResult).when(proxy).injectCredentialsAndInvoke(any(DescribeDBProxyTargetsRequest.class), any());
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

        ArgumentCaptor<DeregisterDBProxyTargetsRequest> captor = ArgumentCaptor.forClass(DeregisterDBProxyTargetsRequest.class);
        verify(proxy).injectCredentialsAndInvoke(any(DescribeDBProxyTargetsRequest.class), any());
        verify(proxy, times(2)).injectCredentialsAndInvoke(captor.capture(), any());
        DeregisterDBProxyTargetsRequest deregisterDBProxyTargetsRequest = captor.getValue();
        assertThat(deregisterDBProxyTargetsRequest.getDBInstanceIdentifiers()).isEqualTo(ImmutableList.of(instanceId));
        assertThat(deregisterDBProxyTargetsRequest.getDBClusterIdentifiers().size()).isEqualTo(0);
    }

    @Test
    public void handleRequest_DeregisterCluster() {
        String instanceId= "instanceID";
        String clusterName = "clusterName";
        DBProxyTarget instance = new DBProxyTarget().withRdsResourceId(instanceId).withType("RDS_INSTANCE");
        DBProxyTarget cluster = new DBProxyTarget().withRdsResourceId(clusterName).withType("TRACKED_CLUSTER");
        DescribeDBProxyTargetsResult describeResult = new DescribeDBProxyTargetsResult()
                                                              .withTargets(instance, cluster);
        doReturn(describeResult).when(proxy).injectCredentialsAndInvoke(any(DescribeDBProxyTargetsRequest.class), any());
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

        ArgumentCaptor<DeregisterDBProxyTargetsRequest> captor = ArgumentCaptor.forClass(DeregisterDBProxyTargetsRequest.class);
        verify(proxy).injectCredentialsAndInvoke(any(DescribeDBProxyTargetsRequest.class), any());
        verify(proxy, times(2)).injectCredentialsAndInvoke(captor.capture(), any());
        DeregisterDBProxyTargetsRequest deregisterDBProxyTargetsRequest = captor.getValue();
        assertThat(deregisterDBProxyTargetsRequest.getDBClusterIdentifiers()).isEqualTo(ImmutableList.of(clusterName));
        assertThat(deregisterDBProxyTargetsRequest.getDBInstanceIdentifiers().size()).isEqualTo(0);
    }
}
