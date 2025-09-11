package software.amazon.rds.dbproxyendpoint;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBProxyEndpoint;
import software.amazon.awssdk.services.rds.model.DbProxyEndpointNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyEndpointsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyEndpointsResponse;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ReadHandler extends BaseHandler<CallbackContext> {
    private AmazonWebServicesClientProxy clientProxy;
    private RdsClient rdsClient;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        clientProxy = proxy;
        rdsClient = RdsClient.create();

        final ResourceModel model;
        try {
            model = describeDBProxyEndpoint(request.getDesiredResourceState().getDBProxyEndpointName());
        } catch (DbProxyEndpointNotFoundException | CfnNotFoundException e) {
            return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.NotFound);
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModel(model)
                .status(OperationStatus.SUCCESS)
                .build();
    }

    private ResourceModel describeDBProxyEndpoint(final String proxyEndpointName) {
        DescribeDbProxyEndpointsRequest request = DescribeDbProxyEndpointsRequest.builder().dbProxyEndpointName(proxyEndpointName).build();

        final DescribeDbProxyEndpointsResponse response = clientProxy.injectCredentialsAndInvokeV2(request, rdsClient::describeDBProxyEndpoints);

        if (response != null && response.dbProxyEndpoints() != null && response.dbProxyEndpoints().size() == 1) {
            DBProxyEndpoint proxyEndpoint = response.dbProxyEndpoints().get(0);
            ResourceModel resourceModel = Utility.resultToModel(proxyEndpoint);
            ListTagsForResourceRequest tagRequest = ListTagsForResourceRequest.builder().resourceName(proxyEndpoint.dbProxyEndpointArn()).build();

            final ListTagsForResourceResponse tagResponse = clientProxy.injectCredentialsAndInvokeV2(tagRequest, rdsClient::listTagsForResource);
            if (tagResponse != null && tagResponse.tagList()!= null && !tagResponse.tagList().isEmpty()) {
                resourceModel.setTags(convertTags(tagResponse.tagList()));
            }
            return resourceModel;
        } else {
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, proxyEndpointName);
        }
    }

    private List<TagFormat> convertTags(List<Tag> tags) {
        List<TagFormat> convertedTags = new ArrayList<>();

        if (tags == null || tags.isEmpty()) {
            return convertedTags;
        }

        for(Tag tag : tags){
            convertedTags.add(TagFormat.builder()
                    .key(tag.key())
                    .value(tag.value())
                    .build());
        }

        return convertedTags;
    }
}
