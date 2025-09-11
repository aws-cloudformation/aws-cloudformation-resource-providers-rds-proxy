package software.amazon.rds.dbproxy;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbProxyNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbProxiesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxiesResponse;
import software.amazon.awssdk.services.rds.model.DBProxy;
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
            model = describeDBProxy(request.getDesiredResourceState().getDBProxyName());
        } catch (DbProxyNotFoundException | CfnNotFoundException e) {
            return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.NotFound);
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                       .resourceModel(model)
                       .status(OperationStatus.SUCCESS)
                       .build();
    }

    private ResourceModel describeDBProxy(final String proxyName) {
        DescribeDbProxiesRequest request = DescribeDbProxiesRequest.builder().dbProxyName(proxyName).build();

        final DescribeDbProxiesResponse response = clientProxy.injectCredentialsAndInvokeV2(request, rdsClient::describeDBProxies);

        if (response != null && response.dbProxies() != null && response.dbProxies().size() == 1) {
            DBProxy proxy = response.dbProxies().get(0);
            ResourceModel resourceModel = Utility.resultToModel(proxy);
            ListTagsForResourceRequest tagRequest = ListTagsForResourceRequest.builder().resourceName(proxy.dbProxyArn()).build();

            final ListTagsForResourceResponse tagsResponse = clientProxy.injectCredentialsAndInvokeV2(tagRequest, rdsClient::listTagsForResource);
            if (tagsResponse != null && tagsResponse.tagList()!= null && !tagsResponse.tagList().isEmpty()) {
                resourceModel.setTags(convertTags(tagsResponse.tagList()));
            }
            return resourceModel;
        } else {
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, proxyName);
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
