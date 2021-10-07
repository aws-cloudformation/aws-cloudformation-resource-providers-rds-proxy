package software.amazon.rds.dbproxy;

import com.amazonaws.services.rds.model.DBProxyNotFoundException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.DBProxy;
import com.amazonaws.services.rds.model.DescribeDBProxiesRequest;
import com.amazonaws.services.rds.model.DescribeDBProxiesResult;
import com.amazonaws.services.rds.model.ListTagsForResourceRequest;
import com.amazonaws.services.rds.model.ListTagsForResourceResult;
import com.amazonaws.services.rds.model.Tag;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ReadHandler extends BaseHandler<CallbackContext> {
    private AmazonWebServicesClientProxy clientProxy;
    private AmazonRDS rdsClient;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        clientProxy = proxy;
        rdsClient = AmazonRDSClientBuilder.defaultClient();

        final ResourceModel model;
        try {
            model = describeDBProxy(request.getDesiredResourceState().getDBProxyName());
        } catch (DBProxyNotFoundException | CfnNotFoundException e) {
            return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.NotFound);
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                       .resourceModel(model)
                       .status(OperationStatus.SUCCESS)
                       .build();
    }

    private ResourceModel describeDBProxy(final String proxyName) {
        DescribeDBProxiesRequest request = new DescribeDBProxiesRequest().withDBProxyName(proxyName);

        final DescribeDBProxiesResult result = clientProxy.injectCredentialsAndInvoke(request, rdsClient::describeDBProxies);

        if (result != null && result.getDBProxies() != null && result.getDBProxies().size() == 1) {
            DBProxy proxy = result.getDBProxies().get(0);
            ResourceModel resourceModel = Utility.resultToModel(proxy);
            ListTagsForResourceRequest tagRequest = new ListTagsForResourceRequest().withResourceName(proxy.getDBProxyArn());

            final ListTagsForResourceResult tagResult = clientProxy.injectCredentialsAndInvoke(tagRequest, rdsClient::listTagsForResource);
            if (tagResult != null && tagResult.getTagList()!= null && tagResult.getTagList().size() > 0) {
                resourceModel.setTags(convertTags(tagResult.getTagList()));
            }
            return resourceModel;
        } else {
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, proxyName);
        }
    }

    private List<TagFormat> convertTags(List<Tag> tags) {
        List<TagFormat> convertedTags = new ArrayList<>();

        if (tags == null || tags.size() == 0) {
            return convertedTags;
        }

        for(Tag tag : tags){
            convertedTags.add(TagFormat.builder()
                                       .key(tag.getKey())
                                       .value(tag.getValue())
                                       .build());
        }

        return convertedTags;
    }
}
