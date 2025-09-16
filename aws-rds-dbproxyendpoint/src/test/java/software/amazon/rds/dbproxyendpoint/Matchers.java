package software.amazon.rds.dbproxyendpoint;

import static org.assertj.core.api.Assertions.assertThat;

import software.amazon.awssdk.services.rds.model.DBProxyEndpoint;

public class Matchers {
    public static void assertThatModelsAreEqual(final Object rawModel,
                                                final DBProxyEndpoint sdkModel) {
        assertThat(rawModel).isInstanceOf(ResourceModel.class);
        ResourceModel model = (ResourceModel)rawModel;
        assertThat(model.getDBProxyEndpointName()).isEqualTo(sdkModel.dbProxyEndpointName());
        assertThat(model.getDBProxyEndpointArn()).isEqualTo(sdkModel.dbProxyEndpointArn());
        assertThat(model.getDBProxyName()).isEqualTo(sdkModel.dbProxyName());
        assertThat(model.getVpcId()).isEqualTo(sdkModel.vpcId());
        assertThat(model.getVpcSecurityGroupIds()).isEqualTo(sdkModel.vpcSecurityGroupIds());
        assertThat(model.getVpcSubnetIds()).isEqualTo(sdkModel.vpcSubnetIds());
        assertThat(model.getEndpoint()).isEqualTo(sdkModel.endpoint());
        assertThat(model.getTargetRole()).isEqualTo(sdkModel.targetRole());
        assertThat(model.getIsDefault()).isEqualTo(sdkModel.isDefault());
    }
}
