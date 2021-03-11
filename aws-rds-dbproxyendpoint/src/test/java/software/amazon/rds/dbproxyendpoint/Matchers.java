package software.amazon.rds.dbproxyendpoint;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.rds.model.DBProxyEndpoint;

public class Matchers {
    public static void assertThatModelsAreEqual(final Object rawModel,
                                                final DBProxyEndpoint sdkModel) {
        assertThat(rawModel).isInstanceOf(ResourceModel.class);
        ResourceModel model = (ResourceModel)rawModel;
        assertThat(model.getDBProxyEndpointName()).isEqualTo(sdkModel.getDBProxyEndpointName());
        assertThat(model.getDBProxyEndpointArn()).isEqualTo(sdkModel.getDBProxyEndpointArn());
        assertThat(model.getDBProxyName()).isEqualTo(sdkModel.getDBProxyName());
        assertThat(model.getVpcId()).isEqualTo(sdkModel.getVpcId());
        assertThat(model.getVpcSecurityGroupIds()).isEqualTo(sdkModel.getVpcSecurityGroupIds());
        assertThat(model.getVpcSubnetIds()).isEqualTo(sdkModel.getVpcSubnetIds());
        assertThat(model.getEndpoint()).isEqualTo(sdkModel.getEndpoint());
        assertThat(model.getTargetRole()).isEqualTo(sdkModel.getTargetRole());
        assertThat(model.getIsDefault()).isEqualTo(sdkModel.getIsDefault());
    }
}
