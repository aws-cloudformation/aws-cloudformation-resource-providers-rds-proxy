package software.amazon.rds.dbproxy;

import static org.assertj.core.api.Assertions.assertThat;

import software.amazon.awssdk.services.rds.model.DBProxy;

public class Matchers {
    public static void assertThatModelsAreEqual(final Object rawModel,
                                                final DBProxy sdkModel) {
        assertThat(rawModel).isInstanceOf(ResourceModel.class);
        ResourceModel model = (ResourceModel)rawModel;
        assertThat(model.getDBProxyName()).isEqualTo(sdkModel.dbProxyName());
        assertThat(model.getDBProxyArn()).isEqualTo(sdkModel.dbProxyArn());
        assertThat(model.getRoleArn()).isEqualTo(sdkModel.roleArn());
        assertThat(model.getVpcId()).isEqualTo(sdkModel.vpcId());
        assertThat(model.getVpcSubnetIds()).isEqualTo(sdkModel.vpcSubnetIds());
        assertThat(model.getVpcSecurityGroupIds()).isEqualTo(sdkModel.vpcSecurityGroupIds());
    }
}
