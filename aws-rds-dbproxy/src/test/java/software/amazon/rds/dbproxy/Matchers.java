package software.amazon.rds.dbproxy;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.rds.model.DBProxy;

public class Matchers {
    public static void assertThatModelsAreEqual(final Object rawModel,
                                                final DBProxy sdkModel) {
        assertThat(rawModel).isInstanceOf(ResourceModel.class);
        ResourceModel model = (ResourceModel)rawModel;
        assertThat(model.getDBProxyName()).isEqualTo(sdkModel.getDBProxyName());
        assertThat(model.getDBProxyArn()).isEqualTo(sdkModel.getDBProxyArn());
        assertThat(model.getRoleArn()).isEqualTo(sdkModel.getRoleArn());
        assertThat(model.getVpcId()).isEqualTo(sdkModel.getVpcId());
        assertThat(model.getVpcSubnetIds()).isEqualTo(sdkModel.getVpcSubnetIds());
        assertThat(model.getVpcSecurityGroupIds()).isEqualTo(sdkModel.getVpcSecurityGroupIds());
    }
}
