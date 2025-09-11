package software.amazon.rds.dbproxytargetgroup;

import static org.assertj.core.api.Assertions.assertThat;

import software.amazon.awssdk.services.rds.model.DBProxyTargetGroup;

public class Matchers {
    public static void assertThatModelsAreEqual(final Object rawModel,
                                                final DBProxyTargetGroup sdkModel) {
        assertThat(rawModel).isInstanceOf(ResourceModel.class);
        ResourceModel model = (ResourceModel)rawModel;
        assertThat(model.getDBProxyName()).isEqualTo(sdkModel.dbProxyName());
        assertThat(model.getTargetGroupName()).isEqualTo(sdkModel.targetGroupName());
    }
}
