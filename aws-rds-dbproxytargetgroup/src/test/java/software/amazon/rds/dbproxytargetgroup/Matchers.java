package software.amazon.rds.dbproxytargetgroup;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.rds.model.DBProxyTargetGroup;

public class Matchers {
    public static void assertThatModelsAreEqual(final Object rawModel,
                                                final DBProxyTargetGroup sdkModel) {
        assertThat(rawModel).isInstanceOf(ResourceModel.class);
        ResourceModel model = (ResourceModel)rawModel;
        assertThat(model.getDbProxyName()).isEqualTo(sdkModel.getDBProxyName());
        assertThat(model.getTargetGroupName()).isEqualTo(sdkModel.getTargetGroupName());
    }
}
