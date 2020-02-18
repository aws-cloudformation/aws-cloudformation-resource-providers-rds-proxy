package software.amazon.rds.dbproxy;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.rds.model.DBProxy;

public class Matchers {
    public static void assertThatModelsAreEqual(final Object rawModel,
                                                final DBProxy sdkModel) {
        assertThat(rawModel).isInstanceOf(ResourceModel.class);
        ResourceModel model = (ResourceModel)rawModel;
        assertThat(model.getDbProxyName()).isEqualTo(sdkModel.getDBProxyName());
    }
}
