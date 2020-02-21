package software.amazon.rds.dbproxytargetgroup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.amazonaws.services.rds.model.ConnectionPoolConfigurationInfo;
import com.amazonaws.services.rds.model.DBProxyTargetGroup;

public class Utility {
    static <A, B> List<B> map(Collection<A> xs, Function<A, B> f) {
        return Optional.ofNullable(xs).orElse(Collections.emptyList()).stream().map(f)
                       .collect(Collectors.toList());
    }

    public static ResourceModel resultToModel(DBProxyTargetGroup targetGroup){
            return ResourceModel
                   .builder()
                   .dbProxyName(targetGroup.getDBProxyName())
                   .targetGroupName(targetGroup.getTargetGroupName())
                   .connectionPoolConfigurationInfo(resultToModel(targetGroup.getConnectionPoolConfig()))
                   .build();
    }

    public static ConnectionPoolConfigurationInfoFormat resultToModel(ConnectionPoolConfigurationInfo connectionPoolConfig) {
        return ConnectionPoolConfigurationInfoFormat
                       .builder()
                       .maxConnectionsPercent(connectionPoolConfig.getMaxConnectionsPercent())
                       .maxIdleConnectionsPercent(connectionPoolConfig.getMaxIdleConnectionsPercent())
                       .connectionBorrowTimeout(connectionPoolConfig.getConnectionBorrowTimeout())
                       .sessionPinningFilters(connectionPoolConfig.getSessionPinningFilters())
                       .initQuery(connectionPoolConfig.getInitQuery())
                       .build();
    }

    static List<String> getClusters(ResourceModel model) {
        if (model.getClusterIdentifiers() != null && model.getClusterIdentifiers().size() > 0) {
            return model.getClusterIdentifiers();
        }
        return new ArrayList<>();
    }

    static List<String> getInstances(ResourceModel model) {
        if (model.getInstanceIdentifiers() != null && model.getInstanceIdentifiers().size() > 0) {
            return model.getInstanceIdentifiers();
        }
        return new ArrayList<>();
    }

    static List<String> listDifference(List<String> list1, List<String> list2) {
        if (list1.size() > 0 && list2.size() > 0) {
            list1.removeAll(list2);
        }
        return list1;
    }


}
