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
    public static ResourceModel resultToModel(DBProxyTargetGroup targetGroup){
            return ResourceModel
                   .builder()
                   .dBProxyName(targetGroup.getDBProxyName())
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
        return Optional.ofNullable(model.getDBClusterIdentifiers()).orElse(new ArrayList<>());
    }

    static List<String> getInstances(ResourceModel model) {
        return Optional.ofNullable(model.getDBInstanceIdentifiers()).orElse(new ArrayList<>());
    }
}
