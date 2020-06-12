package software.amazon.rds.dbproxytargetgroup;

import static software.amazon.rds.dbproxytargetgroup.Constants.AVAILABLE_STATE;
import static software.amazon.rds.dbproxytargetgroup.Constants.RDS_INSTANCE;
import static software.amazon.rds.dbproxytargetgroup.Constants.TRACKED_CLUSTER;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.amazonaws.services.rds.model.ConnectionPoolConfigurationInfo;
import com.amazonaws.services.rds.model.DBProxyTarget;
import com.amazonaws.services.rds.model.DBProxyTargetGroup;
import com.amazonaws.services.rds.model.DescribeDBProxyTargetsResult;

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

    static boolean validateHealth(DescribeDBProxyTargetsResult describeResult) {
        for (DBProxyTarget target:describeResult.getTargets()) {
            // Tracked cluster do not currently have their own health state, adding optional
            // health checks for future proofing
            if (target.getType().equalsIgnoreCase(TRACKED_CLUSTER)){
                if (target.getTargetHealth() != null
                    && target.getTargetHealth().getState() != null
                    && !target.getTargetHealth().getState().equalsIgnoreCase(AVAILABLE_STATE)) {
                    return false;
                }
            }

            if (target.getType().equalsIgnoreCase(RDS_INSTANCE)){
                if (!target.getTargetHealth().getState().equalsIgnoreCase(AVAILABLE_STATE)) {
                    return false;
                }
            }
        }

        return true;
    }
}
