package software.amazon.rds.dbproxytargetgroup;

import static software.amazon.rds.dbproxytargetgroup.Constants.AVAILABLE_STATE;
import static software.amazon.rds.dbproxytargetgroup.Constants.RDS_INSTANCE;
import static software.amazon.rds.dbproxytargetgroup.Constants.TRACKED_CLUSTER;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import software.amazon.awssdk.services.rds.model.ConnectionPoolConfigurationInfo;
import software.amazon.awssdk.services.rds.model.DBProxyTarget;
import software.amazon.awssdk.services.rds.model.DBProxyTargetGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbProxyTargetsResponse;


public class Utility {
    public static ResourceModel resultToModel(DBProxyTargetGroup targetGroup){
            return ResourceModel
                   .builder()
                   .dBProxyName(targetGroup.dbProxyName())
                   .targetGroupName(targetGroup.targetGroupName())
                   .targetGroupArn(targetGroup.targetGroupArn())
                   .connectionPoolConfigurationInfo(resultToModel(targetGroup.connectionPoolConfig()))
                   .build();
    }

    public static ConnectionPoolConfigurationInfoFormat resultToModel(ConnectionPoolConfigurationInfo connectionPoolConfig) {
        return ConnectionPoolConfigurationInfoFormat
                       .builder()
                       .maxConnectionsPercent(connectionPoolConfig.maxConnectionsPercent())
                       .maxIdleConnectionsPercent(connectionPoolConfig.maxIdleConnectionsPercent())
                       .connectionBorrowTimeout(connectionPoolConfig.connectionBorrowTimeout())
                       .sessionPinningFilters(connectionPoolConfig.sessionPinningFilters())
                       .initQuery(connectionPoolConfig.initQuery())
                       .build();
    }

    static List<String> getClusters(ResourceModel model) {
        return Optional.ofNullable(model.getDBClusterIdentifiers()).orElse(new ArrayList<>());
    }

    static List<String> getInstances(ResourceModel model) {
        return Optional.ofNullable(model.getDBInstanceIdentifiers()).orElse(new ArrayList<>());
    }

    static boolean validateHealth(DescribeDbProxyTargetsResponse describeResponse) {
        for (DBProxyTarget target:describeResponse.targets()) {
            // Tracked cluster do not currently have their own health state, adding optional
            // health checks for future proofing
            if (target.type().toString().equalsIgnoreCase(TRACKED_CLUSTER)){
                if (target.targetHealth() != null
                    && target.targetHealth().state() != null
                    && !target.targetHealth().state().toString().equalsIgnoreCase(AVAILABLE_STATE)) {
                    return false;
                }
            }

            if (target.type().toString().equalsIgnoreCase(RDS_INSTANCE)){
                if (target.targetHealth() == null ||
                    !target.targetHealth().state().toString().equalsIgnoreCase(AVAILABLE_STATE)) {
                    return false;
                }
            }
        }

        return true;
    }
}
