package software.amazon.rds.dbproxytargetgroup;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.services.rds.model.DBProxy;
import software.amazon.awssdk.services.rds.model.DBProxyTarget;
import software.amazon.awssdk.services.rds.model.DBProxyTargetGroup;

@Builder(toBuilder = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CallbackContext {
    private DBProxy proxy;
    private DBProxyTargetGroup targetGroupStatus;
    private List<DBProxyTarget> targets;
    private boolean allTargetsHealthy;
    private boolean targetsDeregistered;

    private Integer stabilizationRetriesRemaining;
}
