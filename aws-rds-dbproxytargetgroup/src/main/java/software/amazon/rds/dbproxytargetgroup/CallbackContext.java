package software.amazon.rds.dbproxytargetgroup;

import java.util.List;

import com.amazonaws.services.rds.model.DBProxy;
import com.amazonaws.services.rds.model.DBProxyTarget;
import com.amazonaws.services.rds.model.DBProxyTargetGroup;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder(toBuilder = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CallbackContext {
    private DBProxy proxy;
    private DBProxyTargetGroup targetGroupStatus;
    private List<DBProxyTarget> targets;
    private boolean targetsDeregistered;

    private Integer stabilizationRetriesRemaining;
}
