package software.amazon.rds.dbproxyendpoint;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.services.rds.model.DBProxyEndpoint;

@Builder(toBuilder = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CallbackContext {
    private DBProxyEndpoint proxyEndpoint;
    private boolean deleted;
    private Integer stabilizationRetriesRemaining;
    private boolean tagsDeregistered;
    private boolean tagsRegistered;
}
