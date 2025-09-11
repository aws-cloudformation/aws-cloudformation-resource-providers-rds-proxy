package software.amazon.rds.dbproxy;

import software.amazon.awssdk.services.rds.model.DBProxy;
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
    private boolean deleted;
    private Integer stabilizationRetriesRemaining;
    private boolean tagsDeregistered;
    private boolean tagsRegistered;
}
