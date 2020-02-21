package software.amazon.rds.dbproxytargetgroup;

import java.util.Map;
import org.json.JSONObject;
import org.json.JSONTokener;

class Configuration extends BaseConfiguration {

    public Configuration() {
        super("aws-rds-dbproxytargetgroup.json");
    }
}
