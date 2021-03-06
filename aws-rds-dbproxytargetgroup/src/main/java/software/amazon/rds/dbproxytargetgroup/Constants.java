package software.amazon.rds.dbproxytargetgroup;

public class Constants {
    public static final String DELETING_PROXY_STATE = "deleting";
    public static final String AVAILABLE_STATE = "AVAILABLE";
    public static final String TRACKED_CLUSTER = "TRACKED_CLUSTER";
    public static final String RDS_INSTANCE = "RDS_INSTANCE";
    public static final int NUMBER_OF_STATE_POLL_RETRIES = 240;
    public static final int POLL_RETRY_DELAY_IN_MS = 5000;
}
