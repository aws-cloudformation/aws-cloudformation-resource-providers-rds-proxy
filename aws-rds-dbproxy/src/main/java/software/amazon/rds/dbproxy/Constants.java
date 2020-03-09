package software.amazon.rds.dbproxy;

import java.util.List;

import com.google.common.collect.ImmutableList;

public class Constants {
    public static final String AVAILABLE_PROXY_STATE = "available";
    public static final List<String> TERMINAL_FAILURE_STATES = ImmutableList.of("incompatible-network",
                                                                                 "insufficient-resource-limits");
    public static final int NUMBER_OF_STATE_POLL_RETRIES = 240;
    public static final int POLL_RETRY_DELAY_IN_MS = 5000;
}
