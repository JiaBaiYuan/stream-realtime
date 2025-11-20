package com.stream.realtime.lululemon;

import com.core.ConfigUtils;

public class DbusLogETLMetricCalculateV2 {
    private static final String KAFKA_BOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String KAFKA_LOG_TOPIC = "realtime_v3_logs";
    private static final String DORIS_FE_IP = ConfigUtils.getString("doris.fe.ip");
    private static final String DORIS_LOG_TABLE_NAME = ConfigUtils.getString("doris.log.device.table");
    private static final String DORIS_REGION_TABLE_NAME = ConfigUtils.getString("doris.log.region.table");
    private static final String DORIS_SEARCH_TABLE_NAME = ConfigUtils.getString("doris.log.search.table");
    private static final String DORIS_PAGE_TABLE_NAME = ConfigUtils.getString("doris.log.page.table");
    private static final String DORIS_USERNAME = ConfigUtils.getString("doris.user.name");
    private static final String DORIS_PASSWORD = ConfigUtils.getString("doris.user.password");
    private static final int DORIS_BUFFER_COUNT = 2;
    private static final int DORIS_BUFFER_SIZE = 1024;
}
