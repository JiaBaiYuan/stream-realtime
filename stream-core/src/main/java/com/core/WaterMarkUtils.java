package com.core;

import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * @author weikaijun
 * @date 2022-07-05 15:34
 **/
@Slf4j
public class WaterMarkUtils {

    private static final Logger logger = LoggerFactory.getLogger(WaterMarkUtils.class);

    // =========================================================
    // 1️⃣ ETH 告警流
    public static WatermarkStrategy<JSONObject> getEthWarnWaterMark(long durationSeconds) {
        return WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(durationSeconds))
                .withTimestampAssigner((record, ts) -> {
                    long time;
                    time = record.containsKey("block_timestamp")
                            ? record.getLong("block_timestamp")
                            : record.getLong("timestamp");
                    return time * 1000;
                });
    }

    // =========================================================
    // 2️⃣ ETH 流动性窗口流
    public static WatermarkStrategy<List<JSONObject>> getEthLiquidityWaterMark(long durationSeconds) {
        return WatermarkStrategy
                .<List<JSONObject>>forBoundedOutOfOrderness(Duration.ofSeconds(durationSeconds))
                .withTimestampAssigner((list, ts) -> {
                    JSONObject record = list.get(0);
                    return record.getLong("window_start_time");
                });
    }

    // =========================================================
    // 3️⃣ 通用字符串 JSON 流（指定时间字段）
    public static WatermarkStrategy<String> publicAssignWatermarkStrategy(String timestampField, long maxOutOfOrderlessSeconds) {
        return WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(maxOutOfOrderlessSeconds))
                .withTimestampAssigner((event, timestamp) -> {
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(event);
                        if (event != null && jsonObject.containsKey(timestampField)) {
                            return jsonObject.getLong(timestampField);
                        }
                        return 0L;
                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.error("Failed to parse event or get field '" + timestampField + "': " + event);
                        return 0L;
                    }
                });
    }

    // =========================================================
    // ✅ 4️⃣ 通用构建器，用于你的主任务 (DbusLogETLMetricTask)
    public static <T> WatermarkStrategy<T> buildWatermarkStrategy(
            java.util.function.Function<T, Long> timestampExtractor,
            long maxOutOfOrderMillis) {

        return WatermarkStrategy
                .<T>forBoundedOutOfOrderness(Duration.ofMillis(maxOutOfOrderMillis))
                .withTimestampAssigner((event, ts) -> {
                    try {
                        Long t = timestampExtractor.apply(event);
                        return t == null ? System.currentTimeMillis() : t;
                    } catch (Exception e) {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static class JSONObjectTsExtractor implements java.util.function.Function<JSONObject, Long>, java.io.Serializable {
        @Override
        public Long apply(JSONObject value) {
            return value.getLong("ts_ms");
        }
    }

}