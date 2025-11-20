package com.stream.realtime.lululemon;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class SqlServerToKafka {

    // SQL Server 配置（已修复 SSL）
    private static final String JDBC_URL =
            "jdbc:sqlserver://192.168.200.102:1433;" +
                    "databaseName=realtime_v3;" +
                    "encrypt=false;" +
                    "trustServerCertificate=true;";

    private static final String JDBC_USER = "sa";
    private static final String JDBC_PWD = "Xy0511./";

    // Kafka 配置
    private static final String KAFKA_BOOTSTRAP = "cdh01:9092,cdh02:9092,cdh03:9092";
    private static final String KAFKA_TOPIC = "realtime_v3_logs_order_info";

    // 用于追踪最新的 ID
    private static long lastMaxId = 0L;

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        // Kafka Producer 配置
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        System.out.println("🚀 SQLServer → Kafka 实时同步程序已启动......");

        // 主循环，每 3 秒轮询一次
        while (true) {
            syncNewRows(producer);
            TimeUnit.SECONDS.sleep(3);
        }
    }

    /**
     * 实时同步 SQLServer 数据到 Kafka
     */
    private static void syncNewRows(KafkaProducer<String, String> producer) {

        String sql = "SELECT * FROM dbo.oms_order_dtl_enhanced2 WHERE id > ? ORDER BY id ASC";

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PWD);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setLong(1, lastMaxId);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {

                Map<String, Object> row = new LinkedHashMap<>();
                row.put("id", rs.getInt("id"));
                row.put("order_id", rs.getString("order_id"));
                row.put("user_id", rs.getString("user_id"));
                row.put("user_name", rs.getString("user_name"));
                row.put("product_id", rs.getString("product_id"));
                row.put("total_amount", rs.getString("total_amount"));
                row.put("product_name", rs.getString("product_name"));
                row.put("brand", rs.getString("brand"));
                row.put("english_name", rs.getString("english_name"));
                row.put("chinese_name", rs.getString("chinese_name"));
                row.put("ai_review", rs.getString("ai_review"));
                row.put("has_sensitive_content", rs.getString("has_sensitive_content"));
                row.put("sensitive_words", rs.getString("sensitive_words"));
                row.put("violation_handled", rs.getString("violation_handled"));
                row.put("ds", rs.getString("ds"));
                row.put("created_time", rs.getTimestamp("created_time") != null
                        ? rs.getTimestamp("created_time").toString()
                        : null);

                String json = mapper.writeValueAsString(row);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(KAFKA_TOPIC, String.valueOf(row.get("id")), json);

                producer.send(record);
                System.out.println("发送到 Kafka → " + json);

                lastMaxId = rs.getInt("id");
            }

        } catch (Exception e) {
            System.err.println("同步 SQLServer → Kafka 出错: " + e.getMessage());
        }
    }
}