#!/usr/bin/env python3
# coding: utf-8

import os
import json
import logging
from pyflink.datastream import StreamExecutionEnvironment, FlatMapFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.common import Types
import jpype
import jpype.imports
from jpype.types import *

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SensitiveWordDetector(FlatMapFunction):
    """
    Flink FlatMapFunction 用于检测敏感评论
    """

    def __init__(self, sensitive_words_file="../resource/data/sensitiveword/suspected-sensitive-words.txt"):
        self.sensitive_words_file = sensitive_words_file
        self.sensitive_words = set()

    def open(self, runtime_context: RuntimeContext):
        """初始化敏感词库"""
        logger.info("正在加载敏感词库...")
        try:
            with open(self.sensitive_words_file, 'r', encoding='utf-8') as f:
                for line in f:
                    word = line.strip()
                    if word and len(word) > 1:  # 过滤空行和单字
                        self.sensitive_words.add(word)
                        # 同时添加小写版本
                        self.sensitive_words.add(word.lower())

            logger.info(f"敏感词库加载完成，共 {len(self.sensitive_words)} 个敏感词")
        except Exception as e:
            logger.error(f"加载敏感词库失败: {e}")
            # 使用一些默认敏感词作为备用
            self.sensitive_words = {
                '狐狸性爱通讯', '亲民党', '英语枪手', '中央无能', '考试枪手',
                '中国当局', '伊斯兰运动', '猥亵', '建国党', '华主席'
            }

    def detect_sensitive_words(self, text):
        """检测文本中的敏感词"""
        if not text:
            return []

        text_lower = text.lower()
        detected_words = []

        for word in self.sensitive_words:
            if word in text_lower:
                detected_words.append(word)

        return detected_words

    def flat_map(self, value):
        """
        处理每条评论数据
        输入: JSON字符串
        输出: 处理后的数据(可能多条)
        """
        try:
            # 解析JSON数据
            comment_data = json.loads(value)

            # 提取评论内容
            user_comment = comment_data.get('user_comment', '')
            order_id = comment_data.get('order_id', '')
            user_id = comment_data.get('user_id', '')

            # 检测敏感词
            sensitive_words = self.detect_sensitive_words(user_comment)
            is_sensitive = len(sensitive_words) > 0

            # 丰富输出数据
            output_data = {
                **comment_data,
                'is_sensitive': is_sensitive,
                'sensitive_words': sensitive_words,
                'sensitive_word_count': len(sensitive_words),
                'process_timestamp': int(os.times().elapsed * 1000),  # 处理时间戳
                'action_taken': 'blocked' if is_sensitive else 'passed'
            }

            # 输出处理结果
            yield json.dumps(output_data, ensure_ascii=False)

            # 如果是敏感评论，额外生成告警信息
            if is_sensitive:
                alert_data = {
                    'alert_type': 'SENSITIVE_COMMENT',
                    'order_id': order_id,
                    'user_id': user_id,
                    'sensitive_words': sensitive_words,
                    'comment_preview': user_comment[:100] + '...' if len(user_comment) > 100 else user_comment,
                    'alert_timestamp': int(os.times().elapsed * 1000),
                    'severity': 'HIGH'
                }
                yield json.dumps(alert_data, ensure_ascii=False)

        except Exception as e:
            logger.error(f"处理评论数据失败: {e}, 原始数据: {value}")
            # 输出错误信息
            error_data = {
                'error': str(e),
                'raw_data': value,
                'process_timestamp': int(os.times().elapsed * 1000)
            }
            yield json.dumps(error_data, ensure_ascii=False)

class CommentDataGenerator:
    """
    评论数据处理主类
    """

    def __init__(self):
        self.env = StreamExecutionEnvironment.get_execution_environment()
        # 设置并行度
        self.env.set_parallelism(1)

        # 数据库配置（从环境变量获取）
        self.sqlserver_config = {
            'host': os.getenv("sqlserver_ip", "localhost"),
            'port': os.getenv("sqlserver_port", "1433"),
            'database': os.getenv("sqlserver_db", "realtime_v3"),
            'username': os.getenv("sqlserver_user_name", "sa"),
            'password': os.getenv("sqlserver_user_pwd", "password")
        }

        # Kafka配置
        self.kafka_config = {
            'bootstrap_servers': os.getenv("kafka_bootstrap_servers", "cdh01:9092,cdh02:9092,cdh03:9092"),
            'source_topic': 'realtime_v3_logs',
            'sink_topic': 'processed_comments',
            'alert_topic': 'sensitive_alerts'
        }

    def create_kafka_source(self):
        """创建Kafka数据源"""
        return KafkaSource.builder() \
            .set_bootstrap_servers(self.kafka_config['bootstrap_servers']) \
            .set_topics(self.kafka_config['source_topic']) \
            .set_group_id("flink_comment_processor") \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

    def create_jdbc_sink(self):
        """创建JDBC Sink用于写入黑名单"""
        jdbc_connection_options = JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
            .with_url(f"jdbc:sqlserver://{self.sqlserver_config['host']}:{self.sqlserver_config['port']};"
                      f"databaseName={self.sqlserver_config['database']}") \
            .with_driver_name("com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .with_user_name(self.sqlserver_config['username']) \
            .with_password(self.sqlserver_config['password']) \
            .build()

        jdbc_execution_options = JdbcExecutionOptions.builder() \
            .with_batch_interval_ms(1000) \
            .with_batch_size(100) \
            .with_max_retries(3) \
            .build()

        return JdbcSink.sink(
            "INSERT INTO user_blacklist (user_id, order_id, sensitive_comment, sensitive_words, detected_time, ds, ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
            self.create_statement_builder(),
            jdbc_connection_options,
            jdbc_execution_options
        )

    def create_statement_builder(self):
        """创建JDBC语句构建器"""
        from pyflink.datastream.connectors.jdbc import JdbcStatementBuilder

        class BlacklistStatementBuilder(JdbcStatementBuilder):
            def accept(self, statement, row):
                statement.setString(1, row[0])  # user_id
                statement.setString(2, row[1])  # order_id
                statement.setString(3, row[2])  # sensitive_comment
                statement.setString(4, row[3])  # sensitive_words
                statement.setLong(5, row[4])    # detected_time
                statement.setString(6, row[5])  # ds
                statement.setLong(7, row[6])    # ts

        return BlacklistStatementBuilder()

    def process_comments_to_blacklist(self, data_stream):
        """处理敏感评论到黑名单"""
        from pyflink.common import Row

        def map_to_blacklist(comment_json):
            """将评论数据映射为黑名单记录"""
            try:
                comment_data = json.loads(comment_json)

                if comment_data.get('is_sensitive', False):
                    # 准备黑名单数据
                    sensitive_comment = comment_data.get('user_comment', '')[:1000]  # 限制长度
                    sensitive_words = ','.join(comment_data.get('sensitive_words', []))

                    return Row(
                        comment_data.get('user_id', ''),
                        comment_data.get('order_id', ''),
                        sensitive_comment,
                        sensitive_words,
                        comment_data.get('process_timestamp', 0),
                        comment_data.get('ds', ''),
                        comment_data.get('ts', 0)
                    )
                return None
            except Exception as e:
                logger.error(f"映射黑名单数据失败: {e}")
                return None

        # 过滤并映射数据
        blacklist_stream = data_stream \
            .map(map_to_blacklist) \
            .filter(lambda x: x is not None)

        # 写入黑名单表
        blacklist_stream.sink_to(self.create_jdbc_sink())

        return blacklist_stream

    def run(self):
        """运行Flink处理作业"""
        logger.info("🚀 启动Flink敏感评论检测作业...")

        try:
            # 创建数据源
            source = self.create_kafka_source()
            data_stream = self.env.from_source(
                source,
                WatermarkStrategy.no_watermarks(),
                "Kafka Source"
            )

            # 敏感词检测处理
            processed_stream = data_stream \
                .flat_map(SensitiveWordDetector()) \
                .name("SensitiveWordDetection")

            # 输出处理结果到Kafka（这里简化，实际需要配置Kafka Sink）
            processed_stream.print().name("ProcessedOutput")

            # 处理敏感评论到黑名单
            self.process_comments_to_blacklist(processed_stream)

            # 执行作业
            self.env.execute("Real-time Sensitive Comment Detection")

        except Exception as e:
            logger.error(f"Flink作业执行失败: {e}")
            raise

def create_blacklist_table_sql():
    """
    创建黑名单表的SQL语句
    """
    return """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='user_blacklist' AND xtype='U')
    CREATE TABLE user_blacklist (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        user_id NVARCHAR(255) NOT NULL,
        order_id NVARCHAR(255) NOT NULL,
        sensitive_comment NVARCHAR(MAX),
        sensitive_words NVARCHAR(MAX),
        detected_time BIGINT,
        ds NVARCHAR(20),
        ts BIGINT,
        created_time DATETIME2 DEFAULT GETDATE(),
        status NVARCHAR(50) DEFAULT 'ACTIVE'
    );
    
    -- 创建索引
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_user_blacklist_user_id')
    CREATE INDEX idx_user_blacklist_user_id ON user_blacklist(user_id);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_user_blacklist_order_id')  
    CREATE INDEX idx_user_blacklist_order_id ON user_blacklist(order_id);
    
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_user_blacklist_created_time')
    CREATE INDEX idx_user_blacklist_created_time ON user_blacklist(created_time);
    """

def setup_database():
    """设置数据库表"""
    import pymssql

    try:
        conn = pymssql.connect(
            server=os.getenv("sqlserver_ip"),
            port=os.getenv("sqlserver_port"),
            user=os.getenv("sqlserver_user_name"),
            password=os.getenv("sqlserver_user_pwd"),
            database=os.getenv("sqlserver_db")
        )

        with conn.cursor() as cursor:
            cursor.execute(create_blacklist_table_sql())
            conn.commit()
            logger.info("✅ 黑名单表创建/验证完成")

    except Exception as e:
        logger.error(f"数据库设置失败: {e}")
        raise

if __name__ == "__main__":
    # 设置数据库
    setup_database()

    # 启动Flink处理作业
    processor = CommentDataGenerator()
    processor.run()