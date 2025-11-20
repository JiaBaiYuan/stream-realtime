import pandas as pd
import time
import logging
import pyodbc
import re
import random   # ← 加上这一行
from datetime import datetime
# ---------------------------
# 日志配置
# ---------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ===========================
# 敏感词分类器
# ===========================
class SensitiveWordClassifier:
    def __init__(self):
        self.p0_words = set()
        self.p1_words = set()
        self.p2_words = set()
        self.all_words = {}

    def load_sensitive_words(self, file_path: str):
        """从文件加载敏感词（格式：词,等级）"""
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or "," not in line:
                    continue
                word, level = line.split(",", 1)
                word, level = word.strip(), level.strip()
                if level == "P0":
                    self.p0_words.add(word)
                elif level == "P1":
                    self.p1_words.add(word)
                else:
                    self.p2_words.add(word)
                self.all_words[word] = level

        logger.info(f"敏感词加载完成：P0={len(self.p0_words)} P1={len(self.p1_words)} P2={len(self.p2_words)}")

    def check_sensitive(self, text: str):
        """检测文本中的敏感词"""
        hit_words = []
        level_hit = False

        for w, level in self.all_words.items():
            if w in text:
                hit_words.append(f"{w}({level})")
                if level in ["P0", "P1"]:
                    level_hit = True

        return level_hit, ",".join(hit_words)

    def get_statistics(self):
        return {
            "p0_count": len(self.p0_words),
            "p1_count": len(self.p1_words),
            "p2_count": len(self.p2_words),
            "total_count": len(self.all_words)
        }


# ===========================
# SQL Server 连接
# ===========================
def get_sqlserver_conn():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=192.168.200.102,1433;"
        "DATABASE=realtime_v3;"
        "UID=sa;"
        "PWD=Xy0511./;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


# ===========================
# 检查并创建增强表
# ===========================
def check_and_create_table(conn):
    """自动建表（包含 user_name + total_amount）"""
    cursor = conn.cursor()

    cursor.execute("""
    IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name='oms_order_dtl_enhanced2')
    CREATE TABLE oms_order_dtl_enhanced2 (
        id INT IDENTITY(1,1) PRIMARY KEY,
        order_id NVARCHAR(255),
        user_id NVARCHAR(255),
        user_name NVARCHAR(255),
        total_amount DECIMAL(18,2),
        product_id NVARCHAR(255),
        product_name NVARCHAR(500),
        brand NVARCHAR(255),
        english_name NVARCHAR(255),
        chinese_name NVARCHAR(300),
        ai_review NVARCHAR(1000),
        has_sensitive_content BIT,
        sensitive_words NVARCHAR(500),
        violation_handled BIT DEFAULT 0,
        ds NVARCHAR(255),
        created_time DATETIME DEFAULT GETDATE()
    )
    """)
    conn.commit()

    logger.info("表检查完成（包含 user_name + total_amount）")
    # ===========================
# 产品信息拆分（brand / english_name / chinese_name）
# ===========================
def fix_encoding(text):
    if not isinstance(text, str):
        return text
    try:
        return text
    except Exception:
        return text

def split_product_info(text: str):
    """根据 '丨' 和首个中文汉字拆分 product_name -> brand / english_name / chinese_name"""
    if not text or not isinstance(text, str):
        return {"brand": "", "english_name": "", "chinese_name": ""}

    cleaned = fix_encoding(text).strip()
    # 常见格式：品牌丨EnglishName中文说明
    if "丨" in cleaned:
        parts = cleaned.split("丨", 1)
        brand = parts[0].strip()
        rest = parts[1].strip()
        # 尝试拆分英文+中文
        m = re.match(r'^([^\u4e00-\u9fa5]*)(.*)$', rest)
        if m:
            eng = m.group(1).strip()
            chi = m.group(2).strip()
            return {"brand": brand, "english_name": eng, "chinese_name": chi}
        else:
            return {"brand": brand, "english_name": rest, "chinese_name": ""}
    else:
        # 没有分隔符，找到第一个中文字符索引
        for idx, ch in enumerate(cleaned):
            if '\u4e00' <= ch <= '\u9fff':
                return {"brand": "", "english_name": cleaned[:idx].strip(), "chinese_name": cleaned[idx:].strip()}
        return {"brand": "", "english_name": cleaned, "chinese_name": ""}


# ===========================
# 违规/黑名单记录写入（写入 user_violation_records 表）
# ===========================
def mark_violation_user(conn, user_id, order_id, sensitive_words, ai_review):
    try:
        cursor = conn.cursor()
        # 创建表（如果不存在）
        cursor.execute("""
        IF OBJECT_ID('user_violation_records', 'U') IS NULL
        CREATE TABLE user_violation_records (
            id INT IDENTITY(1,1) PRIMARY KEY,
            user_id NVARCHAR(100),
            user_name NVARCHAR(200),
            order_id NVARCHAR(100),
            sensitive_words NVARCHAR(500),
            review_content NVARCHAR(1000),
            violation_level NVARCHAR(50),
            p0_count INT DEFAULT 0,
            p1_count INT DEFAULT 0,
            p2_count INT DEFAULT 0,
            created_time DATETIME DEFAULT GETDATE(),
            handled BIT DEFAULT 0
        )
        """)
        conn.commit()
    except Exception as e:
        logger.warning(f"ensure user_violation_records table: {e}")

    # 统计级别
    p0 = p1 = p2 = 0
    for w in sensitive_words:
        lvl = classifier.all_words.get(w, "P2")
        if lvl == "P0":
            p0 += 1
        elif lvl == "P1":
            p1 += 1
        else:
            p2 += 1

    if p0 > 0:
        level = "CRITICAL"
    elif p1 >= 2:
        level = "HIGH"
    elif p1 >= 1 or p2 >= 3:
        level = "MEDIUM"
    else:
        level = "LOW"

    try:
        cursor = conn.cursor()
        insert_sql = """
        INSERT INTO user_violation_records
        (user_id, user_name, order_id, sensitive_words, review_content, violation_level, p0_count, p1_count, p2_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        # try to get user_name from oms_order_dtl (best effort)
        user_name = None
        try:
            cur2 = conn.cursor()
            cur2.execute("SELECT user_name FROM oms_order_dtl WHERE user_id = ?", (str(user_id),))
            r = cur2.fetchone()
            if r:
                user_name = r[0]
        except Exception:
            pass

        cursor.execute(insert_sql, (
            str(user_id),
            str(user_name) if user_name else "",
            str(order_id),
            ",".join(sensitive_words),
            str(ai_review)[:1000],
            level,
            int(p0),
            int(p1),
            int(p2)
        ))
        conn.commit()
        logger.warning(f"标记违规用户 {user_id} (order {order_id}) level={level} words={sensitive_words}")
    except Exception as e:
        logger.error(f"写 user_violation_records 失败: {e}")
        try:
            conn.rollback()
        except Exception:
            pass


# ===========================
# AI 评论生成（简化版：如果不能调用外部API则使用规则生成）
# ===========================
def generate_ai_review_local(brand, english_name, chinese_name):
    """简单的本地模拟 AI 生成器（无需外部API）"""
    templates_pos = [
        "{brand}{ename}不错，面料舒服，款式好，推荐购买。",
        "很喜欢这款{brand}{ename}，穿着舒适，物流也快。",
        "{brand}{ename}质量不错，值得入手。"
    ]
    templates_neu = [
        "{brand}{ename}总体还行，有优点也有不足。",
        "{brand}{ename}性价比一般，按需购买。"
    ]
    templates_neg = [
        "{brand}{ename}不太满意，做工一般，有点失望。",
        "{brand}{ename}质量低于预期，不推荐。"
    ]
    r = random.random()
    if r < 0.55:
        tpl = random.choice(templates_pos)
    elif r < 0.85:
        tpl = random.choice(templates_neu)
    else:
        tpl = random.choice(templates_neg)

    ename = english_name if english_name else chinese_name if chinese_name else ""
    return tpl.format(brand=f"{brand} " if brand else "", ename=ename)


# ===========================
# 插入到 oms_order_dtl_enhanced2（包含 user_name, total_amount）
# ===========================
def insert_data_to_sqlserver(conn, df: pd.DataFrame):
    try:
        cursor = conn.cursor()
        insert_sql = """
        INSERT INTO oms_order_dtl_enhanced2
        (order_id, user_id, user_name, total_amount, product_id, product_name, brand, english_name, chinese_name, ai_review, has_sensitive_content, sensitive_words, ds)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        inserted = 0
        skipped = 0
        for _, row in df.iterrows():
            try:
                cursor.execute(insert_sql, (
                    str(row.get("order_id", "")),
                    str(row.get("user_id", "")),
                    str(row.get("user_name", ""))[:200],
                    float(row.get("total_amount", 0.0)) if row.get("total_amount", None) is not None else None,
                    str(row.get("product_id", "")),
                    str(row.get("product_name", ""))[:500],
                    str(row.get("brand", ""))[:100],
                    str(row.get("english_name", ""))[:200],
                    str(row.get("chinese_name", ""))[:300],
                    str(row.get("ai_review", ""))[:1000],
                    int(row.get("has_sensitive_content", 0)),
                    str(row.get("sensitive_words", ""))[:500],
                    row.get("ds", None)
                ))
                inserted += 1
            except Exception as e:
                logger.warning(f"插入失败，跳过一条: {e}")
                skipped += 1
                continue
        conn.commit()
        logger.info(f"插入完成: 成功 {inserted} / 跳过 {skipped}")
    except Exception as e:
        logger.error(f"insert_data_to_sqlserver 出错: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        raise
# ===========================
# 主流程
# ===========================
def main():
    try:
        conn = get_sqlserver_conn()
        logger.info("SQL Server数据库连接成功!")

        # 显示敏感词统计信息
        stats = classifier.get_statistics()
        logger.info(f"敏感词分类器统计: P0-{stats['p0_count']}个, P1-{stats['p1_count']}个, P2-{stats['p2_count']}个, 总计-{stats['total_count']}个")

        # 检查并创建表
        check_and_create_table(conn)

        # SQL查询：获取原始订单数据 + 用户名称 + 总金额
        query_sql = """
        SELECT TOP 20 
            o.order_id,
            o.user_id,
            u.user_name AS user_name,
            o.total_amount,
            o.product_id,
            o.product_name,
            o.ds
        FROM oms_order_dtl o
        LEFT JOIN oms_order_dtl u ON o.user_id = u.user_id
        """
        df = pd.read_sql_query(query_sql, conn)
        logger.info(f"获取到 {len(df)} 条原始数据")

        # 显示DS字段分布
        if 'ds' in df.columns:
            ds_counts = df['ds'].value_counts().head(5)
            logger.info("DS字段分布（前5条）:")
            for ds, count in ds_counts.items():
                logger.info(f"  {ds}: {count}条")

        # 产品信息拆分
        logger.info("开始拆分产品信息...")
        results = []
        for pname in df['product_name']:
            results.append(split_product_info(pname))
        result_df = pd.DataFrame(results)
        final_df = pd.concat([df.reset_index(drop=True), result_df.reset_index(drop=True)], axis=1)

        # AI 评论生成 + 敏感词检测
        logger.info("开始生成AI评论并检测敏感词...")
        processed_data = []
        for i, row in final_df.iterrows():
            logger.info(f"\n处理第 {i+1}/{len(final_df)} 条数据: 订单 {row['order_id']}")
            # 生成 AI 评论
            ai_review = generate_ai_review_local(row.get('brand', ''), row.get('english_name', ''), row.get('chinese_name', ''))
            row['ai_review'] = ai_review

            # 敏感词检测
            detected_words = classifier.detect(ai_review)
            row['has_sensitive_content'] = 1 if detected_words else 0
            row['sensitive_words'] = ",".join(detected_words) if detected_words else ""

            # 如果存在敏感词，标记违规用户
            if detected_words:
                mark_violation_user(conn, row.get('user_id'), row.get('order_id'), detected_words, ai_review)

            processed_data.append(row)
            logger.info(f"AI评论: {ai_review}")
            if detected_words:
                logger.info(f"敏感词: {row['sensitive_words']}")

            # API 限制延迟
            time.sleep(0.5)

        # 创建最终 DataFrame
        final_processed_df = pd.DataFrame(processed_data)

        # 统计敏感内容
        sensitive_count = final_processed_df['has_sensitive_content'].sum()
        logger.info(f"\n处理完成! 总评论数: {len(final_processed_df)}, 含敏感内容: {sensitive_count}, 比例: {sensitive_count/len(final_processed_df)*100:.1f}%")

        # 保存备份 CSV
        if len(final_processed_df) > 0:
            output_file = "oms_order_dtl_enhanced2_ai.csv"
            final_processed_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"数据已备份到 CSV: {output_file}")

        # 插入到 SQL Server
        logger.info("开始插入数据到 SQL Server...")
        insert_data_to_sqlserver(conn, final_processed_df)

        # 验证插入
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM oms_order_dtl_enhanced2")
        total_count = cursor.fetchone()[0]
        logger.info(f"oms_order_dtl_enhanced2 当前总记录数: {total_count}")

        # 显示敏感内容统计
        cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN has_sensitive_content = 1 THEN 1 ELSE 0 END) as sensitive_count
        FROM oms_order_dtl_enhanced2
        """)
        stats = cursor.fetchone()
        logger.info(f"敏感内容统计 - 总数: {stats[0]}, 含敏感内容: {stats[1]}")

        conn.close()
        logger.info("\n整个处理流程完成!")

    except Exception as e:
        logger.error(f"数据处理失败: {e}")
        import traceback
        traceback.print_exc()


# ===========================
# 程序入口
# ===========================
if __name__ == "__main__":
    classifier = SensitiveWordClassifier()
    classifier.load_sensitive_words("D:/WorkSpace/2301B/stream_denv_realtime_data/data_monck/suspected-sensitive-words.txt")
    main()