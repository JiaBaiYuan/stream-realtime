# save as test_comment_data_pyodbc.py
import logging
import os
import random
import re
import time
import warnings
from typing import Dict, List, Set, Tuple

import pandas as pd
import pyodbc
import requests
from dotenv import load_dotenv

# 过滤警告
warnings.filterwarnings('ignore', category=UserWarning, message='pandas only supports SQLAlchemy connectable')

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

# 数据库配置（从 .env 读取）
mysql_ip = os.getenv('sqlserver_ip')
mysql_port = os.getenv('sqlserver_port')
mysql_user_name = os.getenv('sqlserver_user_name')
mysql_user_pwd = os.getenv('sqlserver_user_pwd')
mysql_order_db = os.getenv('sqlserver_db')

# 简单校验环境变量
if not all([mysql_ip, mysql_port, mysql_user_name, mysql_user_pwd, mysql_order_db]):
    logger.error("请检查 .env 文件，缺失 sqlserver_ip/sqlserver_port/sqlserver_user_name/sqlserver_user_pwd/sqlserver_db")
    raise SystemExit(1)

# 硅基流动API配置
SILICONFLOW_API_KEY = 'sk-xlwrtkadumgenzvsdeodercahegrcevgsslzpuhreimvjddm'
SILICONFLOW_API_URL = "https://api.siliconflow.cn/v1/chat/completions"
MODEL_NAME = "Qwen/Qwen3-8B"

# ---------- 敏感词分类器（使用cc.py的正确逻辑） ----------
class SensitiveWordClassifier:
    """敏感词分类器 - 使用cc.py的正确逻辑"""

    def __init__(self):
        self.sensitive_words = []
        self._word_categories = {}

    def load_sensitive_words(self, file_path: str = None) -> bool:
        """加载敏感词 - 使用cc.py的正确逻辑"""
        try:
            if file_path and os.path.exists(file_path):
                self.sensitive_words = self._read_words_from_file(file_path)
                logger.info(f"从文件加载了 {len(self.sensitive_words)} 个敏感词")

                # 为所有敏感词设置默认分类
                for word in self.sensitive_words:
                    self._word_categories[word] = self._determine_category(word)

                return True
            else:
                logger.warning("未找到敏感词文件，使用空词库")
                return False

        except Exception as e:
            logger.error(f"加载敏感词失败: {e}")
            return False

    def _read_words_from_file(self, file_path: str) -> List[str]:
        """从文件读取敏感词 - 使用cc.py的正确逻辑"""
        words = set()
        try:
            with open(file_path, "r", encoding='utf-8') as f:
                for line in f:
                    word = line.strip()
                    if word and not word.startswith('#'):
                        if '|' in word:
                            words.update(w.strip() for w in word.split('|') if w.strip())
                        else:
                            words.add(word)
            return list(words)
        except Exception as e:
            logger.error(f"读取文件失败: {e}")
            return []

    def _determine_category(self, word: str) -> str:
        """确定敏感词分类"""
        # 基础敏感词分类逻辑
        p0_keywords = ['毒品', '枪支', '暴恐', '分裂', '邪教', '诈骗', '爆炸', '恐怖', '赌博', '色情']
        p1_keywords = ['傻逼', '操你', '妈的', '地域黑', '地图炮', '尼哥', '尼玛', '草泥马', '法克', '黑鬼', '白皮猪']

        if any(keyword in word for keyword in p0_keywords):
            return "p0"
        elif any(keyword in word for keyword in p1_keywords):
            return "p1"
        elif len(word) <= 3:
            return "p1"
        else:
            return "p2"

    def check_sensitive(self, text: str) -> Dict[str, List[str]]:
        """检测文本中是否包含敏感词 - 使用cc.py的正确逻辑"""
        if not text or not isinstance(text, str):
            return {"p0": [], "p1": [], "p2": []}

        result = {"p0": [], "p1": [], "p2": []}

        # 使用cc.py的直接匹配逻辑
        for word in self.sensitive_words:
            if word in text:
                category = self._word_categories.get(word, "p2")
                result[category].append(word)

        # 去重
        for category in result:
            result[category] = list(set(result[category]))

        return result

    def get_statistics(self) -> Dict[str, int]:
        """获取统计信息"""
        p0_count = len([w for w in self.sensitive_words if self._word_categories.get(w) == "p0"])
        p1_count = len([w for w in self.sensitive_words if self._word_categories.get(w) == "p1"])
        p2_count = len([w for w in self.sensitive_words if self._word_categories.get(w) == "p2"])

        return {
            "p0_count": p0_count,
            "p1_count": p1_count,
            "p2_count": p2_count,
            "total_count": len(self.sensitive_words)
        }

# ---------- 使用cc.py的正确敏感词检测函数 ----------
def detect_sensitive_content(text: str, sensitive_words: list):
    """检测文本中的敏感内容 - 使用cc.py的正确逻辑"""
    if not isinstance(text, str):
        return False, []

    found = [w for w in sensitive_words if w in text]
    return bool(found), found

def inject_random_sensitive_words(text: str, sensitive_words: list, probability: float = 0.3) -> Tuple[str, List[str]]:
    """
    随机向评论中插入敏感词 - 使用cc.py的正确逻辑
    probability: 插入敏感词的概率(0~1)
    返回: 修改后的文本，插入的敏感词列表
    """
    if random.random() > probability or not sensitive_words:
        return text, []

    # 随机选择1~3个敏感词插入
    num_to_insert = random.randint(1, min(3, len(sensitive_words)))
    insert_words = random.sample(sensitive_words, num_to_insert)

    # 随机插入到文本中
    segments = text.split()
    for word in insert_words:
        idx = random.randint(0, len(segments))
        segments.insert(idx, word)
    new_text = " ".join(segments)
    return new_text, insert_words

def get_random_element(lst):
    """从列表中随机返回一个元素"""
    if not lst:
        return None
    return random.choice(lst)

# ---------- DB connection helpers ----------
def get_sqlserver_conn(max_retries: int = 3, retry_delay: int = 5):
    """
    使用 pyodbc 返回一个 SQL Server 连接。
    需要在 Windows 上安装 Microsoft ODBC Driver 17 for SQL Server，或相应 Linux 驱动。
    """
    driver = os.getenv('ODBC_DRIVER', 'ODBC Driver 17 for SQL Server')
    server = f"{mysql_ip},{mysql_port}"
    database = mysql_order_db
    uid = mysql_user_name
    pwd = mysql_user_pwd

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={uid};"
        f"PWD={pwd};"
        "TrustServerCertificate=Yes;"
        "Encrypt=Yes;"
    )

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            conn = pyodbc.connect(conn_str, timeout=30)
            # pyodbc 默认 autocommit=False，保持手动 commit
            logger.info(f"SQL Server 连接成功: {server} (尝试 {attempt})")
            return conn
        except Exception as e:
            last_err = e
            logger.warning(f"尝试连接 SQL Server 失败 (第 {attempt} 次): {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
    logger.error("无法连接 SQL Server，请检查网络/用户名/密码/驱动。最后错误: %s", last_err)
    raise last_err

# ---------- DB-related operations (使用 pyodbc 的 ? 占位符) ----------
def mark_violation_user(conn, user_id, order_id, sensitive_words, review_content):
    """标记违规用户并记录到数据库 - pyodbc 版本"""
    try:
        cursor = conn.cursor()

        # 检查用户违规表是否存在，不存在则创建
        check_table_sql = """
        IF OBJECT_ID('user_violation_records', 'U') IS NULL
        BEGIN
            CREATE TABLE user_violation_records (
                id INT IDENTITY(1,1) PRIMARY KEY,
                user_id NVARCHAR(100),
                order_id NVARCHAR(100),
                sensitive_words NVARCHAR(500),
                review_content NVARCHAR(1000),
                violation_level NVARCHAR(50),
                p0_count INT DEFAULT 0,
                p1_count INT DEFAULT 0,
                p2_count INT DEFAULT 0,
                handled BIT DEFAULT 0,
                created_time DATETIME DEFAULT GETDATE()
            )
        END
        """
        cursor.execute(check_table_sql)

        # 分析敏感词级别
        p0_count = 0
        p1_count = 0
        p2_count = 0

        for word in sensitive_words:
            category = classifier._word_categories.get(word, "p2")
            if category == "p0":
                p0_count += 1
            elif category == "p1":
                p1_count += 1
            else:
                p2_count += 1

        # 确定违规级别
        violation_level = "LOW"
        if p0_count > 0:
            violation_level = "CRITICAL"
        elif p1_count >= 2:
            violation_level = "HIGH"
        elif p1_count >= 1 or  p2_count >= 3:
            violation_level = "MEDIUM"

        # 插入违规记录（pyodbc 使用 ? 占位）
        insert_sql = """
        INSERT INTO user_violation_records 
        (user_id, order_id, sensitive_words, review_content, violation_level, p0_count, p1_count, p2_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_sql, (
            str(user_id),
            str(order_id),
            ','.join(sensitive_words),
            str(review_content)[:1000],
            violation_level,
            int(p0_count),
            int(p1_count),
            int(p2_count)
        ))
        conn.commit()
        logger.warning(f"用户 {user_id} 因使用敏感词被标记为违规，级别: {violation_level}")
        return True

    except Exception as e:
        logger.error(f"标记违规用户失败: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False

def process_violation_users(conn):
    """处理违规用户（这里可以扩展为发送警告、限制权限等）"""
    try:
        cursor = conn.cursor()

        # 获取未处理的高风险违规用户
        query_sql = """
        SELECT user_id, violation_level, sensitive_words, p0_count, p1_count, p2_count, created_time
        FROM user_violation_records 
        WHERE handled = 0 AND violation_level IN ('CRITICAL', 'HIGH', 'MEDIUM')
        ORDER BY created_time DESC
        """

        cursor.execute(query_sql)
        violations = cursor.fetchall()

        for violation in violations:
            user_id, level, words, p0_count, p1_count, p2_count, create_time = violation
            logger.info(f"处理违规用户: {user_id}, 级别: {level}, P0: {p0_count}, P1: {p1_count}, P2: {p2_count}, 敏感词: {words}")

            # 这里可以添加具体的处理逻辑，比如：发送通知、限制权限等

            # 标记为已处理
            update_sql = "UPDATE user_violation_records SET handled = 1 WHERE user_id = ? AND handled = 0"
            cursor.execute(update_sql, (str(user_id),))

        conn.commit()
        logger.info(f"已处理 {len(violations)} 个违规用户")

    except Exception as e:
        logger.error(f"处理违规用户失败: {e}")
        try:
            conn.rollback()
        except Exception:
            pass

# ---------- 其他工具函数（保留你原来实现） ----------
def fix_encoding(text):
    """修复编码问题"""
    if not isinstance(text, str):
        return text

    encodings_to_try = [
        ('latin-1', 'utf-8'),
        ('latin-1', 'gbk'),
        ('latin-1', 'gb2312'),
        ('utf-8', 'utf-8'),
    ]

    for src_enc, dst_enc in encodings_to_try:
        try:
            return text.encode(src_enc).decode(dst_enc)
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
    return text

def split_product_info(text):
    """根据丨和汉字拆分产品信息"""
    if not isinstance(text, str) or not text.strip():
        return {'brand': '', 'english_name': '', 'chinese_name': '', 'full_text': text}

    cleaned_text = fix_encoding(text)
    pattern = r'^([^丨]+)丨([^\u4e00-\u9fa5]*)([\u4e00-\u9fa5].*)$'

    match = re.match(pattern, cleaned_text)
    if match:
        brand = match.group(1).strip()
        english_part = match.group(2).strip()
        chinese_part = match.group(3).strip()

        return {
            'brand': brand,
            'english_name': english_part,
            'chinese_name': chinese_part,
            'full_text': cleaned_text
        }
    else:
        chinese_chars = re.findall(r'[\u4e00-\u9fa5]', cleaned_text)
        if chinese_chars:
            first_chinese_index = cleaned_text.find(chinese_chars[0])
            return {
                'brand': '',
                'english_name': cleaned_text[:first_chinese_index].strip(),
                'chinese_name': cleaned_text[first_chinese_index:].strip(),
                'full_text': cleaned_text
            }
        else:
            return {
                'brand': '',
                'english_name': cleaned_text,
                'chinese_name': '',
                'full_text': cleaned_text
            }

# ---------- AI 评论生成逻辑（使用正确的敏感词检测） ----------
def generate_ai_review(product_info, user_id, order_id, conn, max_retries=3):
    """
    使用硅基流动API生成商品评论，加入语言和脏话过滤，并检测违规内容
    """
    brand = product_info.get('brand', '')
    english_name = product_info.get('english_name', '')
    chinese_name = product_info.get('chinese_name', '')

    # 随机决定评论类型：55%好评，30%中评，15%差评
    random_num = random.random()

    if random_num < 0.55:
        # 好评 55%
        review_type = "好评"
        sentiment_instruction = "表达满意和推荐，语气积极正面"
    elif random_num < 0.85:
        # 中评 30%
        review_type = "中评"
        sentiment_instruction = "表达中立客观，既有优点也有缺点，语气平和"
    else:
        # 差评 15%
        review_type = "差评"
        sentiment_instruction = "表达不满和批评，语气可以强烈一些"

    # 脏话后缀列表（从py2.py中集成）
    sensitive_suffixes = [
        " 妈的",
        " 操",
        " 傻逼",
        " 垃圾",
        " 特么的",
        " 草",
        " 日",
        " 靠",
        " 尼玛",
        " 我操",
        " 真他妈",
        " 狗日的",
        " 滚蛋",
        " 去死",
        " 王八蛋"
    ]

    # 构建提示词
    prompt = f"""
    请为以下商品生成一条{review_type}：
    品牌：{brand}
    产品名称：{english_name}
    产品描述：{chinese_name}

    要求：
    1. 评论长度在20-50字之间
    2. 语气自然，像真实用户写的
    3. 包含具体的使用体验和感受
    4. {sentiment_instruction}
    5. 直接返回评论内容，不要添加其他说明
    6. 不用思考
    """

    headers = {
        "Authorization": f"Bearer {SILICONFLOW_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "max_tokens": 100,
        "temperature": 0.8,
        "top_p": 0.8,
        "stream": False
    }

    for attempt in range(max_retries):
        try:
            logger.info(f"正在调用AI生成{review_type}... (尝试 {attempt + 1})")
            response = requests.post(SILICONFLOW_API_URL, headers=headers,
                                     json=payload, timeout=60)

            if response.status_code == 200:
                result = response.json()
                if 'choices' in result and len(result['choices']) > 0:
                    review = result['choices'][0]['message']['content'].strip()

                    # 使用正确的敏感词检测方法
                    has_sensitive, detected_words = detect_sensitive_content(review, classifier.sensitive_words)
                    if has_sensitive:
                        # 标记违规用户
                        try:
                            mark_violation_user(conn, user_id, order_id, detected_words, review)
                        except Exception as e:
                            logger.error(f"标记违规用户失败: {e}")

                        # 替换敏感词
                        for word in detected_words:
                            review = review.replace(word, "***")
                        logger.warning(f"检测到敏感词并已处理: {detected_words}")

                    # 为差评添加敏感词后缀（30%概率，从py2.py中的0.3概率）
                    if review_type == "差评" and random.random() < 0.3:
                        suffix = get_random_element(sensitive_suffixes)
                        if suffix:
                            review += suffix
                            # 检测添加的后缀是否包含敏感词
                            has_sensitive_suffix, suffix_words = detect_sensitive_content(suffix, classifier.sensitive_words)
                            if has_sensitive_suffix:
                                try:
                                    mark_violation_user(conn, user_id, order_id, suffix_words, review)
                                except Exception as e:
                                    logger.error(f"标记违规用户失败: {e}")
                                logger.warning(f"后缀包含敏感词，用户已被标记: {suffix_words}")
                            logger.info(f"  {review_type}生成成功（含敏感后缀）: {review}")
                    else:
                        logger.info(f"  {review_type}生成成功: {review}")

                    return review
                else:
                    logger.error("API返回格式异常")
            else:
                logger.error(f"API请求失败，状态码: {response.status_code}")
                if response.status_code == 429:
                    logger.warning("达到速率限制，等待后重试...")
                    time.sleep(10)

        except requests.exceptions.Timeout:
            logger.error("请求超时，重试...")
        except requests.exceptions.ConnectionError:
            logger.error("连接错误，重试...")
        except Exception as e:
            logger.error(f"生成评论异常: {e}")

        # 重试前等待
        if attempt < max_retries - 1:
            wait_time = 5 * (attempt + 1)
            logger.info(f"等待 {wait_time} 秒后重试...")
            time.sleep(wait_time)

    # 如果所有重试都失败，返回默认评论
    if random_num < 0.55:
        default_review = f"{brand}的{english_name}很不错，使用体验很好，值得推荐。"
    elif random_num < 0.85:
        default_review = f"{brand}的{english_name}整体还可以，有些地方不错，但也有一些小问题。"
    else:
        default_review = f"{brand}的{english_name}质量一般，有些失望，不太推荐。"
        # 为差评默认评论添加敏感词后缀（30%概率）
        if random.random() < 0.3:
            suffix = get_random_element(sensitive_suffixes)
            if suffix:
                default_review += suffix
                # 检测默认评论的后缀
                has_sensitive, detected_words = detect_sensitive_content(default_review, classifier.sensitive_words)
                if has_sensitive:
                    try:
                        mark_violation_user(conn, user_id, order_id, detected_words, default_review)
                    except Exception as e:
                        logger.error(f"标记违规用户失败: {e}")

    logger.info(f"AI生成失败，使用默认{review_type}")
    return default_review


def process_row(row, conn):
    """
    处理单条订单数据，生成：
    - brand / english_name / chinese_name（基于 product_name）
    - ai_review（调用硅基流动 API）
    - 随机注入敏感词
    - has_sensitive_content / sensitive_words（检测敏感词）
    """
    try:
        # 解析商品信息
        product_text = row.get('product_name', '')
        product_info = split_product_info(product_text)

        # AI 生成评论
        review = generate_ai_review(
            product_info=product_info,
            user_id=row.get('user_id', ''),
            order_id=row.get('order_id', ''),
            conn=conn
        )

        # 随机注入敏感词（概率30%）- 使用正确的逻辑
        review, injected_words = inject_random_sensitive_words(review, classifier.sensitive_words, probability=0.3)
        if injected_words:
            logger.info(f"随机注入敏感词: {injected_words}")

        # 检测敏感词 - 使用正确的逻辑
        has_sensitive, words = detect_sensitive_content(review, classifier.sensitive_words)

        # 返回处理后的行
        return {
            "order_id": row.get("order_id", ""),
            "user_id": row.get("user_id", ""),
            "user_name": row.get("user_name", ""),
            "product_id": row.get("product_id", ""),
            "total_amount": row.get("total_amount", ""),
            "product_name": product_text,
            "brand": product_info.get('brand', ''),
            "english_name": product_info.get('english_name', ''),
            "chinese_name": product_info.get('chinese_name', ''),
            "ai_review": review,
            "has_sensitive_content": int(has_sensitive),
            "sensitive_words": ",".join(words),
            "ds": row.get("ds", None)
        }

    except Exception as e:
        logger.error(f"处理行失败: {e}")
        return None

# ---------- table check/create (pyodbc) ----------
def check_and_create_table(conn):
    """检查表是否存在，如果不存在则创建，如果存在则添加缺失字段"""
    try:
        cursor = conn.cursor()

        # 首先检查表是否存在
        check_table_sql = """
        IF OBJECT_ID('oms_order_dtl_enhanced2', 'U') IS NOT NULL
        BEGIN
            SELECT 1
        END
        ELSE
        BEGIN
            SELECT 0
        END
        """
        cursor.execute(check_table_sql)
        row = cursor.fetchone()
        table_exists = row[0] if row else 0

        if table_exists == 0:
            # 表不存在，创建新表
            create_table_sql = """
            CREATE TABLE oms_order_dtl_enhanced2 (
                id INT IDENTITY(1,1) PRIMARY KEY,
                order_id NVARCHAR(255),
                user_id NVARCHAR(255),
                user_name NVARCHAR(255),
                product_id NVARCHAR(255),
                total_amount NVARCHAR(255),
                product_name NVARCHAR(500),
                brand NVARCHAR(255),
                english_name NVARCHAR(255),
                chinese_name NVARCHAR(300),
                ai_review NVARCHAR(1000),
                has_sensitive_content BIT DEFAULT 0,
                sensitive_words NVARCHAR(500),
                violation_handled BIT DEFAULT 0,
                ds DATE,
                created_time DATETIME DEFAULT GETDATE()
            )
            """
            cursor.execute(create_table_sql)
            logger.info("新表创建成功: oms_order_dtl_enhanced2")
        else:
            # 表已存在，检查并添加缺失字段
            logger.info("表已存在: oms_order_dtl_enhanced2，检查字段完整性...")

            # 检查字段是否存在，如果不存在则添加
            columns_to_check = [
                ('has_sensitive_content', 'BIT DEFAULT 0'),
                ('sensitive_words', 'NVARCHAR(500)'),
                ('violation_handled', 'BIT DEFAULT 0')
            ]

            for column_name, column_type in columns_to_check:
                check_column_sql = f"""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns 
                    WHERE object_id = OBJECT_ID('oms_order_dtl_enhanced2') 
                    AND name = '{column_name}'
                )
                BEGIN
                    ALTER TABLE oms_order_dtl_enhanced2 ADD {column_name} {column_type}
                END
                """
                cursor.execute(check_column_sql)

            logger.info("表字段检查完成")

        conn.commit()

    except Exception as e:
        logger.error(f"检查/创建表失败: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        raise

# ---------- insert (pyodbc) ----------
def insert_data_to_sqlserver(conn, df):
    """将数据插入到SQL Server，避免重复插入"""
    try:
        cursor = conn.cursor()

        # 插入SQL - 使用 ? 占位
        insert_sql = """
        INSERT INTO oms_order_dtl_enhanced2
        (order_id, user_id, user_name, product_id, total_amount, product_name, brand, english_name, chinese_name, ai_review, has_sensitive_content, sensitive_words, ds)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        inserted_count = 0
        skipped_count = 0

        for _, row in df.iterrows():
            try:
                cursor.execute(insert_sql, (
                    str(row.get('order_id', '')),
                    str(row.get('user_id', '')),
                    str(row.get('user_name', '')),
                    str(row.get('product_id', '')),
                    str(row.get('total_amount', '')),
                    str(row.get('product_name', ''))[:500],
                    str(row.get('brand', ''))[:100],
                    str(row.get('english_name', ''))[:200],
                    str(row.get('chinese_name', ''))[:300],
                    str(row.get('ai_review', ''))[:1000],
                    int(row.get('has_sensitive_content', 0)),
                    str(row.get('sensitive_words', ''))[:500],
                    row.get('ds', None)
                ))
                inserted_count += 1
            except Exception as e:
                # 捕获所有插入错误并跳过该条记录
                logger.warning(f"插入数据时遇到错误，跳过该条记录: {e}")
                skipped_count += 1
                continue

        conn.commit()
        logger.info(f"成功插入 {inserted_count} 条新数据，跳过 {skipped_count} 条重复或错误数据")

    except Exception as e:
        logger.error(f"插入数据失败: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        raise

# ---------- 主流程 ----------
def main():
    try:
        conn = get_sqlserver_conn()

        logger.info("SQL Server数据库连接成功!")

        # 显示敏感词分类器统计信息
        stats = classifier.get_statistics()
        logger.info(f"敏感词分类器统计: P0-{stats['p0_count']}个, P1-{stats['p1_count']}个, P2-{stats['p2_count']}个, 总计-{stats['total_count']}个")

        # 检查并创建表（如果不存在）
        check_and_create_table(conn)

        # SQL查询 - 获取数据（包含ds字段）
        query_sql = """
            SELECT TOP 20 
                    order_id,
                    user_id,
                    user_name,
                    product_id,
                    total_amount,
                    product_name,
                    ds
            FROM oms_order_dtl;
            """


        # 获取数据
        df = pd.read_sql_query(query_sql, conn)
        logger.info(f"获取到 {len(df)} 条原始数据")

        # 显示ds字段的分布情况
        if 'ds' in df.columns:
            ds_counts = df['ds'].value_counts().head(5)
            logger.info(f"DS字段分布（前5个）:")
            for ds, count in ds_counts.items():
                logger.info(f"  {ds}: {count}条")

        # 处理产品名称拆分
        logger.info("开始处理产品信息拆分...")
        results = []
        for product_name in df['product_name']:
            result = split_product_info(product_name)
            results.append(result)

        # 合并结果
        result_df = pd.DataFrame(results)
        final_df = pd.concat([df.reset_index(drop=True), result_df.reset_index(drop=True)], axis=1)

        # 为每个产品生成AI评论
        logger.info("开始使用AI生成评论...")
        processed_data = []

        for i, row in final_df.iterrows():
            logger.info(f"\n正在处理第 {i + 1}/{len(final_df)} 条数据...")
            logger.info(f"  产品: {row.get('brand','')} - {row.get('english_name','')}")
            if 'ds' in row:
                logger.info(f"  日期: {row.get('ds')}")

            # 使用process_row处理单行数据
            processed_row = process_row(row, conn)
            if processed_row:
                processed_data.append(processed_row)

            # 添加延迟避免API限制
            logger.info("等待3秒...")
            time.sleep(3)

        # 创建最终DataFrame
        final_processed_df = pd.DataFrame(processed_data)

        # 显示处理统计
        sensitive_count = final_processed_df['has_sensitive_content'].sum()
        logger.info(f"\n评论生成完成！敏感内容统计:")
        logger.info(f"- 总评论数: {len(final_processed_df)}")
        logger.info(f"- 含敏感内容: {sensitive_count}")
        logger.info(f"- 敏感内容比例: {sensitive_count/len(final_processed_df)*100:.1f}%" if len(final_processed_df) > 0 else "- 敏感内容比例: 0%")

        # 显示前5条结果
        if len(final_processed_df) > 0:
            logger.info("\n前5条处理结果:")
            logger.info("=" * 120)
            for i, row in final_processed_df.head(5).iterrows():
                logger.info(f"产品 {i + 1}:")
                logger.info(f"  订单ID: {row.get('order_id','')}")
                logger.info(f"  用户ID: {row.get('user_id','')}")
                logger.info(f"  品牌: {row.get('brand','')}")
                logger.info(f"  品类名称: {row.get('english_name','')}")
                logger.info(f"  中文描述: {row.get('chinese_name','')}")
                if 'ds' in row:
                    logger.info(f"  日期: {row.get('ds')}")
                logger.info(f"  AI评论: {row.get('ai_review','')}")
                logger.info(f"  含敏感内容: {'是' if row.get('has_sensitive_content',0) else '否'}")
                if row.get('sensitive_words'):
                    logger.info(f"  敏感词: {row.get('sensitive_words')}")
                logger.info("-" * 120)

            # 保存到CSV文件（备份）
            output_file = "oms_order_dtl_enhanced2_ai.csv"
            final_processed_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"\n数据已备份到CSV文件: {output_file}")

            # 插入到SQL Server新表（避免重复）
            logger.info("正在将数据插入到SQL Server...")
            insert_data_to_sqlserver(conn, final_processed_df)
        else:
            logger.warning("没有成功处理的数据，跳过插入数据库步骤")

        # 处理违规用户
        logger.info("开始处理违规用户...")
        process_violation_users(conn)

        # 验证SQL Server插入的数据
        verify_sql = "SELECT COUNT(*) as total_count FROM oms_order_dtl_enhanced2"
        cursor = conn.cursor()
        cursor.execute(verify_sql)
        count = cursor.fetchone()[0]
        logger.info(f"SQL Server新表中现有数据量: {count} 条")

        # 显示敏感内容统计
        sensitive_stats_sql = """
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN has_sensitive_content = 1 THEN 1 ELSE 0 END) as sensitive_count
        FROM oms_order_dtl_enhanced2
        """
        cursor.execute(sensitive_stats_sql)
        stats = cursor.fetchone()
        logger.info(f"敏感内容统计 - 总数: {stats[0]}, 含敏感内容: {stats[1]}")

        conn.close()

        logger.info("\n处理完成！")
        logger.info(f"- 原始数据: {len(df)} 条")
        logger.info(f"- 成功处理: {len(processed_data)} 条")
        logger.info(f"- 存储位置: SQL Server表 [oms_order_dtl_enhanced2]")
        logger.info(f"- 新增字段: brand, english_name, chinese_name, ai_review, has_sensitive_content, sensitive_words")
        if len(processed_data) > 0:
            logger.info(f"- 备份文件: {output_file}")
        logger.info(f"- 违规用户处理: 已完成")

    except Exception as e:
        logger.error(f"数据处理失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # 全局创建敏感词分类器
    classifier = SensitiveWordClassifier()
    classifier.load_sensitive_words("D:/WorkSpace/2301B/stream_denv_realtime_data/data_monck/suspected-sensitive-words.txt")
    main()