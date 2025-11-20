import pandas as pd
import os
import random
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# --------------------------
# 加载敏感词
# --------------------------
def load_sensitive_words(file_path: str):
    words = set()
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            word = line.strip()
            if word and not word.startswith('#'):
                if '|' in word:
                    words.update(w.strip() for w in word.split('|') if w.strip())
                else:
                    words.add(word)
    logger.info(f"加载 {len(words)} 个敏感词")
    return list(words)

# --------------------------
# 检测敏感词
# --------------------------
def check_sensitive(text: str, sensitive_words: list):
    found = [w for w in sensitive_words if w in text]
    return found, bool(found)

# --------------------------
# 随机插入敏感词（测试用）
# --------------------------
def inject_sensitive_words(text: str, sensitive_words: list, probability: float = 0.2):
    if random.random() > probability or not sensitive_words:
        return text
    num_to_insert = random.randint(1, min(3, len(sensitive_words)))
    insert_words = random.sample(sensitive_words, num_to_insert)
    segments = text.split()
    for word in insert_words:
        idx = random.randint(0, len(segments))
        segments.insert(idx, word)
    return " ".join(segments)

# --------------------------
# 主流程
# --------------------------
if __name__ == "__main__":
    sensitive_words_file = r"/suspected-sensitive-words.txt"
    sensitive_words = load_sensitive_words(sensitive_words_file)

    # 构建测试数据
    data = [
        {"order_id": "test001", "ai_review": "这款产品质量非常好，非常推荐！"},
        {"order_id": "test002", "ai_review": "这款产品太差了，完全不值这个价钱，XXX敏感词测试"},
    ]
    df = pd.DataFrame(data)

    # ✅ 初始化敏感词列
    df['sensitive_words'] = ""
    df['has_sensitive_content'] = False

    # 插入敏感词并检测
    for idx, row in df.iterrows():
        # 随机插入敏感词（这里概率设为1，保证插入测试）
        new_text = inject_sensitive_words(row['ai_review'], sensitive_words, probability=1.0)
        df.at[idx, 'ai_review'] = new_text

        words_found, has_sensitive = check_sensitive(new_text, sensitive_words)
        # 转换为逗号分隔字符串
        df.at[idx, 'sensitive_words'] = ",".join(words_found) if words_found else ""
        df.at[idx, 'has_sensitive_content'] = has_sensitive

    print(df[['ai_review', 'sensitive_words', 'has_sensitive_content']])
