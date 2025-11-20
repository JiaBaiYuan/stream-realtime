import requests
import json
import random

def generate_review_online():
    """使用免费的在线 API"""

    # 尝试多个免费的文本生成 API
    apis = [
        {
            'name': 'Hugging Face',
            'url': 'https://api-inference.huggingface.co/models/Qwen/Qwen2.5-0.5B-Instruct',
            'headers': {'Authorization': 'Bearer hf_你的token'},
            'data_key': 'inputs'
        }
    ]

    review_type = random.choice(["好评", "差评"])
    prompt = f'为"女士运动高腰中长紧身裤21"生成{review_type}，返回JSON：{{"product": "女士运动高腰中长紧身裤21", "comment": "内容"}}'

    for api in apis:
        try:
            response = requests.post(
                api['url'],
                headers=api.get('headers', {}),
                json={api['data_key']: prompt, 'parameters': {'max_new_tokens': 100}},
                timeout=30
            )

            if response.status_code == 200:
                result = response.json()
                if isinstance(result, list) and len(result) > 0:
                    text = result[0].get('generated_text', '')
                    return text if text else json.dumps({
                        "product": "女士运动高腰中长紧身裤21",
                        "comment": f"这是一条{review_type}：产品质量很好，穿着舒适。"
                    }, ensure_ascii=False)
        except:
            continue

    # 如果所有 API 都失败，返回模拟数据
    return generate_fallback_review(review_type)

def generate_fallback_review(review_type):
    """生成模拟评论数据"""

    good_comments = [
        "质量很好，弹性适中，穿着舒适运动不会下滑，非常满意！",
        "高腰设计很贴心，包裹性好，面料透气运动出汗也不闷热。",
        "版型很正显瘦效果好，长度刚好适合各种运动场合。"
    ]

    bad_comments = [
        "弹性不够穿着有点紧，运动时不太舒服希望改进面料。",
        "尺寸偏小建议买大一码，腰部设计有点勒运动体验一般。",
        "面料一般洗后有点起球，性价比不是很高考虑退货。"
    ]

    comment = random.choice(good_comments if review_type == "好评" else bad_comments)

    return json.dumps({
        "product": "女士运动高腰中长紧身裤21",
        "comment": comment
    }, ensure_ascii=False)

# 使用示例
review = generate_review_online()
print("生成的评论:", review)