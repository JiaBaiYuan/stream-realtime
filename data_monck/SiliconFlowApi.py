# !/usr/bin/python
# coding: utf-8

"""
@Project 
@File    SiliconFlowApi.py
@Author  jiaboyuan
@Date    2025/11/13 19:28
"""
import os
from openai import OpenAI

def call_silicon_model(p_model_name, p_prompt):
	# SiliconFlow API 基础配置
	silicon_flow_model_url = "https://api.siliconflow.cn/v1"
	silicon_api_token = "sk-xlwrtkadumgenzvsdeodercahegrcevgsslzpuhreimvjddm"  # ✅ 你的密钥

	# 创建客户端
	openai_client = OpenAI(api_key=silicon_api_token, base_url=silicon_flow_model_url)

	# 发送请求
	resp = openai_client.chat.completions.create(
		model=p_model_name,
		messages=[
			{
				"role": "system",
				"content": "You are a helpful assistant designed to output JSON."
			},
			{
				"role": "user",
				"content": f"? {p_prompt} ? Please respond in the format {{'user_comment':...}}"
			}
		],
		stream=False,
		temperature=0.7,
		max_tokens=512,
		extra_body={
			"enable_thinking": False
		},
		response_format={"type": "json_object"}
	)

	# 返回结果
	return resp.choices[0].message.content


if __name__ == "__main__":
	prompt = "请用中文写一条非常不满、带有攻击性、甚至不文明的差评，针对商品：<t丨Speed Up 女士运动高腰内衬款短裤 2.5>，价格 680 元。"
	result = call_silicon_model("Pro/deepseek-ai/DeepSeek-R1-Distill-Qwen-7B", p_prompt=prompt)
	print(result)
