"""LLM客户端模块
支持OpenAI compatible接口的streaming实现，包含VLM支持
"""
import logging
import time
from typing import Dict, Any, List, Optional
from openai import OpenAI

try:
    from .config import config
except ImportError:
    # 当作脚本直接运行时的导入
    from config import config


class LLMClient:
    """统一的LLM客户端，支持文本和视觉多模态模型"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # 从配置文件获取配置（按优先级：环境变量 > config.ini > 默认值）
        llm_config = config.get_llm_config()
        self.api_key = llm_config.get('openai_api_key')
        self.base_url = llm_config.get('openai_base_url', 'https://api.openai.com/v1')

        # 获取不同类型的模型配置
        self.fast_model = llm_config.get('fast_model_name', 'gpt-4.1')
        self.vlm_model = llm_config.get('fast_vlm_name', 'gpt-4.1')
        self.smart_model = llm_config.get('smart_model_name', 'gpt-4.1')

        if not self.api_key:
            raise ValueError("未找到OPENAI_API_KEY配置，请在环境变量或config.ini中设置")

        # 初始化OpenAI客户端
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )

        self.logger.info(f"LLM客户端初始化成功")
        self.logger.info(f"快速模型: {self.fast_model}")
        self.logger.info(f"视觉模型: {self.vlm_model}")
        self.logger.info(f"智能模型: {self.smart_model}")

    def call_fast_model(self, prompt: str, temperature: float = 0.1) -> Dict[str, Any]:
        """
        调用快速模型进行信息提取
        适用于：结构化信息提取、分类等快速任务
        """
        return self._make_request(prompt, self.fast_model, temperature)

    def call_smart_model(self, prompt: str, temperature: float = 0.5) -> Dict[str, Any]:
        """
        调用智能模型进行深度分析
        适用于：报告生成、深度洞察、综合分析等复杂任务
        """
        return self._make_request(prompt, self.smart_model, temperature)

    def call_vlm(self, prompt: str, image_urls: List[str], temperature: float = 0.3, max_retries: int = 3) -> Dict[str, Any]:
        """
        调用视觉多模态模型进行图文分析，包含重试机制

        Args:
            prompt: 文本提示词
            image_urls: 图片URL列表
            temperature: 生成温度
            max_retries: 最大重试次数

        Returns:
            响应结果字典
        """
        if not image_urls:
            return {
                'success': False,
                'error': '没有提供图片URL',
                'model': self.vlm_model
            }

        # 验证图片URL数量限制
        if len(image_urls) > 10:  # 大多数VLM API都有图片数量限制
            self.logger.warning(f"图片数量过多({len(image_urls)})，截取前10张")
            image_urls = image_urls[:10]

        for attempt in range(max_retries):
            try:
                self.logger.info(f"调用VLM模型: {self.vlm_model} (尝试 {attempt + 1}/{max_retries})")
                self.logger.info(f"图片数量: {len(image_urls)}")
                self.logger.info(f"提示词长度: {len(prompt)} 字符")

                # 构建消息内容
                content = [{"type": "text", "text": prompt}]

                # 添加图片
                for i, url in enumerate(image_urls):
                    content.append({
                        "type": "image_url",
                        "image_url": {"url": url}
                    })
                    self.logger.debug(f"添加图片 {i+1}: {url}")

                # 创建请求
                response = self.client.chat.completions.create(
                    model=self.vlm_model,
                    messages=[{
                        "role": "user",
                        "content": content
                    }],
                    temperature=temperature,
                    stream=True
                )

                # 收集streaming响应
                full_content = ""
                chunk_count = 0

                self.logger.info("开始处理VLM streaming响应...")

                for chunk in response:
                    chunk_count += 1
                    delta = chunk.choices[0].delta
                    content_chunk = getattr(delta, 'content', None)

                    if content_chunk:
                        full_content += content_chunk
                        self.logger.debug(f"VLM Chunk {chunk_count}: {content_chunk[:50]}...")

                self.logger.info(f"VLM调用完成 - 处理了 {chunk_count} 个chunks")
                self.logger.info(f"响应内容长度: {len(full_content)} 字符")

                # 检查响应内容是否为空
                if not full_content.strip():
                    raise ValueError("VLM返回空响应")

                return {
                    'success': True,
                    'content': full_content.strip(),
                    'model': self.vlm_model,
                    'provider': 'openai_compatible',
                    'attempt': attempt + 1
                }

            except Exception as e:
                error_msg = f"VLM调用失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}"
                self.logger.error(error_msg)

                # 如果是图片格式错误或400错误，不进行重试
                if "400" in str(e) or "图片输入格式" in str(e) or "解析错误" in str(e):
                    self.logger.error("检测到图片格式错误，不进行重试")
                    return {
                        'success': False,
                        'error': f"图片格式错误: {str(e)}",
                        'model': self.vlm_model,
                        'final_attempt': True
                    }

                # 如果不是最后一次尝试，等待后重试
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2  # 递增等待时间: 2, 4, 6秒
                    self.logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    # 最后一次尝试失败
                    self.logger.error(error_msg, exc_info=True)
                    return {
                        'success': False,
                        'error': error_msg,
                        'model': self.vlm_model,
                        'total_attempts': max_retries
                    }

    def _make_request(self, prompt: str, model_name: str, temperature: float) -> Dict[str, Any]:
        """
        执行具体的LLM请求，支持streaming

        Args:
            prompt: 提示词
            model_name: 模型名称
            temperature: 生成温度

        Returns:
            响应结果字典
        """
        try:
            self.logger.info(f"调用LLM: {model_name}")
            self.logger.info(f"提示词长度: {len(prompt)} 字符")

            # 创建streaming请求
            response = self.client.chat.completions.create(
                model=model_name,
                messages=[
                    {'role': 'system', 'content': '你是一个专业的内容分析师，擅长总结和提取关键信息。'},
                    {'role': 'user', 'content': prompt}
                ],
                temperature=temperature,
                stream=True
            )

            # 收集所有streaming内容
            full_content = ""
            reasoning_content_full = ""
            chunk_count = 0

            self.logger.info("开始streaming响应处理...")

            for chunk in response:
                chunk_count += 1
                delta = chunk.choices[0].delta

                # 安全地获取reasoning_content和content
                reasoning_content = getattr(delta, 'reasoning_content', None)
                content_chunk = getattr(delta, 'content', None)

                if reasoning_content:
                    # 推理内容单独收集，但不加入最终结果
                    reasoning_content_full += reasoning_content
                    self.logger.debug(f"Chunk {chunk_count} - Reasoning: {reasoning_content[:50]}...")

                if content_chunk:
                    # 只收集最终的content内容
                    full_content += content_chunk
                    self.logger.debug(f"Chunk {chunk_count} - Content: {content_chunk[:50]}...")

            self.logger.info(f"LLM调用完成 - 处理了 {chunk_count} 个chunks")
            self.logger.info(f"响应内容长度: {len(full_content)} 字符")

            return {
                'success': True,
                'content': full_content.strip(),
                'model': model_name,
                'provider': 'openai_compatible'
            }

        except Exception as e:
            error_msg = f"LLM调用失败: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return {
                'success': False,
                'error': error_msg,
                'model': model_name
            }

    def analyze_content(self, content: str, prompt_template: str) -> Dict[str, Any]:
        """使用快速模型分析内容（保持向后兼容性）"""
        try:
            # 格式化提示词
            prompt = prompt_template.format(content=content)
            return self.call_fast_model(prompt)

        except Exception as e:
            error_msg = f"内容分析失败: {str(e)}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'provider': 'openai_compatible'
            }


# 全局LLM客户端实例
try:
    llm_client = LLMClient()
except Exception as e:
    logging.getLogger(__name__).warning(f"LLM客户端初始化失败: {e}")
    llm_client = None
