"""LLM客户端模块
支持OpenAI compatible接口的streaming实现
"""
import logging
from typing import Dict, Any
from openai import OpenAI

try:
    from .config import config
except ImportError:
    # 当作脚本直接运行时的导入
    from config import config


class LLMClient:
    """简化的LLM客户端，仅支持OpenAI compatible接口"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # 从配置文件获取配置（按优先级：环境变量 > config.ini > 默认值）
        llm_config = config.get_llm_config()
        self.api_key = llm_config.get('openai_api_key')
        self.model = llm_config.get('openai_model', 'gpt-3.5-turbo')
        self.base_url = llm_config.get('openai_base_url', 'https://api.openai.com/v1')
        
        if not self.api_key:
            raise ValueError("未找到OPENAI_API_KEY配置，请在环境变量或config.ini中设置")
        
        # 初始化OpenAI客户端
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )
        
        self.logger.info(f"LLM客户端初始化成功 - Model: {self.model}, Base URL: {self.base_url}")
    
    def analyze_content(self, content: str, prompt_template: str) -> Dict[str, Any]:
        """使用streaming方式分析内容"""
        try:
            # 格式化提示词
            prompt = prompt_template.format(content=content)
            
            # 调试输出：显示请求信息（仅在DEBUG级别显示详细内容）
            self.logger.info("开始LLM内容分析...")
            self.logger.debug("=== LLM 请求调试信息 ===")
            self.logger.debug(f"模型: {self.model}")
            self.logger.info(f"内容长度: {len(content)} 字符")
            self.logger.debug(f"提示词长度: {len(prompt)} 字符")
            self.logger.debug(f"内容预览: {content[:200]}...")
            
            # 创建streaming请求
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {'role': 'system', 'content': '你是一个专业的内容分析师，擅长总结和提取关键信息。'},
                    {'role': 'user', 'content': prompt}
                ],
                temperature=0.3,
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
            
            # 调试输出：显示响应结果（敏感信息仅在DEBUG级别显示）
            self.logger.info("LLM分析完成")
            self.logger.debug("=== LLM 响应调试信息 ===")
            self.logger.info(f"处理了 {chunk_count} 个 chunks")
            if reasoning_content_full:
                self.logger.info(f"推理内容长度: {len(reasoning_content_full)} 字符")
                self.logger.debug(f"推理内容预览: {reasoning_content_full[:200]}...")
            self.logger.info(f"最终内容长度: {len(full_content)} 字符")
            self.logger.info(f"最终内容预览: {full_content[:300]}...")
            self.logger.debug("=== LLM 最终内容 ===")
            self.logger.debug(full_content)
            self.logger.debug("=== LLM 响应结束 ===")
            
            return {
                'success': True,
                'content': full_content,
                'provider': 'openai_compatible',
                'model': self.model
            }
            
        except Exception as e:
            error_msg = f"LLM分析失败: {str(e)}"
            self.logger.error(error_msg)
            self.logger.error(f"错误类型: {type(e).__name__}")
            import traceback
            self.logger.error(f"堆栈追踪: {traceback.format_exc()}")
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
