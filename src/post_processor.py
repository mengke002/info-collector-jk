"""
Post后处理模块
实现即刻Post的智能解读和分析，支持并发处理
"""
import logging
import re
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import config
from .database import DatabaseManager
from .llm_client import LLMClient

logger = logging.getLogger(__name__)


def normalize_image_url(url: str) -> str:
    """
    移除URL的查询参数和片段，只保留干净的路径

    Args:
        url: 原始图片URL

    Returns:
        清理后的URL
    """
    if not url:
        return ""

    try:
        parsed = urlparse(url)
        # 只保留 scheme, netloc, path
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
    except Exception as e:
        logger.warning(f"URL标准化失败: {url}, 错误: {e}")
        return url


def extract_image_urls_from_markdown(markdown_content: str) -> List[str]:
    """
    从Markdown内容中提取图片URL

    Args:
        markdown_content: Markdown格式的内容

    Returns:
        图片URL列表
    """
    if not markdown_content:
        return []

    # 匹配Markdown图片语法 ![alt](url)
    img_pattern = r'!\[.*?\]\((https?://[^)]+)\)'
    urls = re.findall(img_pattern, markdown_content)

    # 清理和标准化URL
    clean_urls = []
    for url in urls:
        clean_url = normalize_image_url(url)
        if clean_url:
            clean_urls.append(clean_url)

    return clean_urls


def get_vlm_prompt(post_text: str) -> str:
    """
    获取视觉多模态模型的提示词

    Args:
        post_text: Post文本内容

    Returns:
        格式化的提示词
    """
    return f"""# Role: 社交媒体内容分析师

# Context:
你正在分析一条来自社交媒体的 post。这条 post 包含文本和一张或多张图片。你的任务是深度融合文本和图片信息，提取信息与价值。

# Input:
- Post 文本: "{post_text}"
- 图片: 参考附件

# Your Task:
请结合给定的文本和所有图片，完成以下分析，并按顺序输出：
1.  **原始发文**: 原始post内容及简单说明
2.  **图片信息**: 每张图片分别展示了什么内容？有什么意义？它们如何与文本内容关联？
3.  **深入解读**: 结合post内容和图片信息，做1个深度解读，分析作者的情绪、观点以及他/她真正想传达的核心思想。

严格遵循上述输出要求。"""


def get_llm_prompt(post_text: str) -> str:
    """
    获取纯文本模型的提示词

    Args:
        post_text: Post文本内容

    Returns:
        格式化的提示词
    """
    return f"""# Role: 社交媒体内容分析师

# Context:
你正在分析一条来自社交媒体的纯文本 post。你的任务是深度挖掘文本背后的信息、情绪和潜在意图。

# Input:
- Post 文本: "{post_text}"

# Your Task:
请分析给定的文本，完成以下任务，并按顺序输出：
1.  **原始发文**: 原始post内容及简单概括
2.  **核心观点与主题**: 以列表形式，提炼出Post的核心观点、讨论的主题或关键信息
3.  **情绪与语气**: 分析作者在字里行间流露出的情绪（如喜悦、深思、批判等）和整体语气
4.  **深入解读**: 结合以上分析，做一个深度解读。推断作者发表这篇Post的可能动机，以及他/她希望引发读者怎样的思考或共鸣。

严格遵循上述输出要求。"""


class PostProcessor:
    """Post后处理器，负责智能解读和分析Post内容"""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        初始化Post处理器

        Args:
            db_manager: 数据库管理器，如果为None则创建新实例
        """
        self.db_manager = db_manager or DatabaseManager()
        self.llm_client = LLMClient()
        self.logger = logging.getLogger(__name__)

        # 获取模型配置
        self.llm_config = config.get_llm_config()
        self.fast_model = self.llm_config['fast_model_name']
        self.vlm_model = self.llm_config['fast_vlm_name']

        # 获取并发配置
        executor_config = config.get_executor_config()
        self.fast_llm_workers = executor_config['fast_llm_workers']
        self.fast_vlm_workers = executor_config['fast_vlm_workers']

        self.logger.info("Post处理器初始化完成")
        self.logger.info(f"快速文本模型: {self.fast_model} (并发数: {self.fast_llm_workers})")
        self.logger.info(f"视觉多模态模型: {self.vlm_model} (并发数: {self.fast_vlm_workers})")

    def process_unprocessed_posts(self, hours_back: int = 36) -> Dict[str, int]:
        """
        分类并发处理未解读的帖子

        Args:
            hours_back: 回溯小时数，默认36小时

        Returns:
            处理统计信息
        """
        self.logger.info(f"开始分类并发处理未解读的帖子，回溯{hours_back}小时")

        # 获取未处理的帖子
        unprocessed_posts = self.db_manager.get_unprocessed_posts(hours_back)
        total_posts = len(unprocessed_posts)

        if total_posts == 0:
            self.logger.info("没有找到需要处理的帖子")
            return {'total': 0, 'success': 0, 'failed': 0}

        # 分类帖子：有图片的和纯文本的
        vlm_posts = []
        llm_posts = []

        for post in unprocessed_posts:
            post_text = post.get('summary', '') or post.get('title', '')
            image_urls = extract_image_urls_from_markdown(post_text)
            if len(image_urls) > 0:
                vlm_posts.append(post)
            else:
                llm_posts.append(post)

        self.logger.info(f"帖子分类完成: 总数{total_posts}, VLM任务{len(vlm_posts)}个, LLM任务{len(llm_posts)}个")

        # 使用两个独立的线程池处理不同类型的任务
        success_count = 0
        failed_count = 0

        # 创建两个线程池分别处理VLM和LLM任务
        with ThreadPoolExecutor(max_workers=self.fast_vlm_workers, thread_name_prefix="VLM") as vlm_executor, \
             ThreadPoolExecutor(max_workers=self.fast_llm_workers, thread_name_prefix="LLM") as llm_executor:

            # 提交VLM任务
            vlm_futures = {
                vlm_executor.submit(self._process_vlm_post, post): post
                for post in vlm_posts
            }

            # 提交LLM任务
            llm_futures = {
                llm_executor.submit(self._process_llm_post, post): post
                for post in llm_posts
            }

            # 合并所有futures
            all_futures = {**vlm_futures, **llm_futures}

            # 处理完成的任务
            for i, future in enumerate(as_completed(all_futures), 1):
                post = all_futures[future]
                try:
                    success = future.result()
                    if success:
                        success_count += 1
                    else:
                        failed_count += 1

                    if i % 10 == 0 or i == total_posts:
                        self.logger.info(f"处理进度: {i}/{total_posts} ({success_count}成功, {failed_count}失败)")

                except Exception as e:
                    self.logger.error(f"处理帖子{post['id']}时发生异常: {e}", exc_info=True)
                    failed_count += 1

                    # 记录失败状态
                    try:
                        self.db_manager.save_post_interpretation(
                            post['id'],
                            f"并发处理异常: {str(e)}",
                            "error",
                            'failed'
                        )
                    except Exception as save_error:
                        self.logger.error(f"保存失败状态时出错: {save_error}")

        result = {
            'total': total_posts,
            'success': success_count,
            'failed': failed_count,
            'vlm_posts': len(vlm_posts),
            'llm_posts': len(llm_posts)
        }

        self.logger.info(f"分类并发处理完成: {result}")
        return result

    def _process_vlm_post(self, post: Dict[str, Any]) -> bool:
        """
        处理需要VLM的帖子

        Args:
            post: 帖子信息字典

        Returns:
            是否处理成功
        """
        post_id = post['id']
        post_text = post.get('summary', '') or post.get('title', '')

        if not post_text:
            self.logger.warning(f"VLM帖子{post_id}没有内容，跳过处理")
            return False

        # 提取图片URL
        image_urls = extract_image_urls_from_markdown(post_text)

        try:
            self.logger.debug(f"VLM处理帖子{post_id}，包含{len(image_urls)}张图片")
            interpretation = self._call_vlm_for_post(post_text, image_urls)

            if interpretation:
                # 保存解读结果
                self.db_manager.save_post_interpretation(
                    post_id,
                    interpretation,
                    self.vlm_model,
                    'success'
                )
                self.logger.debug(f"VLM帖子{post_id}处理成功")
                return True
            else:
                self.logger.error(f"VLM帖子{post_id}解读失败，VLM返回空结果")
                return False

        except Exception as e:
            self.logger.error(f"VLM处理帖子{post_id}时出错: {e}")
            return False

    def _process_llm_post(self, post: Dict[str, Any]) -> bool:
        """
        处理纯文本帖子

        Args:
            post: 帖子信息字典

        Returns:
            是否处理成功
        """
        post_id = post['id']
        post_text = post.get('summary', '') or post.get('title', '')

        if not post_text:
            self.logger.warning(f"LLM帖子{post_id}没有内容，跳过处理")
            return False

        try:
            self.logger.debug(f"LLM处理纯文本帖子{post_id}")
            interpretation = self._call_llm_for_post(post_text)

            if interpretation:
                # 保存解读结果
                self.db_manager.save_post_interpretation(
                    post_id,
                    interpretation,
                    self.fast_model,
                    'success'
                )
                self.logger.debug(f"LLM帖子{post_id}处理成功")
                return True
            else:
                self.logger.error(f"LLM帖子{post_id}解读失败，LLM返回空结果")
                return False

        except Exception as e:
            self.logger.error(f"LLM处理帖子{post_id}时出错: {e}")
            return False

    def _call_vlm_for_post(self, post_text: str, image_urls: List[str]) -> Optional[str]:
        """
        调用视觉多模态模型处理带图片的Post

        Args:
            post_text: Post文本内容
            image_urls: 图片URL列表

        Returns:
            LLM解读结果，失败时返回None
        """
        prompt = get_vlm_prompt(post_text)

        try:
            # 调用VLM API
            result = self.llm_client.call_vlm(prompt, image_urls)

            if result.get('success'):
                return result.get('content')
            else:
                self.logger.error(f"VLM调用失败: {result.get('error')}")
                return None

        except Exception as e:
            self.logger.error(f"VLM调用异常: {e}")
            return None

    def _call_llm_for_post(self, post_text: str) -> Optional[str]:
        """
        调用LLM处理纯文本Post

        Args:
            post_text: Post文本内容

        Returns:
            LLM解读结果，失败时返回None
        """
        prompt = get_llm_prompt(post_text)

        try:
            # 调用快速模型
            result = self.llm_client.call_fast_model(prompt)

            if result.get('success'):
                return result.get('content')
            else:
                self.logger.error(f"LLM调用失败: {result.get('error')}")
                return None

        except Exception as e:
            self.logger.error(f"LLM调用异常: {e}")
            return None


def run_post_processing(hours_back: int = 36) -> Dict[str, int]:
    """
    运行Post后处理流程的便捷函数

    Args:
        hours_back: 回溯小时数

    Returns:
        处理统计信息
    """
    processor = PostProcessor()
    return processor.process_unprocessed_posts(hours_back)


if __name__ == "__main__":
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # 运行处理流程
    result = run_post_processing()
    print(f"处理结果: {result}")