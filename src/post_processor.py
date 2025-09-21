"""
Post后处理模块
实现即刻Post的智能解读和分析，支持并发处理
"""
import logging
import re
import requests
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


def is_valid_image_url(url: str) -> bool:
    """
    检查URL是否为有效的图片URL格式

    Args:
        url: 图片URL

    Returns:
        是否为有效的图片URL
    """
    if not url:
        return False

    try:
        parsed = urlparse(url)
        # 检查是否为HTTP/HTTPS协议
        if parsed.scheme not in ['http', 'https']:
            return False

        # 检查是否有有效的域名
        if not parsed.netloc:
            return False

        # 检查文件扩展名是否为常见图片格式
        path = parsed.path.lower()
        valid_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg']

        # 如果URL末尾有图片扩展名，直接验证
        for ext in valid_extensions:
            if path.endswith(ext):
                return True

        # 对于没有明确扩展名的URL，也允许通过（可能是动态图片URL）
        # 但至少要有路径
        return bool(path and path != '/')

    except Exception as e:
        logger.warning(f"URL格式验证失败: {url}, 错误: {e}")
        return False


def validate_image_url_accessible(url: str, timeout: int = 3) -> bool:
    """
    轻量级验证图片URL是否可访问（可选验证，用于关键场景）

    Args:
        url: 图片URL
        timeout: 请求超时时间（秒）

    Returns:
        图片是否可访问
    """
    try:
        # 使用HEAD请求，不下载图片内容，只获取头部信息
        response = requests.head(url, timeout=timeout, allow_redirects=True)

        # 检查HTTP状态码
        if response.status_code != 200:
            logger.warning(f"图片URL返回非200状态码: {url} -> {response.status_code}")
            return False

        # 可选：检查Content-Type是否为图片类型（放宽条件）
        content_type = response.headers.get('content-type', '').lower()
        if content_type and not any(ct in content_type for ct in ['image/', 'application/octet-stream']):
            logger.debug(f"图片URL的Content-Type可能不是图片类型: {url} -> {content_type}")
            # 不直接返回False，让VLM API自己判断

        logger.debug(f"图片URL验证成功: {url}")
        return True

    except requests.exceptions.Timeout:
        logger.warning(f"图片URL访问超时: {url}")
        return False
    except requests.exceptions.RequestException as e:
        logger.warning(f"图片URL访问失败: {url}, 错误: {e}")
        return False
    except Exception as e:
        logger.error(f"图片URL验证异常: {url}, 错误: {e}")
        return False


def extract_image_urls_from_markdown(markdown_content: str, skip_accessibility_check: bool = True) -> List[str]:
    """
    从Markdown内容中提取图片URL，并进行格式验证

    Args:
        markdown_content: Markdown格式的内容
        skip_accessibility_check: 是否跳过可访问性检查（默认跳过以提高速度）

    Returns:
        经过验证的图片URL列表
    """
    if not markdown_content:
        return []

    # 匹配Markdown图片语法 ![alt](url)
    img_pattern = r'!\[.*?\]\((https?://[^)]+)\)'
    urls = re.findall(img_pattern, markdown_content)

    # 清理、标准化和验证URL
    valid_urls = []
    for url in urls:
        # 标准化URL
        clean_url = normalize_image_url(url)
        if not clean_url:
            continue

        # 格式验证（快速，本地验证）
        if not is_valid_image_url(clean_url):
            logger.warning(f"图片URL格式无效，跳过: {clean_url}")
            continue

        # 可访问性验证（可选，需要网络请求）
        if not skip_accessibility_check:
            if not validate_image_url_accessible(clean_url, timeout=3):
                logger.warning(f"图片URL无法访问，跳过: {clean_url}")
                continue

        valid_urls.append(clean_url)
        logger.debug(f"图片URL验证通过: {clean_url}")

    if skip_accessibility_check:
        logger.info(f"从{len(urls)}个原始图片URL中格式验证通过{len(valid_urls)}个（跳过可访问性检查以提高速度）")
    else:
        logger.info(f"从{len(urls)}个原始图片URL中完全验证通过{len(valid_urls)}个")

    return valid_urls


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
        处理需要VLM的帖子，包含图片验证和错误处理

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

        # 提取并验证图片URL
        image_urls = extract_image_urls_from_markdown(post_text)

        # 如果没有有效的图片URL，降级为纯文本处理
        if not image_urls:
            self.logger.info(f"帖子{post_id}没有有效图片URL，降级为LLM处理")
            return self._process_llm_post(post)

        try:
            self.logger.debug(f"VLM处理帖子{post_id}，包含{len(image_urls)}张有效图片")
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
                # VLM失败时，保存失败记录并尝试降级为纯文本处理
                self.logger.warning(f"VLM帖子{post_id}解读失败，尝试降级为LLM处理")

                # 尝试纯文本处理作为降级方案
                fallback_interpretation = self._call_llm_for_post(post_text)
                if fallback_interpretation:
                    self.db_manager.save_post_interpretation(
                        post_id,
                        f"[降级处理] {fallback_interpretation}",
                        self.fast_model,
                        'success'
                    )
                    self.logger.info(f"帖子{post_id}降级为LLM处理成功")
                    return True
                else:
                    # 记录最终失败
                    self.db_manager.save_post_interpretation(
                        post_id,
                        "VLM和LLM处理均失败",
                        "failed",
                        'failed'
                    )
                    return False

        except Exception as e:
            self.logger.error(f"VLM处理帖子{post_id}时出错: {e}")
            # 记录错误状态
            try:
                self.db_manager.save_post_interpretation(
                    post_id,
                    f"处理异常: {str(e)}",
                    "error",
                    'failed'
                )
            except Exception as save_error:
                self.logger.error(f"保存错误状态失败: {save_error}")
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
        调用视觉多模态模型处理带图片的Post，包含增强的错误处理

        Args:
            post_text: Post文本内容
            image_urls: 图片URL列表

        Returns:
            LLM解读结果，失败时返回None
        """
        if not image_urls:
            self.logger.warning("没有有效的图片URL，无法调用VLM")
            return None

        prompt = get_vlm_prompt(post_text)

        try:
            self.logger.info(f"准备调用VLM处理{len(image_urls)}张图片")
            # 调用VLM API（已包含重试机制）
            result = self.llm_client.call_vlm(prompt, image_urls)

            if result.get('success'):
                content = result.get('content')
                if content and content.strip():
                    self.logger.info(f"VLM调用成功，返回内容长度: {len(content)} 字符")
                    return content
                else:
                    self.logger.error("VLM调用成功但返回空内容")
                    return None
            else:
                error_info = result.get('error', '未知错误')

                # 如果是图片格式错误，记录详细信息
                if result.get('final_attempt'):
                    self.logger.error(f"VLM调用因图片格式错误终止: {error_info}")
                else:
                    self.logger.error(f"VLM调用失败: {error_info}")

                return None

        except Exception as e:
            self.logger.error(f"VLM调用异常: {e}", exc_info=True)
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