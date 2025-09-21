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


def batch_validate_image_urls(image_urls: List[str], max_workers: int = 10) -> Dict[str, bool]:
    """
    批量并发验证图片URL

    Args:
        image_urls: 图片URL列表
        max_workers: 最大并发数

    Returns:
        URL到验证结果的映射
    """
    if not image_urls:
        return {}

    # 去重
    unique_urls = list(set(image_urls))
    logger.debug(f"开始批量验证{len(unique_urls)}个唯一图片URL...")

    # 先进行格式验证（无网络开销）
    format_valid_urls = []
    format_failed = 0

    for url in unique_urls:
        clean_url = normalize_image_url(url)
        if clean_url and is_valid_image_url(clean_url):
            format_valid_urls.append(clean_url)
        else:
            format_failed += 1

    # 并发进行网络可访问性验证
    url_validation_results = {}
    access_failed = 0

    if format_valid_urls:
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="URLValidation") as executor:
            # 提交所有验证任务
            future_to_url = {
                executor.submit(validate_image_url_accessible, url, 3): url
                for url in format_valid_urls
            }

            # 收集结果
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    is_accessible = future.result()
                    url_validation_results[url] = is_accessible
                    if not is_accessible:
                        access_failed += 1
                except Exception as e:
                    logger.debug(f"验证URL {url} 时发生异常: {e}")
                    url_validation_results[url] = False
                    access_failed += 1

    # 为所有原始 URL 生成结果映射
    result_map = {}
    for original_url in image_urls:
        clean_url = normalize_image_url(original_url)
        if clean_url and is_valid_image_url(clean_url):
            result_map[original_url] = url_validation_results.get(clean_url, False)
        else:
            result_map[original_url] = False

    # 统计日志
    valid_count = sum(1 for v in result_map.values() if v)
    logger.info(
        f"批量图片验证完成: 原始{len(image_urls)}个 -> 有效{valid_count}个 "
        f"(格式失败:{format_failed}个, 访问失败:{access_failed}个)"
    )

    return result_map


def extract_image_urls_from_markdown(markdown_content: str, skip_accessibility_check: bool = False) -> List[str]:
    """
    从Markdown内容中提取图片URL（简化版，不进行网络验证）

    Args:
        markdown_content: Markdown格式的内容
        skip_accessibility_check: 是否跳过可访问性检查（此参数已废弃，保留以兼容）

    Returns:
        经过格式验证的图片URL列表
    """
    if not markdown_content:
        return []

    # 匹配Markdown图片语法 ![alt](url)
    img_pattern = r'!\[.*?\]\((https?://[^)]+)\)'
    urls = re.findall(img_pattern, markdown_content)

    if not urls:
        return []

    # 只进行格式验证，不进行网络验证
    valid_urls = []
    for url in urls:
        clean_url = normalize_image_url(url)
        if clean_url and is_valid_image_url(clean_url):
            valid_urls.append(clean_url)

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

        # 获取DEBUG模式配置
        debug_mode = config.get_logging_config().get('debug_mode', True)

        # 获取未处理的帖子
        unprocessed_posts = self.db_manager.get_unprocessed_posts(hours_back)
        total_posts = len(unprocessed_posts)

        if total_posts == 0:
            self.logger.info("没有找到需要处理的帖子")
            return {'total': 0, 'success': 0, 'failed': 0}

        # 分类帖子：有图片的和纯文本的，同时收集所有图片URL
        vlm_posts = []
        llm_posts = []
        all_image_urls = []  # 收集所有需要验证的图片URL
        post_image_mapping = {}  # 帖子到图片URL的映射

        # 统计信息（只在debug模式下详细记录）
        if debug_mode:
            image_validation_stats = {
                'total_posts_with_images': 0,
                'total_image_urls': 0,
                'valid_image_urls': 0,
                'posts_downgraded_to_llm': 0
            }

        # 第一步：快速分类和收集图片URL
        for post in unprocessed_posts:
            post_text = post.get('summary', '') or post.get('title', '')

            # 检查是否包含图片（只进行格式验证）
            img_pattern = r'!\[.*?\]\((https?://[^)]+)\)'
            raw_image_urls = re.findall(img_pattern, post_text)

            if raw_image_urls:
                # 进行格式验证
                format_valid_urls = extract_image_urls_from_markdown(post_text)

                if format_valid_urls:
                    vlm_posts.append(post)
                    post_image_mapping[post['id']] = format_valid_urls
                    all_image_urls.extend(format_valid_urls)

                    if debug_mode:
                        image_validation_stats['total_posts_with_images'] += 1
                        image_validation_stats['total_image_urls'] += len(raw_image_urls)
                else:
                    # 格式验证全部失败，直接降级为LLM
                    llm_posts.append(post)
                    if debug_mode:
                        image_validation_stats['total_posts_with_images'] += 1
                        image_validation_stats['total_image_urls'] += len(raw_image_urls)
                        image_validation_stats['posts_downgraded_to_llm'] += 1
            else:
                llm_posts.append(post)

        # 第二步：批量并发验证所有图片URL
        url_validation_results = {}
        if all_image_urls:
            logger.info(f"开始批量验证{len(set(all_image_urls))}个唯一图片URL...")
            url_validation_results = batch_validate_image_urls(all_image_urls, max_workers=8)

            # 根据验证结果重新分类帖子
            vlm_posts_final = []
            for post in vlm_posts:
                post_urls = post_image_mapping[post['id']]
                valid_urls = [url for url in post_urls if url_validation_results.get(url, False)]

                if valid_urls:
                    vlm_posts_final.append(post)
                    post_image_mapping[post['id']] = valid_urls  # 更新为有效的URL
                else:
                    # 所有图片都无效，降级为LLM
                    llm_posts.append(post)
                    if debug_mode:
                        image_validation_stats['posts_downgraded_to_llm'] += 1
                        logger.debug(f"帖子{post['id']}的所有图片URL都无法访问，降级为LLM处理")

            vlm_posts = vlm_posts_final

            if debug_mode:
                image_validation_stats['valid_image_urls'] = sum(1 for v in url_validation_results.values() if v)

        # 打印分类统计（简化版或详细版）
        if debug_mode:
            self.logger.info("="*60)
            self.logger.info("帖子分类和图片验证统计:")
            self.logger.info(f"  总帖子数: {total_posts}")
            self.logger.info(f"  包含图片的帖子: {image_validation_stats['total_posts_with_images']}")
            self.logger.info(f"  纯文本帖子: {total_posts - image_validation_stats['total_posts_with_images']}")
            self.logger.info(f"  VLM任务: {len(vlm_posts)}个")
            self.logger.info(f"  LLM任务: {len(llm_posts)}个")

            if image_validation_stats['total_image_urls'] > 0:
                self.logger.info("图片URL验证详情:")
                self.logger.info(f"  总图片URL数: {image_validation_stats['total_image_urls']}")
                self.logger.info(f"  有效图片URL数: {image_validation_stats['valid_image_urls']}")
                self.logger.info(f"  无效图片URL数: {image_validation_stats['total_image_urls'] - image_validation_stats['valid_image_urls']}")
                self.logger.info(f"  图片验证成功率: {image_validation_stats['valid_image_urls']/image_validation_stats['total_image_urls']*100:.1f}%")
                self.logger.info(f"  因图片全部无效而降级的帖子: {image_validation_stats['posts_downgraded_to_llm']}")
            self.logger.info("="*60)
        else:
            self.logger.info(f"帖子分类完成: 总数{total_posts}, VLM任务{len(vlm_posts)}个, LLM任务{len(llm_posts)}个")

        # 使用两个独立的线程池处理不同类型的任务
        success_count = 0
        failed_count = 0

        # Debug模式下的详细统计
        if debug_mode:
            vlm_success = 0
            vlm_failed = 0
            llm_success = 0
            llm_failed = 0
            vlm_format_errors = 0
            vlm_downgraded = 0

        # 创建两个线程池分别处理VLM和LLM任务
        with ThreadPoolExecutor(max_workers=self.fast_vlm_workers, thread_name_prefix="VLM") as vlm_executor, \
             ThreadPoolExecutor(max_workers=self.fast_llm_workers, thread_name_prefix="LLM") as llm_executor:

            # 提交VLM任务
            if debug_mode:
                vlm_futures = {
                    vlm_executor.submit(self._process_vlm_post_with_stats, post, post_image_mapping.get(post['id'], [])): post
                    for post in vlm_posts
                }
            else:
                vlm_futures = {
                    vlm_executor.submit(self._process_vlm_post, post, post_image_mapping.get(post['id'], [])): post
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
                is_vlm_task = future in vlm_futures

                try:
                    if debug_mode and is_vlm_task:
                        # Debug模式下VLM任务返回详细结果
                        result = future.result()
                        if result['success']:
                            success_count += 1
                            vlm_success += 1
                            if result.get('downgraded', False):
                                vlm_downgraded += 1
                        else:
                            failed_count += 1
                            vlm_failed += 1
                            if result.get('format_error', False):
                                vlm_format_errors += 1
                    else:
                        # 普通模式或LLM任务返回布尔值
                        success = future.result()
                        if success:
                            success_count += 1
                            if debug_mode:
                                if is_vlm_task:
                                    vlm_success += 1
                                else:
                                    llm_success += 1
                        else:
                            failed_count += 1
                            if debug_mode:
                                if is_vlm_task:
                                    vlm_failed += 1
                                else:
                                    llm_failed += 1

                    # 定期打印进度信息
                    progress_interval = 5 if debug_mode else 10
                    if i % progress_interval == 0 or i == total_posts:
                        if debug_mode:
                            progress_percent = (i / total_posts) * 100
                            self.logger.info(
                                f"处理进度: {i}/{total_posts} ({progress_percent:.1f}%) | "
                                f"成功:{success_count} 失败:{failed_count} | "
                                f"VLM成功:{vlm_success} VLM失败:{vlm_failed} | "
                                f"LLM成功:{llm_success} LLM失败:{llm_failed}"
                            )
                        else:
                            self.logger.info(f"处理进度: {i}/{total_posts} ({success_count}成功, {failed_count}失败)")

                except Exception as e:
                    self.logger.error(f"处理帖子{post['id']}时发生异常: {e}", exc_info=True)
                    failed_count += 1
                    if debug_mode:
                        if is_vlm_task:
                            vlm_failed += 1
                        else:
                            llm_failed += 1

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

        # 构建结果
        result = {
            'total': total_posts,
            'success': success_count,
            'failed': failed_count,
            'vlm_posts': len(vlm_posts),
            'llm_posts': len(llm_posts)
        }

        # Debug模式下添加详细统计
        if debug_mode:
            result.update({
                'vlm_success': vlm_success,
                'vlm_failed': vlm_failed,
                'llm_success': llm_success,
                'llm_failed': llm_failed,
                'vlm_format_errors': vlm_format_errors,
                'vlm_downgraded': vlm_downgraded,
                **image_validation_stats
            })

        # 最终统计日志
        if debug_mode:
            self.logger.info("="*60)
            self.logger.info("处理完成，最终统计:")
            self.logger.info(f"  总体成功率: {success_count}/{total_posts} ({success_count/total_posts*100:.1f}%)")
            self.logger.info(f"  VLM任务: {vlm_success}/{len(vlm_posts)} 成功 ({vlm_success/len(vlm_posts)*100:.1f}% 成功率)" if vlm_posts else "  VLM任务: 0个")
            self.logger.info(f"  LLM任务: {llm_success}/{len(llm_posts)} 成功 ({llm_success/len(llm_posts)*100:.1f}% 成功率)" if llm_posts else "  LLM任务: 0个")
            if vlm_format_errors > 0:
                self.logger.info(f"  VLM图片格式错误: {vlm_format_errors}个")
            if vlm_downgraded > 0:
                self.logger.info(f"  VLM失败后降级成功: {vlm_downgraded}个")
            self.logger.info("="*60)
        else:
            self.logger.info(f"分类并发处理完成: {result}")

        return result

    def _process_vlm_post(self, post: Dict[str, Any], validated_image_urls: List[str] = None) -> bool:
        """
        处理需要VLM的帖子，使用预验证的图片URL

        Args:
            post: 帖子信息字典
            validated_image_urls: 预验证的图片URL列表

        Returns:
            是否处理成功
        """
        post_id = post['id']
        post_text = post.get('summary', '') or post.get('title', '')

        if not post_text:
            self.logger.warning(f"VLM帖子{post_id}没有内容，跳过处理")
            return False

        # 使用预验证的图片URL
        if validated_image_urls:
            image_urls = validated_image_urls
        else:
            # 如果没有预验证的URL，降级为纯文本处理
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

    def _process_vlm_post_with_stats(self, post: Dict[str, Any], validated_image_urls: List[str] = None) -> Dict[str, Any]:
        """
        处理需要VLM的帖子，返回详细统计信息（Debug模式）

        Args:
            post: 帖子信息字典
            validated_image_urls: 预验证的图片URL列表

        Returns:
            包含成功状态和详细信息的字典
        """
        post_id = post['id']
        post_text = post.get('summary', '') or post.get('title', '')

        result = {
            'success': False,
            'downgraded': False,
            'format_error': False
        }

        if not post_text:
            self.logger.warning(f"VLM帖子{post_id}没有内容，跳过处理")
            return result

        # 使用预验证的图片URL
        if validated_image_urls:
            image_urls = validated_image_urls
        else:
            # 如果没有预验证的URL，降级为纯文本处理
            self.logger.info(f"帖子{post_id}没有有效图片URL，降级为LLM处理")
            success = self._process_llm_post(post)
            result['success'] = success
            result['downgraded'] = success
            return result

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
                result['success'] = True
                return result
            else:
                # VLM失败时，检查是否是格式错误
                result['format_error'] = True

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
                    result['success'] = True
                    result['downgraded'] = True
                    return result
                else:
                    # 记录最终失败
                    self.db_manager.save_post_interpretation(
                        post_id,
                        "VLM和LLM处理均失败",
                        "failed",
                        'failed'
                    )
                    return result

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
            return result

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