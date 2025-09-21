"""
Post后处理模块
实现即刻Post的智能解读和分析，支持并发处理
"""
import logging
import re
import requests
import base64
import os
import tempfile
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    logging.getLogger(__name__).warning("PIL/Pillow未安装，无法进行图片格式转换。请安装: pip install pillow")

try:
    from pillow_heif import register_heif_opener
    register_heif_opener()
    HEIF_AVAILABLE = True
except ImportError:
    HEIF_AVAILABLE = False
    logging.getLogger(__name__).warning("pillow-heif未安装，无法处理HEIC格式图片。请安装: pip install pillow-heif")

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


def download_and_convert_image(url: str, target_format: str = 'PNG', timeout: int = 10) -> Optional[str]:
    """
    下载图片并转换为指定格式，返回base64编码

    Args:
        url: 图片URL
        target_format: 目标格式，默认PNG
        timeout: 下载超时时间

    Returns:
        base64编码的图片数据，失败时返回None
    """
    if not PIL_AVAILABLE:
        logger.error("PIL/Pillow未安装，无法转换图片格式")
        return None

    try:
        # 下载图片
        response = requests.get(url, timeout=timeout, stream=True)
        response.raise_for_status()

        # 检查内容大小，避免下载过大的文件
        content_length = response.headers.get('content-length')
        if content_length and int(content_length) > 50 * 1024 * 1024:  # 50MB限制
            logger.warning(f"图片文件过大 ({int(content_length)/(1024*1024):.1f}MB): {url}")
            return None

        # 创建临时文件
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name

            # 分块下载
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    temp_file.write(chunk)

        try:
            # 检查是否为HEIC格式
            url_lower = url.lower()
            is_heic = url_lower.endswith('.heic') or url_lower.endswith('.heif')

            if is_heic and not HEIF_AVAILABLE:
                logger.warning(f"HEIC格式图片需要pillow-heif支持，跳过转换: {url}")
                return None

            # 使用PIL打开并转换图片
            with Image.open(temp_path) as img:
                logger.debug(f"成功打开图片: {url}, 格式: {img.format}, 模式: {img.mode}, 尺寸: {img.size}")

                # 转换RGBA模式以支持透明度
                if img.mode in ('RGBA', 'LA'):
                    # 创建白色背景
                    background = Image.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'RGBA':
                        background.paste(img, mask=img.split()[-1])  # 使用alpha通道作为mask
                    else:
                        background.paste(img, mask=img.split()[-1])
                    img = background
                elif img.mode not in ('RGB', 'L'):
                    img = img.convert('RGB')

                # 保存为目标格式的临时文件
                with tempfile.NamedTemporaryFile(suffix=f'.{target_format.lower()}', delete=False) as converted_file:
                    converted_path = converted_file.name
                    img.save(converted_path, format=target_format, quality=85, optimize=True)

            # 读取转换后的图片并编码为base64
            with open(converted_path, 'rb') as f:
                image_data = f.read()
                base64_image = base64.b64encode(image_data).decode('utf-8')

            # 清理临时文件
            os.unlink(temp_path)
            os.unlink(converted_path)

            logger.debug(f"图片转换成功: {url} -> {target_format} ({len(image_data)} bytes)")
            return base64_image

        except Exception as e:
            logger.warning(f"图片转换失败: {url}, 错误: {e}")
            # 如果是HEIC格式且没有安装支持库，给出更具体的提示
            if url.lower().endswith(('.heic', '.heif')) and not HEIF_AVAILABLE:
                logger.info(f"HEIC格式图片需要安装pillow-heif: pip install pillow-heif")
            return None

    except requests.exceptions.Timeout:
        logger.warning(f"图片下载超时: {url}")
        return None
    except requests.exceptions.RequestException as e:
        logger.warning(f"图片下载失败: {url}, 错误: {e}")
        return None
    except Exception as e:
        logger.error(f"图片处理异常: {url}, 错误: {e}")
        return None
    finally:
        # 确保清理临时文件
        try:
            if 'temp_path' in locals() and os.path.exists(temp_path):
                os.unlink(temp_path)
            if 'converted_path' in locals() and os.path.exists(converted_path):
                os.unlink(converted_path)
        except:
            pass


def is_standard_format(url: str) -> bool:
    """
    检查URL是否为标准格式（PNG/JPG/JPEG），这些格式保持URL

    Args:
        url: 图片URL

    Returns:
        是否为标准格式
    """
    if not url:
        return False

    try:
        parsed = urlparse(url)
        path = parsed.path.lower()
        standard_extensions = ['.png', '.jpg', '.jpeg']
        return any(path.endswith(ext) for ext in standard_extensions)
    except Exception:
        return False


def batch_process_mixed_images(image_urls: List[str], max_workers: int = 8) -> Dict[str, Dict[str, Any]]:
    """
    批量处理图片：PNG/JPG/JPEG格式保持URL，其他格式转换为base64

    Args:
        image_urls: 图片URL列表
        max_workers: 最大并发数

    Returns:
        URL到处理结果的映射，每个结果包含type和data字段
        - type: 'url' 或 'base64'
        - data: URL字符串 或 base64字符串
        - success: 是否处理成功
    """
    if not image_urls:
        return {}

    # 去重并分类
    unique_urls = list(set(image_urls))
    standard_urls = []
    non_standard_urls = []

    for url in unique_urls:
        if is_standard_format(url):
            standard_urls.append(url)
        else:
            non_standard_urls.append(url)

    logger.info(f"开始批量处理{len(unique_urls)}个唯一图片URL: 标准格式{len(standard_urls)}个(PNG/JPG/JPEG保持URL), 其他格式{len(non_standard_urls)}个(转换为base64)")

    url_to_result = {}

    # 标准格式直接保持URL
    for url in standard_urls:
        url_to_result[url] = {
            'type': 'url',
            'data': url,
            'success': True
        }

    # 非标准格式需要下载并转换
    success_count = len(standard_urls)  # 标准格式URLs自动成功
    failed_count = 0

    if non_standard_urls:
        # 并发处理非标准图片
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="ImageProcessor") as executor:
            # 提交所有处理任务
            future_to_url = {
                executor.submit(download_and_convert_image, url): url
                for url in non_standard_urls
            }

            # 收集结果
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    base64_image = future.result()
                    if base64_image:
                        url_to_result[url] = {
                            'type': 'base64',
                            'data': base64_image,
                            'success': True
                        }
                        success_count += 1
                    else:
                        url_to_result[url] = {
                            'type': 'base64',
                            'data': None,
                            'success': False
                        }
                        failed_count += 1
                except Exception as e:
                    logger.error(f"处理图片 {url} 时发生异常: {e}")
                    url_to_result[url] = {
                        'type': 'base64',
                        'data': None,
                        'success': False
                    }
                    failed_count += 1

    # 为所有原始URL生成结果映射
    result_map = {}
    for original_url in image_urls:
        result_map[original_url] = url_to_result.get(original_url, {
            'type': 'url',
            'data': None,
            'success': False
        })

    logger.info(f"批量图片处理完成: 总数{len(image_urls)}个 -> 成功{success_count}个, 失败{failed_count}个")
    return result_map


def extract_image_urls_from_markdown(markdown_content: str) -> List[str]:
    """
    从Markdown内容中提取图片URL，只进行基本格式验证

    Args:
        markdown_content: Markdown格式的内容

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

    # 只进行基本的URL格式验证
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
        all_image_urls = []  # 收集所有需要处理的图片URL
        post_image_mapping = {}  # 帖子到图片URL的映射

        # 统计信息（只在debug模式下详细记录）
        if debug_mode:
            image_stats = {
                'total_posts_with_images': 0,
                'total_image_urls': 0,
                'format_valid_urls': 0,
                'posts_downgraded_to_llm': 0
            }

        # 第一步：快速分类和收集图片URL（只进行格式验证）
        for post in unprocessed_posts:
            post_text = post.get('summary', '') or post.get('title', '')

            # 检查是否包含图片（只进行格式验证）
            img_pattern = r'!\[.*?\]\((https?://[^)]+)\)'
            raw_image_urls = re.findall(img_pattern, post_text)

            if raw_image_urls:
                # 进行基本格式验证
                format_valid_urls = extract_image_urls_from_markdown(post_text)

                if format_valid_urls:
                    vlm_posts.append(post)
                    post_image_mapping[post['id']] = format_valid_urls
                    all_image_urls.extend(format_valid_urls)

                    if debug_mode:
                        image_stats['total_posts_with_images'] += 1
                        image_stats['total_image_urls'] += len(raw_image_urls)
                        image_stats['format_valid_urls'] += len(format_valid_urls)
                else:
                    # 格式验证全部失败，直接降级为LLM
                    llm_posts.append(post)
                    if debug_mode:
                        image_stats['total_posts_with_images'] += 1
                        image_stats['total_image_urls'] += len(raw_image_urls)
                        image_stats['posts_downgraded_to_llm'] += 1
            else:
                llm_posts.append(post)

        # 第二步：批量处理所有图片（PNG保持URL，非PNG转换为base64）
        image_processing_results = {}
        if all_image_urls:
            image_processing_results = batch_process_mixed_images(all_image_urls, max_workers=6)

            # 根据处理结果重新分类帖子
            vlm_posts_final = []
            for post in vlm_posts:
                post_urls = post_image_mapping[post['id']]
                valid_image_data = []

                for url in post_urls:
                    result = image_processing_results.get(url)
                    if result and result.get('success', False):
                        # 添加原始URL信息用于日志
                        img_data = {
                            'type': result['type'],
                            'data': result['data'],
                            'url': url,
                            'success': True
                        }
                        valid_image_data.append(img_data)

                if valid_image_data:
                    vlm_posts_final.append(post)
                    # 保存处理好的图片数据
                    post_image_mapping[post['id']] = valid_image_data
                else:
                    # 所有图片都处理失败，降级为LLM
                    llm_posts.append(post)
                    if debug_mode:
                        image_stats['posts_downgraded_to_llm'] += 1
                        logger.debug(f"帖子{post['id']}的所有图片都处理失败，降级为LLM处理")

            vlm_posts = vlm_posts_final

            if debug_mode:
                processed_success = sum(1 for v in image_processing_results.values() if v.get('success', False))
                image_stats['processed_success'] = processed_success

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

    def _process_vlm_post(self, post: Dict[str, Any], validated_image_data: List[Dict[str, Any]] = None) -> bool:
        """
        处理需要VLM的帖子，使用预处理的图片数据

        Args:
            post: 帖子信息字典
            validated_image_data: 预处理的图片数据列表

        Returns:
            是否处理成功
        """
        post_id = post['id']
        post_text = post.get('summary', '') or post.get('title', '')

        if not post_text:
            self.logger.warning(f"VLM帖子{post_id}没有内容，跳过处理")
            return False

        # 使用预处理的图片数据
        if validated_image_data:
            image_data = validated_image_data
        else:
            # 如果没有预处理的数据，降级为纯文本处理
            self.logger.info(f"帖子{post_id}没有有效图片数据，降级为LLM处理")
            return self._process_llm_post(post)

        try:
            self.logger.debug(f"VLM处理帖子{post_id}，包含{len(image_data)}张有效图片")
            interpretation = self._call_vlm_for_post(post_text, image_data)

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

    def _process_vlm_post_with_stats(self, post: Dict[str, Any], validated_image_data: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        处理需要VLM的帖子，返回详细统计信息（Debug模式）

        Args:
            post: 帖子信息字典
            validated_image_data: 预处理的图片数据列表

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

        # 使用预处理的图片数据
        if validated_image_data:
            image_data = validated_image_data
        else:
            # 如果没有预处理的数据，降级为纯文本处理
            self.logger.info(f"帖子{post_id}没有有效图片数据，降级为LLM处理")
            success = self._process_llm_post(post)
            result['success'] = success
            result['downgraded'] = success
            return result

        try:
            self.logger.debug(f"VLM处理帖子{post_id}，包含{len(image_data)}张有效图片")
            interpretation = self._call_vlm_for_post(post_text, image_data)

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

    def _call_vlm_for_post(self, post_text: str, image_data_list: List[Dict[str, Any]]) -> Optional[str]:
        """
        调用视觉多模态模型处理带图片的Post，支持混合模式

        Args:
            post_text: Post文本内容
            image_data_list: 图片数据列表，支持URL和base64混合格式

        Returns:
            LLM解读结果，失败时返回None
        """
        if not image_data_list:
            self.logger.warning("没有有效的图片数据，无法调用VLM")
            return None

        prompt = get_vlm_prompt(post_text)

        try:
            png_count = sum(1 for img in image_data_list if img.get('type') == 'url')
            base64_count = sum(1 for img in image_data_list if img.get('type') == 'base64')
            self.logger.info(f"准备调用VLM处理{len(image_data_list)}张图片 (PNG-URL: {png_count}张, 转换base64: {base64_count}张)")

            # 调用VLM API（已包含重试机制）
            result = self.llm_client.call_vlm(prompt, image_data_list)

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