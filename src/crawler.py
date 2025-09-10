from __future__ import annotations

"""
Jike crawler - reads user profiles from database, fetches posts via RSSHub, stores into MySQL.
Tables are created automatically by DatabaseManager.
Includes data cleaning: HTML to Markdown conversion and URL normalization.
"""

import logging
import time
import random
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import feedparser
import requests
from markdownify import markdownify as md

# Support running both as a package and as a script
try:
    from .database import DatabaseManager
    from .config import config
except ImportError:  # pragma: no cover - fallback for direct script execution
    import sys
    import os
    sys.path.append(os.path.dirname(__file__))
    from database import DatabaseManager  # type: ignore
    from config import config  # type: ignore

logger = logging.getLogger(__name__)


# 从配置获取参数
MAX_WORKERS = config.get_max_workers()
REQUEST_TIMEOUT = config.get_crawler_config()['request_timeout']
RETRIES_PER_USER = config.get_crawler_config()['max_retries']
RSSHUB_HOSTS = config.get_rsshub_hosts()


# === 数据清洗功能 ===

def normalize_url(url: str) -> str:
    """
    规范化URL，保留文件扩展名之前的部分
    例如: https://cdnv2.ruguoapp.com/FsB3qQPW71BVSeeaMxQoMBbWvejov3.png?imageMogr2/auto-orient/thumbnail/2562228@
    输出: https://cdnv2.ruguoapp.com/FsB3qQPW71BVSeeaMxQoMBbWvejov3.png
    """
    if not url:
        return url
    
    # 解析URL
    parsed = urlparse(url)
    
    # 重建不带参数的URL
    clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    
    return clean_url


def html_to_markdown(html_content: str) -> str:
    """
    使用markdownify将HTML内容转换为Markdown格式
    相比正则表达式，更加高效和完整
    """
    if not html_content:
        return ""
    
    # 首先规范化图片URL
    # 在转换为Markdown之前处理图片URL
    import re
    img_pattern = r'<img\s+src="([^"]+)"([^>]*)>'
    def replace_img(match):
        original_url = match.group(1)
        clean_url = normalize_url(original_url)
        return f'<img src="{clean_url}"{match.group(2)}>'
    
    html_content = re.sub(img_pattern, replace_img, html_content)
    
    # 使用markdownify转换，只指定要转换的标签，不指定要去除的
    markdown_content = md(
        html_content,
        # 只指定要转换的标签
        convert=['p', 'br', 'strong', 'b', 'em', 'i', 'a', 'img', 'div', 'span', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'],
        # 不指定 strip 参数
        # 换行处理
        newline_char='\n'
    )
    
    # 清理多余的空行
    markdown_content = re.sub(r'\n\s*\n\s*\n+', '\n\n', markdown_content)
    markdown_content = markdown_content.strip()
    
    return markdown_content


def clean_post_data(post_data: dict) -> dict:
    """
    清洗单条帖子数据
    
    Args:
        post_data: 包含title, link, summary等字段的字典
        
    Returns:
        清洗后的数据字典
    """
    cleaned_data = post_data.copy()
    
    # 清洗summary字段
    if 'summary' in cleaned_data and cleaned_data['summary']:
        cleaned_data['summary'] = html_to_markdown(cleaned_data['summary'])
    
    return cleaned_data


def get_user_id_from_url(url: str) -> Optional[str]:
    try:
        path = urlparse(url).path
        parts = path.strip('/').split('/')
        if len(parts) > 1 and (parts[0] == 'u' or parts[0] == 'users'):
            return parts[1]
        elif len(parts) == 1 and parts[0]:
            return parts[0]
    except Exception:
        return None
    return None


def to_datetime(parsed_time) -> Optional[datetime]:
    """将RSS解析的时间转换为北京时间（UTC+8）"""
    if not parsed_time:
        return None
    try:
        # RSS源通常提供UTC时间，先转换为UTC datetime对象
        utc_dt = datetime.fromtimestamp(time.mktime(parsed_time), tz=timezone.utc)
        # 转换为北京时间（UTC+8）
        beijing_dt = utc_dt + timedelta(hours=8)
        # 返回不带时区信息的本地时间，用于存储到DATETIME字段
        return beijing_dt.replace(tzinfo=None)
    except Exception:
        return None


def get_profiles_from_database(db_manager: DatabaseManager) -> List[Dict[str, Any]]:
    """从数据库获取用户档案"""
    profiles = db_manager.get_all_profiles()
    logger.info(f"从数据库获取到 {len(profiles)} 个用户档案")
    return profiles


def fetch_user_posts(user_id: str, nickname: str = '', max_retries: int = RETRIES_PER_USER) -> List[Dict[str, Optional[str]]]:
    shuffled_hosts = random.sample(RSSHUB_HOSTS, len(RSSHUB_HOSTS))
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    for i, host in enumerate(shuffled_hosts[:max_retries]):
        feed_url = f"{host}/jike/user/{user_id}"
        try:
            resp = requests.get(feed_url, timeout=REQUEST_TIMEOUT, headers=headers)
            resp.raise_for_status()
            feed = feedparser.parse(resp.content)
            posts = []
            for entry in feed.entries:
                posts.append({
                    'title': entry.get('title', ''),
                    'link': entry.get('link', ''),
                    'summary': entry.get('summary', ''),
                    'published_at': to_datetime(entry.get('published_parsed')),
                })
            return posts
        except requests.exceptions.RequestException:
            time.sleep(1)
            continue
        except Exception:
            continue
    return []


def run() -> Dict[str, Any]:
    """执行爬取任务"""
    start = time.time()
    logger.info("开始即刻用户动态爬取任务")
    
    # 使用已初始化的数据库管理器（不重复初始化）
    db_manager = DatabaseManager(config, auto_init=False)
    # 不再调用 db_manager.init_database()，因为tasks已经初始化过了
    
    # 从数据库获取用户档案
    profiles = get_profiles_from_database(db_manager)
    if not profiles:
        logger.warning("数据库中没有用户档案，请先使用 import_profiles.py 导入用户数据")
        return {
            'success': False,
            'error': '数据库中没有用户档案',
            'profiles_count': 0,
            'posts_inserted': 0,
            'elapsed_seconds': 0
        }
    
    # 获取用户ID映射
    user_ids = [p['jike_user_id'] for p in profiles]
    uid_to_profile = db_manager.get_profile_id_map(user_ids)
    
    # 准备爫取任务
    tasks: List[Tuple[int, str, str, int]] = []  # (idx, user_id, nickname, profile_id)
    for idx, p in enumerate(profiles, start=1):
        uid = p['jike_user_id']
        profile_id = uid_to_profile.get(uid)
        if not profile_id:
            logger.warning(f"用户 {uid} 在数据库中找不到对应的profile_id")
            continue
        tasks.append((idx, uid, p.get('nickname') or '', profile_id))
    
    total_new = 0
    batch: List[Dict[str, Any]] = []
    batch_size = 50  # 减小批量大小，减少内存占用和数据库负载
    
    def flush_batch():
        nonlocal batch, total_new
        if not batch:
            return
        try:
            inserted = db_manager.insert_posts_batch(batch)
            total_new += inserted
            if inserted > 0:
                logger.info(f"入库: {inserted} 条新动态")
        except Exception as e:
            logger.error(f"批量入库失败: {e}")
        finally:
            batch = []
    
    logger.info(f"开始爬取 {len(tasks)} 个用户的动态，使用 {MAX_WORKERS} 个并发线程")
    
    processed_count = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_user_posts, uid, nickname, RETRIES_PER_USER): (idx, uid, nickname, profile_id)
                   for idx, uid, nickname, profile_id in tasks}
        
        for future in as_completed(futures):
            idx, uid, nickname, profile_id = futures[future]
            processed_count += 1
            
            try:
                posts = future.result()
                # 简化日志，直接显示用户信息和进度
                logger.info(f"[{processed_count}/{len(tasks)}] {nickname or uid}: 获取到 {len(posts)} 条动态")
            except Exception as e:
                logger.error(f"[{processed_count}/{len(tasks)}] {nickname or uid}: 爬取失败 - {e}")
                posts = []
            
            if posts:
                for p in posts:
                    link = p.get('link') or ''
                    if not link:
                        continue
                    
                    # 清洗数据（HTML转Markdown、URL规范化）
                    try:
                        cleaned_post = clean_post_data(p)
                    except Exception as e:
                        logger.warning(f"数据清洗失败: {e}，使用原始数据")
                        cleaned_post = p
                    
                    # 截断过长的summary（TEXT类型最大65535字符）
                    summary = cleaned_post.get('summary') or ''
                    if len(summary) > 30000:  # 保守一些，限制在30k字符
                        summary = summary[:30000] + '...'
                    
                    batch.append({
                        'profile_id': profile_id,
                        'link': link,
                        'title': cleaned_post.get('title') or '',
                        'summary': summary,
                        'published_at': cleaned_post.get('published_at'),
                    })
                    
                    # 更频繁的批量处理，减少内存累积
                    if len(batch) >= batch_size:
                        flush_batch()
            
            # 每处理10个用户就刷新一次批量，避免数据积压
            if processed_count % 10 == 0:
                flush_batch()
    
    # 处理最后一批
    flush_batch()
    
    elapsed = time.time() - start
    result = {
        'success': True,
        'profiles_count': len(profiles),
        'posts_inserted': total_new,
        'elapsed_seconds': round(elapsed, 2),
    }
    
    logger.info(f"爬取任务完成: 处理 {result['profiles_count']} 个用户，新增 {result['posts_inserted']} 条动态，耗时 {result['elapsed_seconds']} 秒")
    return result


if __name__ == '__main__':
    # 设置日志
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    stats = run()
    if stats['success']:
        print(f"完成：处理 {stats['profiles_count']} 个用户，新增 {stats['posts_inserted']} 条动态，用时 {stats['elapsed_seconds']} 秒")
    else:
        print(f"失败：{stats.get('error', '未知错误')}")

