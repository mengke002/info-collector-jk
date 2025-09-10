"""
Task wrapper to run Jike collection and management tasks.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

try:
    from .crawler import run as run_crawler
    from .database import DatabaseManager
    from .config import config
except ImportError:
    # Fallback for direct script execution without package context
    from crawler import run as run_crawler  # type: ignore
    from database import DatabaseManager  # type: ignore
    from config import config  # type: ignore

logger = logging.getLogger(__name__)


def run_crawl_task() -> Dict[str, Any]:
    """执行爬取任务"""
    logger.info("开始执行即刻用户动态爬取任务")
    try:
        # 根据记忆中的经验，每个任务方法都必须包含db_manager.init_database()调用
        db_manager = DatabaseManager(config)
        result = run_crawler()
        logger.info(f"爬取任务完成: {result}")
        return result
    except Exception as e:
        logger.error(f"爬取任务失败: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }


def run_cleanup_task(retention_days: int = None) -> Dict[str, Any]:
    """执行数据清理任务"""
    logger.info("开始执行数据清理任务")
    try:
        # 根据记忆中的经验，每个任务方法都必须包含db_manager.init_database()调用
        db_manager = DatabaseManager(config)
        if retention_days is None:
            retention_days = config.get_data_retention_days()
        
        deleted_count = db_manager.cleanup_old_posts(retention_days)
        result = {
            'success': True,
            'deleted_count': deleted_count,
            'retention_days': retention_days
        }
        logger.info(f"清理任务完成: 删除 {deleted_count} 条过期数据（保留 {retention_days} 天）")
        return result
    except Exception as e:
        logger.error(f"清理任务失败: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }


def run_stats_task() -> Dict[str, Any]:
    """执行统计任务"""
    logger.info("开始执行统计任务")
    try:
        # 根据记忆中的经验，每个任务方法都必须包含db_manager.init_database()调用
        db_manager = DatabaseManager(config)
        stats = db_manager.get_profile_stats()
        result = {
            'success': True,
            'stats': stats
        }
        logger.info(f"统计任务完成: {stats}")
        return result
    except Exception as e:
        logger.error(f"统计任务失败: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }


if __name__ == '__main__':
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    print("=== 即刻爬取任务测试 ===")
    crawl_result = run_crawl_task()
    print(f"爬取结果: {crawl_result}")
    
    print("\n=== 统计任务测试 ===")
    stats_result = run_stats_task()
    print(f"统计结果: {stats_result}")

