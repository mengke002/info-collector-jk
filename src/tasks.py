"""
Task wrapper to run Jike collection and management tasks.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Dict, List, Optional

try:
    from .crawler import run as run_crawler
    from .database import DatabaseManager
    from .config import config
    from .post_processor import run_post_processing
except ImportError:
    # Fallback for direct script execution without package context
    from crawler import run as run_crawler  # type: ignore
    from database import DatabaseManager  # type: ignore
    from config import config  # type: ignore
    from post_processor import run_post_processing  # type: ignore

# 按需导入报告生成器，避免无关任务触发其模块编译
def _lazy_get_report_generator():
    try:
        from .report_generator import get_report_generator as _gr
    except ImportError:  # pragma: no cover
        from report_generator import get_report_generator as _gr  # type: ignore
    return _gr()

logger = logging.getLogger(__name__)


def _resolve_async_result(value: Any) -> Any:
    """Return awaitable results in a synchronous context."""
    if inspect.isawaitable(value):
        try:
            return asyncio.run(value)
        except RuntimeError as exc:
            if 'asyncio.run() cannot be called from a running event loop' not in str(exc):
                raise
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(value)
            finally:
                loop.close()
    return value


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

        # 添加后处理统计信息
        postprocessing_stats = db_manager.get_postprocessing_stats()
        stats.update(postprocessing_stats)

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


def run_postprocess_task(hours_back: int = None) -> Dict[str, Any]:
    """执行Post后处理任务"""
    logger.info("开始执行Post后处理任务")
    try:
        # 确保数据库表存在
        _ = DatabaseManager(config)

        if hours_back is None:
            hours_back = 36  # 默认回溯36小时

        result = run_post_processing(hours_back)

        return {
            'success': True,
            'total_posts': result['total'],
            'processed_successfully': result['success'],
            'failed_posts': result['failed'],
            'hours_back': hours_back
        }

    except Exception as e:
        logger.error(f"Post后处理任务失败: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }


def run_report_task(report_type: str,
                    hours_back: Optional[int] = None,
                    days_back: Optional[int] = None,
                    kol_user_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """执行报告生成任务
    report_type: daily_hotspot | weekly_digest | kol_trajectory | quarterly_narrative
    """
    logger.info(f"开始执行报告任务: {report_type}")
    try:
        # 初始化数据库（确保表存在）
        _ = DatabaseManager(config)
        # 延迟导入，避免在非报告类任务执行时编译 report_generator
        rg = _lazy_get_report_generator()
        if report_type == 'daily_hotspot':
            return _resolve_async_result(
                rg.generate_daily_hotspot(hours_back=hours_back)
            )
        elif report_type == 'weekly_digest':
            return _resolve_async_result(
                rg.generate_weekly_digest(days_back=days_back)
            )
        elif report_type == 'kol_trajectory':
            return _resolve_async_result(
                rg.generate_kol_trajectory(kol_ids=kol_user_ids, days_back=days_back)
            )
        elif report_type == 'quarterly_narrative':
            return _resolve_async_result(
                rg.generate_quarterly_narrative(days_back=days_back)
            )
        else:
            return {'success': False, 'error': f'未知报告类型: {report_type}'}
    except Exception as e:
        logger.error(f"报告任务失败: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}


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

    print("\n=== Post后处理任务测试 ===")
    postprocess_result = run_postprocess_task()
    print(f"后处理结果: {postprocess_result}")
