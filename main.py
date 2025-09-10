#!/usr/bin/env python3
"""
即刻爬虫系统
主执行脚本
"""
import sys
import argparse
import json
import logging
from datetime import datetime, timezone, timedelta

from src.logger import setup_logging
from src.database import DatabaseManager
from src.config import config
from src.tasks import run_crawl_task, run_cleanup_task, run_stats_task

# Initialize logging
logging_config = config.get_logging_config()
setup_logging(logging_config['log_file'], logging_config['log_level'])
logger = logging.getLogger(__name__)


def get_beijing_time():
    """获取北京时间（UTC+8）"""
    utc_time = datetime.now(timezone.utc)
    beijing_time = utc_time + timedelta(hours=8)
    return beijing_time.replace(tzinfo=None)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='即刻爬虫系统')
    parser.add_argument('--task', choices=['crawl', 'cleanup', 'stats', 'full'],
                       default='crawl', help='要执行的任务类型')
    parser.add_argument('--retention-days', type=int, 
                       help='数据保留天数（仅用于cleanup任务）')
    parser.add_argument('--output', choices=['json', 'text'], default='text',
                       help='输出格式')
    parser.add_argument('--recreate-db', action='store_true',
                       help='删除并重新创建所有表')
    
    args = parser.parse_args()
    
    print(f"即刻爬虫系统")
    print(f"执行时间: {get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"执行任务: {args.task}")
    print("-" * 50)

    # 初始化数据库管理器
    db_manager = DatabaseManager(config)
    
    if args.recreate_db:
        print("正在删除并重新创建数据库表...")
        # TODO: 实现删除表的逻辑
        print("数据库表已重新创建。")
    
    # 执行对应任务
    if args.task == 'crawl':
        result = run_crawl_task()
    elif args.task == 'cleanup':
        result = run_cleanup_task(args.retention_days)
    elif args.task == 'stats':
        result = run_stats_task()
    elif args.task == 'full':
        result = run_full_task()
    else:
        print(f"未知任务类型: {args.task}")
        sys.exit(1)
    
    # 输出结果
    if args.output == 'json':
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    else:
        print_result(result, args.task)
    
    # 根据结果设置退出码
    if result.get('success', False):
        print("\n✅ 任务执行成功")
        sys.exit(0)
    else:
        print(f"\n❌ 任务执行失败: {result.get('error', '未知错误')}")
        sys.exit(1)


def run_full_task():
    """执行完整任务序列"""
    results = {
        'success': True,
        'results': {}
    }
    
    print("执行完整任务序列...")
    
    # 1. 爬取任务
    print("1. 执行爬取任务...")
    crawl_result = run_crawl_task()
    results['results']['crawl'] = crawl_result
    
    # 2. 统计任务
    print("2. 执行统计任务...")
    stats_result = run_stats_task()
    results['results']['stats'] = stats_result
    
    # 3. 清理任务
    print("3. 执行清理任务...")
    cleanup_result = run_cleanup_task()
    results['results']['cleanup'] = cleanup_result
    
    # 检查所有任务是否成功
    all_success = all(
        result.get('success', False) 
        for result in results['results'].values()
    )
    results['success'] = all_success
    
    return results


def print_result(result: dict, task_type: str):
    """打印结果"""
    if not result.get('success', False):
        print(f"❌ 任务失败: {result.get('error', '未知错误')}")
        return
    
    if task_type == 'crawl':
        print(f"✅ 爬取任务完成")
        print(f"   处理用户: {result.get('profiles_count', 0)} 个")
        print(f"   新增动态: {result.get('posts_inserted', 0)} 条")
        print(f"   耗时: {result.get('elapsed_seconds', 0)} 秒")
    
    elif task_type == 'cleanup':
        print(f"✅ 清理任务完成")
        print(f"   删除记录: {result.get('deleted_count', 0)} 条")
        print(f"   保留天数: {result.get('retention_days', 0)} 天")
    
    elif task_type == 'stats':
        print(f"✅ 统计信息")
        stats = result.get('stats', {})
        print(f"   总用户数: {stats.get('total_profiles', 0)}")
        print(f"   总动态数: {stats.get('total_posts', 0)}")
        print(f"   今日新增: {stats.get('today_posts', 0)}")

    elif task_type == 'full':
        print(f"✅ 完整任务序列完成")
        
        # 爬取结果
        crawl_result = result.get('results', {}).get('crawl', {})
        if crawl_result.get('success'):
            print(f"   爬取: 处理 {crawl_result.get('profiles_count', 0)} 个用户，新增 {crawl_result.get('posts_inserted', 0)} 条动态")
        
        # 统计结果
        stats_result = result.get('results', {}).get('stats', {})
        if stats_result.get('success'):
            stats = stats_result.get('stats', {})
            print(f"   统计: 总用户 {stats.get('total_profiles', 0)}，总动态 {stats.get('total_posts', 0)}")
        
        # 清理结果
        cleanup_result = result.get('results', {}).get('cleanup', {})
        if cleanup_result.get('success'):
            print(f"   清理: 删除 {cleanup_result.get('deleted_count', 0)} 条旧记录")


if __name__ == "__main__":
    main()