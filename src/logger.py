"""
日志管理模块
"""
import logging
import os
from typing import Optional

def setup_logging(log_file: Optional[str] = None, log_level: str = 'INFO') -> None:
    """
    设置全局日志配置
    
    Args:
        log_file: 日志文件路径，如果为None则只输出到控制台
        log_level: 日志级别，如INFO、DEBUG、WARNING、ERROR
    """
    # 转换日志级别
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    # 创建格式器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 获取根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # 清除现有的处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # 文件处理器（如果指定了日志文件）
    if log_file:
        # 确保日志目录存在
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
            
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

def get_logger(name: str) -> logging.Logger:
    """获取指定名称的日志器"""
    return logging.getLogger(name)