"""
配置管理模块
支持环境变量 > config.ini > 默认值的优先级机制
"""
import os
import configparser
from typing import Dict, Any, List
from dotenv import load_dotenv

class Config:
    """配置管理类，支持环境变量优先级的配置加载"""
    
    def __init__(self, config_path: str = 'config.ini'):
        """初始化配置"""
        # 在本地开发环境中，可以加载.env文件
        load_dotenv()
        
        # 读取config.ini文件
        self.config_parser = configparser.ConfigParser()
        
        # 尝试多个可能的配置文件路径
        possible_paths = [
            config_path,  # 原始路径
            os.path.join(os.getcwd(), config_path),  # 当前工作目录
            os.path.join(os.path.dirname(os.path.dirname(__file__)), config_path),  # 项目根目录
        ]
        
        self.config_file = None
        for path in possible_paths:
            if os.path.exists(path):
                self.config_file = path
                break
        
        # 如果config.ini文件存在，则读取
        if self.config_file:
            try:
                self.config_parser.read(self.config_file, encoding='utf-8')
            except (configparser.Error, UnicodeDecodeError) as e:
                pass
    
    def _get_config_value(self, section: str, key: str, env_var: str, default_value: Any, value_type=str) -> Any:
        """
        按优先级获取配置值：环境变量 > config.ini > 默认值
        
        Args:
            section: config.ini中的section名称
            key: config.ini中的key名称
            env_var: 环境变量名称
            default_value: 默认值
            value_type: 值类型转换函数
            
        Returns:
            配置值
        """
        # 1. 优先检查环境变量
        env_value = os.getenv(env_var)
        if env_value is not None:
            try:
                return value_type(env_value)
            except (ValueError, TypeError):
                return default_value
        
        # 2. 检查config.ini文件
        try:
            if self.config_parser.has_section(section) and self.config_parser.has_option(section, key):
                config_value = self.config_parser.get(section, key)
                try:
                    return value_type(config_value)
                except (ValueError, TypeError):
                    return default_value
        except (configparser.Error, UnicodeDecodeError):
            pass
        
        # 3. 返回默认值
        return default_value
    
    def get_database_config(self) -> Dict[str, Any]:
        """获取数据库配置，优先级：环境变量 > config.ini > 默认值"""
        config = {
            'host': self._get_config_value('database', 'host', 'DB_HOST', None),
            'user': self._get_config_value('database', 'user', 'DB_USER', None),
            'password': self._get_config_value('database', 'password', 'DB_PASSWORD', None),
            'database': self._get_config_value('database', 'database', 'DB_NAME', None),
            'port': self._get_config_value('database', 'port', 'DB_PORT', 3306, int),
            'charset': 'utf8mb4',
            'connect_timeout': 30,
            'read_timeout': 120,
            'write_timeout': 60
        }
        
        # 检查SSL模式
        ssl_mode = self._get_config_value('database', 'ssl_mode', 'DB_SSL_MODE', 'disabled')
        if ssl_mode.upper() == 'REQUIRED':
            config['ssl'] = {'mode': 'REQUIRED'}
        
        # 验证必需的数据库配置
        required_fields = ['host', 'user', 'password', 'database']
        missing_fields = [field for field in required_fields if config[field] is None]
        if missing_fields:
            raise ValueError(f"数据库核心配置缺失: {', '.join(missing_fields)}。请在环境变量或config.ini中设置。")
        
        return config
    
    def get_crawler_config(self) -> Dict[str, Any]:
        """获取爬虫配置，优先级：环境变量 > config.ini > 默认值"""
        return {
            'request_timeout': self._get_config_value('crawler', 'request_timeout', 'CRAWLER_REQUEST_TIMEOUT', 15, int),
            'max_retries': self._get_config_value('crawler', 'max_retries', 'CRAWLER_MAX_RETRIES', 6, int),
            'delay_seconds': self._get_config_value('crawler', 'delay_seconds', 'CRAWLER_DELAY_SECONDS', 1.0, float),
            'max_concurrent_requests': self._get_config_value('crawler', 'max_concurrent_requests', 'CRAWLER_MAX_CONCURRENT_REQUESTS', 10, int)
        }
    
    def get_rsshub_hosts(self) -> List[str]:
        """获取RSSHub实例列表"""
        hosts_str = self._get_config_value('rsshub', 'hosts', 'RSSHUB_HOSTS', 
                                          'https://rsshub.rssforever.com,https://rss.injahow.cn')
        return [host.strip() for host in hosts_str.split(',') if host.strip()]
    
    def get_data_retention_days(self) -> int:
        """获取数据保留天数"""
        return self._get_config_value('data_retention', 'days', 'DATA_RETENTION_DAYS', 180, int)
    
    def get_logging_config(self) -> Dict[str, str]:
        """获取日志配置"""
        return {
            'log_level': self._get_config_value('logging', 'log_level', 'LOGGING_LOG_LEVEL', 'INFO'),
            'log_file': self._get_config_value('logging', 'log_file', 'LOGGING_LOG_FILE', 'jike_crawler.log')
        }
    
    def get_executor_config(self) -> Dict[str, Any]:
        """获取执行器配置"""
        return {
            'max_workers': self._get_config_value('executor', 'max_workers', 'EXECUTOR_MAX_WORKERS', 10, int)
        }
    
    def get_max_workers(self) -> int:
        """获取并行工作线程数"""
        return self._get_config_value('executor', 'max_workers', 'EXECUTOR_MAX_WORKERS', 10, int)

    def get_llm_config(self) -> Dict[str, Any]:
        """获取LLM配置，优先级：环境变量 > config.ini > 默认值。
        与 info-collector-linuxdo 保持一致字段。
        """
        return {
            'openai_api_key': self._get_config_value('llm', 'openai_api_key', 'OPENAI_API_KEY', None),
            'openai_model': self._get_config_value('llm', 'openai_model', 'OPENAI_MODEL', 'gpt-3.5-turbo'),
            'openai_base_url': self._get_config_value('llm', 'openai_base_url', 'OPENAI_BASE_URL', 'https://api.openai.com/v1'),
            'max_content_length': self._get_config_value('llm', 'max_content_length', 'LLM_MAX_CONTENT_LENGTH', 380000, int)
        }

    def get_analysis_config(self) -> Dict[str, Any]:
        """获取分析任务配置（报告时间窗口、KOL名单等）"""
        kol_ids_raw = self._get_config_value('analysis', 'kol_user_ids', 'ANALYSIS_KOL_USER_IDS', '', str) or ''
        kol_ids = [i.strip() for i in kol_ids_raw.split(',') if i.strip()]
        return {
            'hours_back_daily': self._get_config_value('analysis', 'hours_back_daily', 'ANALYSIS_HOURS_BACK_DAILY', 24, int),
            'days_back_weekly': self._get_config_value('analysis', 'days_back_weekly', 'ANALYSIS_DAYS_BACK_WEEKLY', 7, int),
            'days_back_quarterly': self._get_config_value('analysis', 'days_back_quarterly', 'ANALYSIS_DAYS_BACK_QUARTERLY', 90, int),
            'days_back_kol': self._get_config_value('analysis', 'days_back_kol', 'ANALYSIS_DAYS_BACK_KOL', 30, int),
            'kol_user_ids': kol_ids
        }

    def get_notion_config(self) -> Dict[str, Any]:
        """获取Notion集成配置，优先级：环境变量 > config.ini > 默认值"""
        return {
            'integration_token': self._get_config_value('notion', 'integration_token', 'NOTION_INTEGRATION_TOKEN', None),
            'parent_page_id': self._get_config_value('notion', 'parent_page_id', 'NOTION_PARENT_PAGE_ID', None)
        }

# 全局配置实例
config = Config()
