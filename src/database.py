"""
MySQL database manager for Jike crawler.
Using config management system, supports database upgrades.
- jk_profiles: users from CSV
- jk_posts: posts linked to profiles
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pymysql

logger = logging.getLogger(__name__)





class DatabaseManager:
    """数据库管理器，支持配置管理系统和自动升级"""

    def __init__(self, config=None, auto_init=True):
        """初始化数据库管理器
        
        Args:
            config: 配置对象，如果为None则从当前目录加载
            auto_init: 是否自动初始化数据库，默认True
        """
        if config is None:
            # 避免循环导入，在需要时导入
            try:
                from .config import config as default_config
                config = default_config
            except ImportError:
                from config import config as default_config
                config = default_config
        
        self.config = config
        self.db_config = config.get_database_config()
        
        # 根据参数决定是否初始化数据库
        if auto_init:
            self.init_database()

    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = pymysql.connect(**self.db_config)
            yield conn
        finally:
            if conn:
                conn.close()

    def init_database(self):
        """初始化数据库，直接创建表结构"""
        logger.info("初始化数据库...")
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # 直接创建表结构
                schemas = self._table_schemas()
                for table_name, schema_sql in schemas.items():
                    logger.info(f"创建表: {table_name}")
                    cur.execute(schema_sql)
                
                conn.commit()
        
        logger.info("数据库初始化完成")


    def _table_schemas(self) -> Dict[str, str]:
        return {
            'jk_profiles': (
                """
                CREATE TABLE IF NOT EXISTS jk_profiles (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    jike_user_id CHAR(36) CHARACTER SET ascii NOT NULL,
                    profile_url VARCHAR(512) CHARACTER SET ascii NOT NULL,
                    avatar_url VARCHAR(512) CHARACTER SET ascii DEFAULT NULL,
                    nickname VARCHAR(128) DEFAULT NULL,
                    bio VARCHAR(1024) DEFAULT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_user_id (jike_user_id),
                    UNIQUE KEY uniq_profile_url (profile_url),
                    INDEX idx_nickname (nickname)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                COMMENT='即刻用户清单'
                """
            ),
            'jk_posts': (
                """
                CREATE TABLE IF NOT EXISTS jk_posts (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    profile_id INT NOT NULL,
                    link VARCHAR(80) CHARACTER SET ascii NOT NULL COMMENT '动态链接',
                    title VARCHAR(160) DEFAULT NULL COMMENT '动态标题',
                    summary TEXT DEFAULT NULL COMMENT '内容摘要，Markdown格式',
                    published_at DATETIME DEFAULT NULL COMMENT '发布时间',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_link (link),
                    INDEX idx_profile_published (profile_id, published_at),
                    CONSTRAINT fk_post_profile FOREIGN KEY (profile_id)
                        REFERENCES jk_profiles(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                COMMENT='即刻用户动态'
                """
            ),
        }

    def upsert_profiles(self, profiles: Sequence[Dict[str, Any]]) -> int:
        if not profiles:
            return 0
        cols = ['jike_user_id', 'profile_url', 'avatar_url', 'nickname', 'bio']
        placeholders = ','.join(['%s'] * len(cols))
        sql = f"""
            INSERT INTO jk_profiles ({', '.join(cols)})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE
                avatar_url = VALUES(avatar_url),
                nickname   = VALUES(nickname),
                bio        = VALUES(bio)
        """
        values = [tuple(p.get(c) for c in cols) for p in profiles]
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, values)
                conn.commit()
                return cur.rowcount

    def get_profile_id_map(self, jike_user_ids: Iterable[str]) -> Dict[str, int]:
        ids = list({i for i in jike_user_ids if i})
        if not ids:
            return {}
        result: Dict[str, int] = {}
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                for i in range(0, len(ids), 500):
                    chunk = ids[i:i+500]
                    placeholders = ','.join(['%s'] * len(chunk))
                    cur.execute(
                        f"SELECT jike_user_id, id FROM jk_profiles WHERE jike_user_id IN ({placeholders})",
                        tuple(chunk),
                    )
                    for uid, pid in cur.fetchall():
                        result[uid] = pid
        return result

    def insert_posts_batch(self, posts: Sequence[Dict[str, Any]]) -> int:
        """批量插入动态数据，使用IGNORE忽略重复数据，提高性能"""
        if not posts:
            return 0
        
        cols = ['profile_id', 'link', 'title', 'summary', 'published_at']
        placeholders = ','.join(['%s'] * len(cols))
        
        # 使用INSERT IGNORE忽略重复记录，比ON DUPLICATE KEY UPDATE更高效
        sql = f"""
            INSERT IGNORE INTO jk_posts ({', '.join(cols)})
            VALUES ({placeholders})
        """
        
        values = []
        for p in posts:
            # 确保summary不超过TEXT类型限制（65535字节）
            summary = p.get('summary') or ''
            if isinstance(summary, str) and len(summary.encode('utf8')) > 60000:  # 保守估计
                summary = summary[:20000] + '...'
            
            values.append(tuple([
                p.get('profile_id'),
                p.get('link'),
                p.get('title'),
                summary,
                p.get('published_at')
            ]))
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, values)
                conn.commit()
                return cur.rowcount
    
    def get_all_profiles(self) -> List[Dict[str, Any]]:
        """获取所有用户档案"""
        with self.get_connection() as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute("""
                    SELECT id, jike_user_id, profile_url, avatar_url, nickname, bio, 
                           created_at, updated_at
                    FROM jk_profiles 
                    ORDER BY created_at
                """)
                return cur.fetchall()
    
    def get_profile_stats(self) -> Dict[str, int]:
        """获取数据库统计信息"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # 获取用户总数
                cur.execute("SELECT COUNT(*) FROM jk_profiles")
                profile_count = cur.fetchone()[0]
                
                # 获取帖子总数
                cur.execute("SELECT COUNT(*) FROM jk_posts")
                post_count = cur.fetchone()[0]
                
                # 获取今日新增帖子数
                cur.execute("""
                    SELECT COUNT(*) FROM jk_posts 
                    WHERE DATE(created_at) = CURDATE()
                """)
                today_posts = cur.fetchone()[0]
                
                return {
                    'total_profiles': profile_count,
                    'total_posts': post_count,
                    'today_posts': today_posts
                }
    
    def cleanup_old_posts(self, retention_days: int) -> int:
        """清理旧帖子"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM jk_posts 
                    WHERE created_at < DATE_SUB(NOW(), INTERVAL %s DAY)
                """, (retention_days,))
                conn.commit()
                return cur.rowcount
    
    def get_posts_for_analysis(self, days: int = 7) -> List[Dict[str, Any]]:
        """获取指定天数内的帖子用于分析"""
        with self.get_connection() as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute("""
                    SELECT p.id, p.link, p.title, p.summary, p.published_at,
                           prof.nickname, prof.jike_user_id
                    FROM jk_posts p
                    JOIN jk_profiles prof ON p.profile_id = prof.id
                    WHERE p.created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                    ORDER BY p.created_at DESC
                    LIMIT 1000
                """, (days,))
                return cur.fetchall()

