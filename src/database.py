"""
MySQL database manager for Jike crawler.
Using config management system, supports database upgrades.
- jk_profiles: users from CSV
- jk_posts: posts linked to profiles
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime
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
            'jk_reports': (
                """
                CREATE TABLE IF NOT EXISTS jk_reports (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    report_type ENUM('daily_hotspot','daily_light','daily_deep','weekly_digest','kol_trajectory','quarterly_narrative') NOT NULL,
                    scope VARCHAR(128) DEFAULT NULL COMMENT '如kol为用户ID或昵称，其他为global',
                    analysis_period_start DATETIME NOT NULL,
                    analysis_period_end DATETIME NOT NULL,
                    items_analyzed INT UNSIGNED DEFAULT 0,
                    report_title VARCHAR(200) NOT NULL,
                    report_content MEDIUMTEXT NOT NULL,
                    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_type_time (report_type, generated_at),
                    INDEX idx_period (analysis_period_start, analysis_period_end),
                    INDEX idx_scope (scope)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                COMMENT='即刻分析报告'
                """
            ),
            'postprocessing': (
                """
                CREATE TABLE IF NOT EXISTS postprocessing (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    post_id INT NOT NULL COMMENT '关联jk_posts表的主键',
                    interpretation_text TEXT NOT NULL COMMENT 'LLM生成的完整解读内容',
                    model_name VARCHAR(255) NOT NULL COMMENT '使用的模型名称',
                    status ENUM('success', 'failed') NOT NULL DEFAULT 'success' COMMENT '处理状态',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                    UNIQUE KEY uniq_post_id (post_id),
                    INDEX idx_status (status),
                    INDEX idx_created_at (created_at),
                    CONSTRAINT fk_postprocessing_post FOREIGN KEY (post_id)
                        REFERENCES jk_posts(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                COMMENT='Post后处理解读结果表'
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

    def get_recent_posts(self, hours_back: int = 24, limit: int = 1500) -> List[Dict[str, Any]]:
        """获取最近hours_back小时内新增的帖子，包含解读信息"""
        with self.get_connection() as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT p.id, p.link, p.title, p.summary, p.published_at,
                           prof.nickname, prof.jike_user_id,
                           pp.interpretation_text, pp.model_name as interpretation_model,
                           pp.status as interpretation_status
                    FROM jk_posts p
                    JOIN jk_profiles prof ON p.profile_id = prof.id
                    LEFT JOIN postprocessing pp ON p.id = pp.post_id AND pp.status = 'success'
                    WHERE p.created_at >= DATE_SUB(NOW(), INTERVAL %s HOUR)
                    ORDER BY p.created_at DESC
                    LIMIT {limit}
                    """,
                    (hours_back,)
                )
                return cur.fetchall()

    def get_user_posts_for_analysis(self, jike_user_id: str, days: int = 30, limit: int = 2000) -> List[Dict[str, Any]]:
        """获取指定用户在指定天数内的帖子用于分析，包含解读信息"""
        with self.get_connection() as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT p.id, p.link, p.title, p.summary, p.published_at,
                           prof.nickname, prof.jike_user_id,
                           pp.interpretation_text, pp.model_name as interpretation_model,
                           pp.status as interpretation_status
                    FROM jk_posts p
                    JOIN jk_profiles prof ON p.profile_id = prof.id
                    LEFT JOIN postprocessing pp ON p.id = pp.post_id AND pp.status = 'success'
                    WHERE prof.jike_user_id = %s
                      AND p.created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                    ORDER BY p.created_at DESC
                    LIMIT {limit}
                    """,
                    (jike_user_id, days)
                )
                return cur.fetchall()

    def save_report(self, report_data: Dict[str, Any]) -> int:
        """保存分析报告到jk_reports表"""
        sql = """
        INSERT INTO jk_reports (
            report_type, scope, analysis_period_start, analysis_period_end,
            items_analyzed, report_title, report_content
        ) VALUES (
            %(report_type)s, %(scope)s, %(analysis_period_start)s, %(analysis_period_end)s,
            %(items_analyzed)s, %(report_title)s, %(report_content)s
        )
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, report_data)
                conn.commit()
                return cur.lastrowid

    def get_unprocessed_posts(self, hours_back: int = 36) -> List[Dict[str, Any]]:
        """获取未进行后处理的帖子，回溯指定小时数"""
        with self.get_connection() as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute("""
                    SELECT p.id, p.link, p.title, p.summary, p.published_at,
                           prof.nickname, prof.jike_user_id
                    FROM jk_posts p
                    LEFT JOIN postprocessing pp ON p.id = pp.post_id
                    JOIN jk_profiles prof ON p.profile_id = prof.id
                    WHERE pp.post_id IS NULL
                      AND p.created_at >= DATE_SUB(NOW(), INTERVAL %s HOUR)
                    ORDER BY p.created_at DESC
                    LIMIT 1000
                """, (hours_back,))
                return cur.fetchall()

    def save_post_interpretation(self, post_id: int, interpretation_text: str, model_name: str, status: str = 'success') -> int:
        """保存Post解读结果到postprocessing表"""
        sql = """
        INSERT INTO postprocessing (post_id, interpretation_text, model_name, status)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            interpretation_text = VALUES(interpretation_text),
            model_name = VALUES(model_name),
            status = VALUES(status),
            created_at = CURRENT_TIMESTAMP
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (post_id, interpretation_text, model_name, status))
                conn.commit()
                return cur.lastrowid or cur.rowcount

    def get_posts_with_interpretations(self, days: int = 7, limit: int = 1000) -> List[Dict[str, Any]]:
        """获取包含解读信息的帖子，用于生成最终报告，兼容没有解读内容的情况"""
        with self.get_connection() as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute("""
                    SELECT p.id, p.link, p.title, p.summary, p.published_at,
                           prof.nickname, prof.jike_user_id,
                           pp.interpretation_text, pp.model_name as interpretation_model,
                           pp.status as interpretation_status
                    FROM jk_posts p
                    JOIN jk_profiles prof ON p.profile_id = prof.id
                    LEFT JOIN postprocessing pp ON p.id = pp.post_id AND pp.status = 'success'
                    WHERE p.created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                    ORDER BY p.created_at DESC
                    LIMIT %s
                """, (days, limit))
                return cur.fetchall()

    def get_posts_for_analysis(self, days: int = 7) -> List[Dict[str, Any]]:
        """获取指定天数内的帖子用于分析，兼容有无解读内容"""
        with self.get_connection() as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute("""
                    SELECT p.id, p.link, p.title, p.summary, p.published_at,
                           prof.nickname, prof.jike_user_id,
                           pp.interpretation_text, pp.model_name as interpretation_model,
                           pp.status as interpretation_status
                    FROM jk_posts p
                    JOIN jk_profiles prof ON p.profile_id = prof.id
                    LEFT JOIN postprocessing pp ON p.id = pp.post_id AND pp.status = 'success'
                    WHERE p.created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                    ORDER BY p.created_at DESC
                    LIMIT 1000
                """, (days,))
                return cur.fetchall()

    def get_recent_daily_reports(self, days: int = 7) -> List[Dict[str, Any]]:
        """获取最近若干天的每日热点报告（每个自然日最新一篇）"""
        if days <= 0:
            return []

        with self.get_connection() as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, report_title, report_content,
                           analysis_period_start, analysis_period_end,
                           items_analyzed, generated_at
                    FROM jk_reports
                    WHERE report_type = 'daily_hotspot'
                      AND analysis_period_end >= DATE_SUB(NOW(), INTERVAL %s DAY)
                    ORDER BY analysis_period_end DESC, generated_at DESC
                    """,
                    (days + 1,)
                )
                rows = cur.fetchall()

        if not rows:
            return []

        seen_dates = set()
        selected: List[Dict[str, Any]] = []

        for row in rows:
            analysis_end = row.get('analysis_period_end')
            if isinstance(analysis_end, datetime):
                day_key = analysis_end.date()
            else:
                # 兜底：如果字段是字符串，尝试解析
                try:
                    parsed = datetime.fromisoformat(str(analysis_end))
                    day_key = parsed.date()
                    row['analysis_period_end'] = parsed
                except Exception:
                    day_key = None
            if day_key is None:
                continue

            analysis_start = row.get('analysis_period_start')
            if analysis_start is not None and not isinstance(analysis_start, datetime):
                try:
                    row['analysis_period_start'] = datetime.fromisoformat(str(analysis_start))
                except Exception:
                    row['analysis_period_start'] = None

            if day_key in seen_dates:
                continue

            seen_dates.add(day_key)
            selected.append(row)

            if len(selected) >= days:
                break

        selected.reverse()
        return selected

    def get_postprocessing_stats(self) -> Dict[str, int]:
        """获取后处理统计信息"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # 总处理数
                cur.execute("SELECT COUNT(*) FROM postprocessing")
                total_processed = cur.fetchone()[0]

                # 成功处理数
                cur.execute("SELECT COUNT(*) FROM postprocessing WHERE status = 'success'")
                success_count = cur.fetchone()[0]

                # 失败处理数
                cur.execute("SELECT COUNT(*) FROM postprocessing WHERE status = 'failed'")
                failed_count = cur.fetchone()[0]

                # 今日处理数
                cur.execute("""
                    SELECT COUNT(*) FROM postprocessing
                    WHERE DATE(created_at) = CURDATE()
                """)
                today_processed = cur.fetchone()[0]

                return {
                    'total_processed': total_processed,
                    'success_count': success_count,
                    'failed_count': failed_count,
                    'today_processed': today_processed
                }
