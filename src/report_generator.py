"""
即刻分析报告生成器
实现四类报告：
- daily_hotspot: 24小时热点追踪器
- weekly_digest: 周度社群洞察摘要
- kol_trajectory: 月度KOL思想轨迹图（可并发多用户）
- quarterly_narrative: 季度战略叙事分析

输出保存至 jk_reports 表,并在报告中附带来源清单。
"""
from __future__ import annotations

import logging
import asyncio
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone, timedelta

try:
    from .database import DatabaseManager
    from .config import config
    from .llm_client import llm_client
except ImportError:  # pragma: no cover
    from database import DatabaseManager  # type: ignore
    from config import config  # type: ignore
    from llm_client import llm_client  # type: ignore


class JKReportGenerator:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = DatabaseManager(config)
        self.llm_cfg = config.get_llm_config()
        self.analysis_cfg = config.get_analysis_config()
        self.max_content_length = int(self.llm_cfg.get('max_content_length', 380000))
        self.max_llm_concurrency = 3  # 与linuxdo保持一致,不从[llm]读取

        # 获取报告上下文模式配置（与post_processor的interpretation_mode独立）
        context_mode = (self.analysis_cfg.get('report_context_mode') if self.analysis_cfg else 'light') or 'light'
        if not isinstance(context_mode, str):
            context_mode = 'light'
        context_mode = context_mode.lower()
        if context_mode not in {'light', 'full'}:
            self.logger.warning(f"未知report_context_mode配置: {context_mode}, 回退到light模式")
            context_mode = 'light'
        self.context_mode = context_mode

        self.logger.info(f"报告生成器初始化完成，report_context_mode={self.context_mode}")

    def _log_task_start(self, task_type: str, **kwargs) -> None:
        """统一的任务开始日志记录"""
        details = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
        self.logger.info(f"开始执行 {task_type} 任务: {details}")

    def _log_task_complete(self, task_type: str, success_count: int, failure_count: int, **kwargs) -> None:
        """统一的任务完成日志记录"""
        status = "成功" if failure_count == 0 else f"部分成功"
        details = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
        self.logger.info(f"{task_type} 任务完成 ({status}): 成功 {success_count} 个，失败 {failure_count} 个。{details}")

    def _handle_task_exception(self, task_type: str, model_name: str, display_name: str, exception: Exception) -> Dict[str, Any]:
        """统一的任务异常处理"""
        error_msg = str(exception)
        self.logger.warning(f"{task_type} 任务异常 - 模型 {model_name} ({display_name}): {error_msg}")
        return {
            'model': model_name,
            'model_display': display_name,
            'success': False,
            'error': error_msg,
            'error_type': type(exception).__name__
        }

    def _create_error_response(self, error_msg: str, **additional_fields) -> Dict[str, Any]:
        """创建标准化的错误响应"""
        response = {
            'success': False,
            'error': error_msg,
            'items_analyzed': 0
        }
        response.update(additional_fields)
        return response

    def _bj_time(self) -> datetime:
        return datetime.now(timezone.utc) + timedelta(hours=8)

    def _get_report_models(self) -> List[str]:
        """获取用于生成报告的模型列表（优先模型 + 默认模型）"""
        if not llm_client:
            return []

        models: List[str] = []
        raw_models = getattr(llm_client, 'models', None) or []

        for model_name in raw_models:
            if model_name and model_name not in models:
                models.append(model_name)

        if models:
            return models

        base_model = getattr(llm_client, 'smart_model', None)
        priority_model = getattr(llm_client, 'priority_model', None)

        if base_model:
            models.append(base_model)
        if priority_model and priority_model not in models:
            models.insert(0, priority_model)

        return models

    def _get_model_display_name(self, model_name: str) -> str:
        """根据模型名称生成用于展示的友好名称"""
        if not model_name:
            return 'LLM'

        lower_name = model_name.lower()
        if 'gemini' in lower_name:
            return 'Gemini'
        if 'deepseek' in lower_name:
            return 'DeepSeek'
        if 'grok' in lower_name:
            return 'Grok'
        # GLM模型识别：通用提取版本号（如GLM-4.5、GLM-4.6、GLM-4v等）
        if 'glm' in lower_name:
            import re
            # 匹配 GLM-数字.数字 或 GLM-数字v 等格式
            match = re.search(r'glm[- ]?(\d+\.?\d*v?)', lower_name)
            if match:
                version = match.group(1)
                return f'GLM{version}'
            else:
                return 'GLM'
        if 'gpt' in lower_name:
            return 'GPT'
        if 'claude' in lower_name:
            return 'Claude'

        return model_name

    async def _generate_daily_report_for_model(
        self,
        *,
        model_name: str,
        display_name: str,
        posts: List[Dict[str, Any]],
        content_md: str,
        sources: List[Dict[str, Any]],
        prompt: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """在独立线程中生成指定模型的日报"""
        return await asyncio.to_thread(
            self._generate_daily_report_for_model_sync,
            model_name,
            display_name,
            posts,
            content_md,
            sources,
            prompt,
            start_time,
            end_time
        )

    def _generate_daily_report_for_model_sync(
        self,
        model_name: str,
        display_name: str,
        posts: List[Dict[str, Any]],
        content_md: str,
        sources: List[Dict[str, Any]],
        prompt: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """同步执行指定模型的日报生成和Notion推送"""

        self.logger.info(f"[{display_name}] 模型线程启动，开始生成日报")

        llm_analysis_result = self._analyze_with_llm(content_md, prompt, model_override=model_name)

        if not llm_analysis_result:
            error_msg = "LLM分析失败，未生成日报"
            self.logger.warning(f"[{display_name}] {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'model': model_name,
                'model_display': display_name
            }

        llm_output = llm_analysis_result.get('content', '')
        # 为LLM生成的报告添加标准头部信息
        beijing_time = self._bj_time()
        header_info = [
            f"# 📈 即刻24小时热点追踪器 - {display_name}",
            "",
            f"*报告生成时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}*  ",
            "",
            f"*数据范围: {start_time.strftime('%Y-%m-%d %H:%M:%S')} - {end_time.strftime('%Y-%m-%d %H:%M:%S')}*  ",
            "",
            f"*分析动态数: {len(posts)} 条*",
            "",
            "---",
            ""
        ]

        # 清理LLM输出中可能的格式问题
        cleaned_llm_output = self._clean_llm_output_for_notion(llm_output)

        sources_section = self._render_sources_section(sources)

        # 构建报告尾部
        footer_lines = ["", "---", ""]
        provider = llm_analysis_result.get('provider')
        model = llm_analysis_result.get('model')
        if provider:
            footer_lines.append(f"*分析引擎: {provider} ({model or 'unknown'})*")
        
        footer_lines.extend([
            "",
            f"📊 **统计摘要**: 本报告分析了 {len(posts)} 条动态",
            "",
            "*本报告由AI自动生成，仅供参考*"
        ])
        footer_section = "\n".join(footer_lines)

        report_content = "\n".join(header_info) + cleaned_llm_output + "\n\n" + sources_section + footer_section

        # 应用来源链接增强后处理
        report_content = self._enhance_source_links(report_content, sources)

        title = f"即刻24h热点观察 - {display_name} - {end_time.strftime('%Y-%m-%d %H:%M')}"
        report_row = {
            'report_type': 'daily_hotspot',
            'scope': 'global',
            'analysis_period_start': start_time,
            'analysis_period_end': end_time,
            'items_analyzed': len(posts),
            'report_title': title,
            'report_content': report_content,
        }
        report_id = self.db.save_report(report_row)

        model_report = {
            'model': model_name,
            'model_display': display_name,
            'success': True,
            'report_id': report_id,
            'report_title': title,
            'provider': llm_analysis_result.get('provider') if llm_analysis_result else None,
            'items_analyzed': len(posts)
        }

        # 尝试推送到Notion
        notion_push_info = None
        try:
            from .notion_client import jike_notion_client

            # 格式化Notion标题
            beijing_time = self._bj_time()
            time_str = beijing_time.strftime('%H:%M')
            notion_title = f"[{time_str}] [{display_name}] 即刻24h热点观察 ({len(posts)}条动态)"

            self.logger.info(f"开始推送日报到Notion ({display_name}): {notion_title}")

            notion_result = jike_notion_client.create_report_page(
                report_title=notion_title,
                report_content=report_content,
                report_date=beijing_time
            )

            if notion_result.get('success'):
                self.logger.info(f"日报成功推送到Notion ({display_name}): {notion_result.get('page_url')}")
                notion_push_info = {
                    'success': True,
                    'page_url': notion_result.get('page_url'),
                    'path': notion_result.get('path')
                }
            else:
                error_msg = notion_result.get('error', '未知错误')
                self.logger.warning(f"推送日报到Notion失败 ({display_name}): {error_msg}")
                notion_push_info = {
                    'success': False,
                    'error': error_msg
                }

        except Exception as e:
            self.logger.warning(f"推送日报到Notion时出错 ({display_name}): {e}")
            notion_push_info = {
                'success': False,
                'error': str(e)
            }

        if notion_push_info:
            model_report['notion_push'] = notion_push_info

        return model_report

    async def _generate_weekly_report_for_model(
        self,
        *,
        model_name: str,
        display_name: str,
        daily_reports: List[Dict[str, Any]],
        content_md: str,
        sources: List[Dict[str, Any]],
        start_time: datetime,
        end_time: datetime,
        items_analyzed: int
    ) -> Dict[str, Any]:
        """在独立线程中生成指定模型的周报"""
        return await asyncio.to_thread(
            self._generate_weekly_report_for_model_sync,
            model_name,
            display_name,
            daily_reports,
            content_md,
            sources,
            start_time,
            end_time,
            items_analyzed
        )

    def _generate_weekly_report_for_model_sync(
        self,
        model_name: str,
        display_name: str,
        daily_reports: List[Dict[str, Any]],
        content_md: str,
        sources: List[Dict[str, Any]],
        start_time: datetime,
        end_time: datetime,
        items_analyzed: int
    ) -> Dict[str, Any]:
        """同步执行指定模型的周报生成和Notion推送"""

        self.logger.info(f"[{display_name}] 模型线程启动，开始生成周报")

        llm_analysis_result = self._analyze_with_llm(content_md, self._prompt_weekly(), model_override=model_name)

        if not llm_analysis_result:
            error_msg = "LLM分析失败，未生成周报"
            self.logger.warning(f"[{display_name}] {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'model': model_name,
                'model_display': display_name
            }

        llm_output = llm_analysis_result.get('content', '')
        beijing_time = self._bj_time()
        header_info = [
            f"# 📊 即刻周度社群洞察 - {display_name}",
            "",
            f"*报告生成时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}*  ",
            "",
            f"*覆盖区间: {start_time.strftime('%Y-%m-%d %H:%M:%S')} - {end_time.strftime('%Y-%m-%d %H:%M:%S')}*  ",
            "",
            f"*日报来源数: {len(daily_reports)} 篇 | 覆盖动态 {items_analyzed} 条*",
            "",
            "---",
            ""
        ]

        cleaned_llm_output = self._clean_llm_output_for_notion(llm_output)
        sources_section = self._render_sources_section(sources)

        footer_lines = ["", "---", ""]
        provider = llm_analysis_result.get('provider')
        model = llm_analysis_result.get('model')
        if provider:
            footer_lines.append(f"*分析引擎: {provider} ({model or 'unknown'})*")
        
        footer_lines.extend([
            "",
            "*本报告由AI自动生成，仅供参考*"
        ])
        footer_section = "\n".join(footer_lines)

        report_parts: List[str] = ["\n".join(header_info), cleaned_llm_output]
        if sources_section:
            report_parts.append("")
            report_parts.append(sources_section)
        report_parts.append("")
        report_parts.append(footer_section)

        report_content = "\n".join(report_parts)
        report_content = self._enhance_source_links(report_content, sources)

        title = f"即刻周度社群洞察 - {display_name} - 截止 {end_time.strftime('%Y-%m-%d')}"
        report_row = {
            'report_type': 'weekly_digest',
            'scope': 'global',
            'analysis_period_start': start_time,
            'analysis_period_end': end_time,
            'items_analyzed': items_analyzed,
            'report_title': title,
            'report_content': report_content,
        }
        report_id = self.db.save_report(report_row)

        model_report = {
            'model': model_name,
            'model_display': display_name,
            'success': True,
            'report_id': report_id,
            'report_title': title,
            'provider': llm_analysis_result.get('provider') if llm_analysis_result else None,
            'items_analyzed': items_analyzed
        }

        # 尝试推送到Notion
        notion_push_info = None
        try:
            from .notion_client import jike_notion_client

            # 格式化Notion标题
            beijing_time = self._bj_time()
            notion_title = f"[{display_name}] 即刻周度社群洞察 - {beijing_time.strftime('%Y%m%d')} ({items_analyzed}条动态)"

            self.logger.info(f"开始推送周报到Notion ({display_name}): {notion_title}")

            notion_result = jike_notion_client.create_report_page(
                report_title=notion_title,
                report_content=report_content,
                report_date=beijing_time
            )

            if notion_result.get('success'):
                self.logger.info(f"周报成功推送到Notion ({display_name}): {notion_result.get('page_url')}")
                notion_push_info = {
                    'success': True,
                    'page_url': notion_result.get('page_url'),
                    'path': notion_result.get('path')
                }
            else:
                error_msg = notion_result.get('error', '未知错误')
                self.logger.warning(f"推送周报到Notion失败 ({display_name}): {error_msg}")
                notion_push_info = {
                    'success': False,
                    'error': error_msg
                }

        except Exception as e:
            self.logger.warning(f"推送周报到Notion时出错 ({display_name}): {e}")
            notion_push_info = {
                'success': False,
                'error': str(e)
            }

        if notion_push_info:
            model_report['notion_push'] = notion_push_info

        return model_report

    # ---------- 数据准备与格式化 ----------
    def _post_has_media(self, post: Dict[str, Any]) -> bool:
        """判断帖子是否包含媒体内容（图片）"""
        import re

        # 获取帖子内容
        post_text = post.get('summary', '') or post.get('title', '')
        if not post_text:
            return False

        # 检查是否包含 Markdown 图片语法
        img_pattern = r'!\[.*?\]\((https?://[^)]+)\)'
        image_urls = re.findall(img_pattern, post_text)

        return len(image_urls) > 0

    def _get_media_count(self, post: Dict[str, Any]) -> int:
        """获取帖子中的图片数量"""
        import re

        post_text = post.get('summary', '') or post.get('title', '')
        if not post_text:
            return 0

        img_pattern = r'!\[.*?\]\((https?://[^)]+)\)'
        image_urls = re.findall(img_pattern, post_text)

        return len(image_urls)

    def _clean_image_urls_from_content(self, content: str, media_count: int = 0) -> str:
        """
        清理帖子内容中的图片URL，替换为简短说明

        Args:
            content: 原始帖子内容
            media_count: 图片数量

        Returns:
            清理后的内容
        """
        if not content:
            return ""

        import re

        # 匹配markdown图片语法：![...](...) 或 ![](...)
        # 以及各种图片URL模式
        markdown_img_pattern = r'!\[.*?\]\([^\)]+\)'

        # 移除所有markdown图片
        cleaned = re.sub(markdown_img_pattern, '', content)

        # 移除可能的图片URL（常见的图片域名）
        img_url_patterns = [
            r'https?://[^\s]*\.(?:jpg|jpeg|png|gif|webp|bmp)[^\s]*',
            r'https?://img\.[^\s]+',
            r'https?://image\.[^\s]+',
            r'https?://pic\.[^\s]+',
        ]

        for pattern in img_url_patterns:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)

        # 清理多余的空行（保留最多一个空行）
        cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
        cleaned = cleaned.strip()

        # 在内容开头添加简短的图片说明
        if media_count > 0:
            img_note = f"[附{media_count}张图]"
            cleaned = f"{img_note}\n{cleaned}"

        return cleaned

    def _truncate(self, text: str, max_len: int) -> str:
        if not text:
            return ""
        if len(text) <= max_len:
            return text
        t = text[:max_len]
        # 尝试在句尾截断
        for d in ['。', '!', '?', '.', '!', '?', '\n']:
            pos = t.rfind(d)
            if pos > max_len * 0.7:
                return t[:pos + 1] + "\n..."
        return t + "\n..."

    def _clean_llm_output_for_notion(self, llm_output: str) -> str:
        """清理LLM输出内容，确保Notion兼容性"""
        if not llm_output:
            return ""

        # 保护Source引用格式，不要替换其中的方括号
        import re

        # 先提取所有Source引用
        source_pattern = r'\[Sources?:\s*[T\d\s,]+\]'
        sources = re.findall(source_pattern, llm_output)

        # 临时替换Source引用为占位符
        temp_llm_output = llm_output
        source_placeholders = {}
        for i, source in enumerate(sources):
            placeholder = f"__SOURCE_PLACEHOLDER_{i}__"
            source_placeholders[placeholder] = source
            temp_llm_output = temp_llm_output.replace(source, placeholder)

        # 替换其他可能导致Markdown链接冲突的方括号
        cleaned = temp_llm_output.replace('[', '【').replace(']', '】')

        # 恢复Source引用
        for placeholder, original_source in source_placeholders.items():
            cleaned = cleaned.replace(placeholder, original_source)

        # 确保行尾有适当的空格用于换行
        lines = cleaned.split('\n')
        processed_lines = []

        for line in lines:
            # 对于以*开头的斜体行，在行尾添加空格以确保换行
            if line.strip().startswith('*') and line.strip().endswith('*'):
                processed_lines.append(line.rstrip() + '  ')
            else:
                processed_lines.append(line)

        return '\n'.join(processed_lines)

    def _format_posts_for_llm(self, posts: List[Dict[str, Any]], source_prefix: str = 'T') -> Tuple[str, List[Dict[str, Any]]]:
        """
        将帖子格式化为带编号的紧凑文本，根据context_mode和媒体情况智能包含解读信息

        优化策略：
        - light模式：仅对图文帖保留解读，纯文本帖只保留原文（压缩上下文）
        - full模式：所有帖子都包含解读（保持完整信息）
        - 清理图片URL，减少token消耗

        Args:
            posts: 帖子数据列表
            source_prefix: 来源ID前缀

        Returns:
            (格式化后的文本, 源映射列表)
        """
        lines: List[str] = []
        sources: List[Dict[str, Any]] = []
        total_chars = 0

        for idx, p in enumerate(posts, 1):
            sid = f"{source_prefix}{idx}"
            nickname = p.get('nickname') or p.get('jike_user_id') or '未知作者'
            link = p.get('link') or ''

            # 获取原始内容
            summary = p.get('summary') or ''

            # 检查是否有媒体
            has_media = self._post_has_media(p)

            # 计算媒体数量
            media_count = self._get_media_count(p) if has_media else 0

            # 清理图片URL，压缩上下文
            summary = self._clean_image_urls_from_content(summary, media_count)

            # 决定是否包含解读
            # light模式：只对有媒体的帖子包含解读
            # full模式：所有帖子都包含解读
            include_interpretation = (self.context_mode == 'full') or (self.context_mode == 'light' and has_media)

            # 获取解读信息
            interpretation_text = ''
            interpretation_model = ''
            if include_interpretation:
                interpretation_text = p.get('interpretation_text') or ''
                interpretation_model = p.get('interpretation_model') or ''

            # 截断处理
            summary_t = self._truncate(summary, 1500)
            interpretation_t = self._truncate(interpretation_text, 3000) if interpretation_text else ''

            # 构建紧凑的帖子块
            if include_interpretation and interpretation_text:
                # 有解读的格式：更紧凑
                block = f"[{sid} @{nickname}]\n{summary_t}\n→ 洞察: {interpretation_t}"
            else:
                # 纯文本格式：极简
                block = f"[{sid} @{nickname}]\n{summary_t}"

            # 检查长度限制
            if total_chars + len(block) > self.max_content_length:
                self.logger.info(f"达到最大内容限制({self.max_content_length}),截断帖子列表于第 {idx-1} 条")
                break

            lines.append(block)
            total_chars += len(block)

            # 构建来源映射（用于后续生成来源清单）
            title = p.get('title') or ''
            title_t = self._truncate(title, 140)
            sources.append({
                'sid': sid,
                'title': title_t or self._truncate(summary, 100),
                'link': link,
                'nickname': nickname,
                'excerpt': self._truncate(summary, 120)
            })

        return "\n\n---\n\n".join(lines), sources

    def _format_daily_reports_for_weekly(self, daily_reports: List[Dict[str, Any]]) -> Tuple[str, List[Dict[str, Any]]]:
        """将每日热点报告合成为周报输入上下文"""
        if not daily_reports:
            return "", []

        lines: List[str] = []
        sources: List[Dict[str, Any]] = []

        for idx, report in enumerate(daily_reports, 1):
            label = f"D{idx}"
            title = report.get('report_title') or f"Daily Hotspot #{idx}"
            items = report.get('items_analyzed') or 0

            start_dt = report.get('analysis_period_start')
            end_dt = report.get('analysis_period_end')

            if isinstance(start_dt, datetime):
                start_str = start_dt.strftime('%Y-%m-%d %H:%M')
            else:
                start_str = str(start_dt)

            if isinstance(end_dt, datetime):
                end_str = end_dt.strftime('%Y-%m-%d %H:%M')
                date_label = end_dt.strftime('%Y-%m-%d')
            else:
                end_str = str(end_dt)
                date_label = str(end_dt)

            lines.append(f"## {label} · {date_label} · {title}")
            lines.append("")
            lines.append(f"*覆盖区间*: {start_str} - {end_str}  |  *汇总动态*: {items} 条")
            lines.append("")

            report_content = (report.get('report_content') or "").strip()
            if report_content:
                lines.append(report_content)
                lines.append("")

            lines.append("---")
            lines.append("")

            sources.append({
                'sid': label,
                'title': title,
                'link': '',
                'nickname': date_label,
                'excerpt': self._truncate(title, 80)
            })

        while lines and lines[-1] == "":
            lines.pop()
        if lines and lines[-1] == "---":
            lines.pop()

        return "\n".join(lines), sources

    def _render_sources_section(self, sources: List[Dict[str, Any]]) -> str:
        if not sources:
            return ""

        lines = ["## 📚 来源清单 (Source List)", ""]
        for s in sources:
            # 清理标题中的方括号，避免与Markdown链接冲突
            clean_title = (s.get('title') or s.get('excerpt') or '').replace('[', '【').replace(']', '】')
            nickname = s.get('nickname') or ''
            if nickname:
                nickname_display = f"@{nickname}"
            else:
                nickname_display = ""

            link = s.get('link')
            if link:
                actor_part = f"[{nickname_display}]({link})" if nickname_display else f"[来源]({link})"
            else:
                actor_part = nickname_display or "来源"

            lines.append(f"- **【{s.get('sid')}】**: {actor_part}: {clean_title}")
        return "\n".join(lines)

    def _enhance_source_links(self, report_content: str, sources: List[Dict[str, Any]]) -> str:
        """
        增强报告中的来源链接，将 [Source: T1, T2] 中的每个 Txx 转换为可点击的链接
        """
        import re

        # 构建来源ID到链接的映射
        source_link_map = {s['sid']: s['link'] for s in sources}

        def replace_source_refs(match):
            # 提取完整的 Source 引用内容
            full_source_text = match.group(0)  # 如 "[Source: T2, T9, T18]"
            source_content = match.group(1)    # 如 "T2, T9, T18"

            # 分割并处理每个来源ID
            source_ids = [sid.strip() for sid in source_content.split(',')]
            linked_sources = []

            for sid in source_ids:
                if sid in source_link_map:
                    # 将 Txx 转换为链接
                    linked_sources.append(f"[{sid}]({source_link_map[sid]})")
                else:
                    # 如果找不到对应链接，保持原样
                    linked_sources.append(sid)

            # 重新组合
            return f"📎 [Source: {', '.join(linked_sources)}]"

        # 查找所有 [Source: ...] 或 [Sources: ...] 模式并替换
        pattern = r'\[Sources?:\s*([T\d\s,]+)\]'
        enhanced_content = re.sub(pattern, replace_source_refs, report_content)

        return enhanced_content

    # ---------- Prompt 模板 ----------
    def _prompt_daily_briefing(self) -> str:
        """构建"即刻日报资讯"式的简报提示词，强调全面性和分类聚合"""

        # 根据context_mode动态生成数据格式描述
        if self.context_mode == 'light':
            data_format_description = """# Input Data Format:
你将收到一系列经过预处理的帖子。纯文本帖只包含原文；图文帖会额外附带AI生成的`→ 洞察:`。
- 纯文本帖: `[T_id @user_handle]` + 帖子原文
- 图文帖: `[T_id @user_handle]` + 帖子原文 + `→ 洞察: {{AI解读}}`"""
        else:  # full mode
            data_format_description = """# Input Data Format:
你将收到一系列经过预处理的帖子。每条帖子都包含原文和AI生成的`→ 洞察:`。
- 格式: `[T_id @user_handle]` + 帖子原文 + `→ 洞察: {{AI解读}}`"""

        return f"""# Role: 资深科技社区分析师，专注于从即刻社区发掘价值信息

# Context:
你正在为忙碌的科技从业者、产品经理和投资者编写一份即刻社区每日快讯。你的目标是快速、精准地捕捉社区内的产品灵感、技术思考、行业趋势和有价值的讨论，而不是简单地罗列新闻。

# Core Principles:
1. **价值优先 (Value First)**: 优先收录具有启发性的思考、新颖的观点和高价值的资源。
2. **分类清晰 (Clear Categorization)**: 严格按照即刻社区的特色主题进行分类，便于读者快速定位自己感兴趣的内容。
3. **详略得当 (Appropriate Detail)**: 每条信息都应提供足够上下文，确保读者能理解其核心价值。避免过度压缩，但保持精炼。
4. **绝对可追溯 (Absolute Traceability)**: 每条信息必须在末尾标注来源 `[Source: T_n]`。

{data_format_description}

# Your Task:
生成一份结构化的日报资讯，严格按照以下Markdown格式。请注意，你的任务是信息聚合与提炼，而非深度分析。

## 🚀 产品与动态
*新产品发布、功能更新、增长策略、用户体验讨论*
- **[产品/功能名]**: 详细介绍其核心动态、用户反馈或增长策略，确保信息完整 (50-200字) [Source: T_n]

---

## 💡 技术与思考
*新技术实践、底层逻辑思考、开发经验、方法论分享*
- **[技术点/思考点]**: 清晰阐述其核心观点、技术细节或方法论，提供足够背景 (50-200字) [Source: T_n]

---

## 📈 行业与趋势
*行业新闻洞察、市场分析、商业模式探讨、投融资动态*
- **[观察点]**: 阐述关键信息、数据和你的解读，说明其对行业的影响 (50-200字) [Source: T_n]

---

## 💬 社区热议
*社区内广泛讨论的文化现象、公共事件或热门话题*
- **[话题名]**: 详细总结讨论的焦点、不同观点和社区情绪，让读者了解全貌 (50-200字) [Source: T_n]

---

## 🌟 精选观点与资源
*值得关注的独特见解、有趣想法或高价值工具/文章分享*
- **[@用户]**: 清晰阐述其核心观点、论据和启发意义 (50-200字) [Source: T_n]
- **[资源名称]**: 详细说明其用途、特点和推荐理由 (50-200字) [Source: T_m]

# Input Data:
{{content}}

# Important Notes:
1. **如果某个分类下有丰富的内容，请尽可能全面地收录，不要遗漏。**
2. **如果内容较少，确保至少有3-5条精华信息。**
3. **如果某个分类下完全没有相关内容，则在最终报告中省略该分类。**
4. 每条信息都必须有 `[Source: T_n]` 标注。
5. **内容为王**: 确保每条信息的描述足够清晰、完整，能够独立成文。对于复杂或重要的动态，宁可篇幅稍长，也要说清楚来龙去脉和核心价值。
"""

    def _prompt_daily(self) -> str:
        """构建日报提示词，根据context_mode调整数据格式说明"""

        # 根据context_mode生成精确的数据格式描述
        if self.context_mode == 'light':
            data_format_description = """# Input Data Format:
你将收到一系列经过预处理的帖子，采用紧凑格式以优化上下文。每条帖子包含原始文本内容；只有当帖子包含图片或多媒体时，才会额外附带AI深度洞察。

**格式说明**：
- 纯文本帖：`[T_id @user_handle]` + 换行 + 帖子原文
- 图文帖：`[T_id @user_handle]` + 换行 + 帖子原文 + 换行 + `→ 洞察: AI生成的综合解读`

**重要**：
1. T_id 是来源标识符，你在分析中引用时使用 `[Source: T_id]` 格式
2. 对于纯文本帖，请直接基于原文进行分析
3. 对于图文帖，请综合原文和洞察内容进行分析"""
        else:  # full mode
            data_format_description = """# Input Data Format:
你将收到一系列经过预处理的帖子，采用紧凑格式以优化上下文。每条帖子都包含原始内容和AI生成的深度洞察。

**格式说明**：
`[T_id @user_handle]` + 换行 + 帖子原文 + 换行 + `→ 洞察: LLM生成的深度解读`

**重要**：
1. T_id 是来源标识符，你在分析中引用时使用 `[Source: T_id]` 格式
2. 请综合利用原文和洞察两部分信息进行分析
3. 洞察部分是AI对帖子的深度解读，是你分析的核心依据"""

        return f"""# Role: 资深社区战略分析师

# Context:
你正在分析一个由技术专家、产品经理、投资人和创业者组成的精英社区——'即刻'在过去24小时内发布的帖子。你的任务是基于我提供的、已编号的原始讨论材料和AI深度洞察（如有），撰写一份信息密度高、内容详尽、可读性强的情报简报。

# Core Principles:
1.  **价值导向与深度优先**: 你的核心目标是挖掘出对从业者有直接价值的信息。在撰写每个部分时，都应追求内容的**深度和完整性**，**避免过于简短的概括**。
2.  **深度合成 (Deep Synthesis)**: 不要简单罗列。你需要将不同来源的信息点连接起来，构建成有意义的叙事（Narrative）。
3.  **注入洞见 (Inject Insight)**: 你不是一个总结者，而是一个分析师。在陈述事实和观点的基础上，**必须**加入你自己的、基于上下文的、有深度的分析和评论。
4.  **绝对可追溯 (Absolute Traceability)**: 你的每一条洞察、判断和建议，都必须在句末使用 `[Source: T_n]` 或 `[Sources: T_n, T_m]` 的格式明确标注信息来源。这是硬性要求,绝对不能遗漏。
5.  **识别帖子类型**: 在分析时，请注意识别每个主题的潜在类型，例如：`[AI/前沿技术]`, `[产品与设计]`, `[创业与投资]`, `[个人成长与思考]`, `[行业与市场动态]`, `[工具与工作流分享]`等。这有助于你判断其核心价值。

---

{data_format_description}

---

# Input Data (帖子数据，已编号):
{{content}}

---

# Your Task:
请严格按照以下结构和要求，生成一份内容丰富详实的完整Markdown报告。

**第一部分：本时段焦点速报 (Top Topics Overview)**
*   任务：通读所有材料，为每个值得关注的核心主题撰写一份**详细摘要**。
*   要求：不仅要总结主题的核心内容，还要**尽可能全面地**列出主要的讨论方向和关键观点。篇幅无需严格限制，力求全面。

**第二部分：核心洞察与趋势 (Executive Summary & Trends)**
*   任务：基于第一部分的所有信息，从全局视角提炼出关键洞察与趋势。
*   要求：
    *   **核心洞察**: **尽可能全面地**提炼你发现的重要趋势或洞察，并详细阐述，**不要局限于少数几点**。
    *   **技术风向与工具箱**: **详细列出并介绍**被热议的新技术、新框架或工具。对于每个项目，请提供更详尽的描述，包括其用途、优点、以及社区讨论中的具体评价。
    *   **社区热议与需求点**: **详细展开**社区普遍关心的话题、遇到的痛点或潜在的需求，说明其背景、当前讨论的焦点以及潜在的影响。

**第三部分：价值信息挖掘 (Valuable Information Mining)**
*   任务：深入挖掘帖子中的高价值信息，并进行详细介绍。
*   要求：
    *   **高价值资源/工具**: **详细列出并介绍**讨论中出现的可以直接使用的软件、库、API、开源项目或学习资料。包括资源的用途和社区评价。
    *   **有趣观点/深度讨论**: **详细阐述**那些引人深思、具有启发性的个人观点或高质量的讨论。分析该观点为何重要或具有启发性。

**第四部分：行动建议 (Actionable Recommendations)**
*   任务：基于以上所有分析，为社区中的不同角色提供丰富且具体的建议。
*   要求：建议必须有高度的针对性，并阐述其背后的逻辑和预期效果。
    *   **给产品经理的建议**: ...
    *   **给创业者/投资者的建议**: ...
    *   **给技术从业者的建议**: ...

---

# Output Format (Strictly follow this Markdown structure):

## 一、本时段焦点速报

### **1. [主题A的标题]**
*   **详细摘要**: [详细摘要该主题的核心内容，并列出主要的讨论方向和关键观点。篇幅无需严格限制，力求全面。] [Source: T_n]

### **2. [主题B的标题]**
*   **详细摘要**: [同上。] [Source: T_m]

...(罗列所有你认为值得报告的热门主题)

---

## 二、核心洞察与趋势

*   **核心洞察**:
    *   [详细阐述你发现的一个重要趋势或洞察。例如：AI Agent的实现和应用成为新的技术焦点，社区内涌现了多个围绕此展开的开源项目和实践讨论，具体表现在...] [Sources: T2, T9]
    *   [详细阐述第二个重要洞察。] [Sources: T3, T7]
    *   ...(尽可能多地列出洞察)

*   **技术风向与工具箱**:
    *   **[技术/工具A]**: [详细介绍它是什么，为什么它现在很热门，社区成员如何评价它，以及它解决了什么具体问题。] [Source: T3]
    *   **[技术/工具B]**: [同上。] [Source: T7]
    *   ...(尽可能多地列出技术/工具)

*   **社区热议与需求点**:
    *   **[热议话题A]**: [详细展开一个被广泛讨论的话题，包括讨论的背景、各方观点、争议点以及对未来的展望。] [Source: T5]
    *   **[普遍需求B]**: [详细总结一个普遍存在的需求，并分析该需求产生的原因和社区提出的潜在解决方案。] [Source: T10]
    *   ...(尽可能多地列出话题/需求)

---

## 三、价值信息挖掘

*   **高价值资源/工具**:
    *   **[资源/工具A]**: [详细介绍该资源/工具，包括其名称、功能、优点以及社区成员分享的使用技巧或经验。] [Source: T2]
    *   **[资源/工具B]**: [同上。] [Source: T8]
    *   ...(尽可能多地列出资源/工具)

*   **有趣观点/深度讨论**:
    *   **[关于"XX"的观点]**: [详细阐述一个有启发性的观点，分析其重要性，并总结因此引发的精彩讨论。] [Source: T4]
    *   **[关于"YY"的讨论]**: [同上。] [Source: T6]
    *   ...(尽可能多地列出观点/讨论)

---

## 四、行动建议

*   **给产品经理的建议**:
    *   [建议1：[提出具体建议]。理由与预期效果：[阐述该建议的逻辑依据，以及采纳后可能带来的好处]。] [Sources: T2, T9]
    *   [建议2：...]

*   **给创业者/投资者的建议**:
    *   [建议1：[提出具体建议]。理由与预期效果：[阐述该建议的逻辑依据，以及采纳后可能带来的好处]。] [Source: T1]
    *   [建议2：...]

*   **给技术从业者的建议**:
    *   [建议1：[提出具体建议]。理由与预期效果：[阐述该建议的逻辑依据，以及采纳后可能带来的好处]。] [Source: T3]
    *   [建议2：...]
"""

    def _prompt_weekly(self) -> str:
        return (
            "# Role: 资深社群战略顾问\n"
            "\n"
            "# Context:\n"
            "你将基于最近7天的《即刻24h热点追踪器》日报汇编（已按时间顺序标记为 D1...Dn）。每份日报都已完成当天的主题提炼与来源引用，请在你的分析中引用这些日报编号，例如 `[Source: D3]` 或 `[Sources: D2, D6]`。目标是从跨日视角识别趋势、结构化洞察，并输出高价值的周度战略建议。\n"
            "\n"
            "# Core Principles:\n"
            "1. 必须在每个结论或建议后注明引用的日报编号，保持可追溯性。\n"
            "2. 强调时间序列上的变化、动因与潜在走向，而不是简单堆叠每日摘要。\n"
            "3. 从技术/产品/创业/投资/行业/文化等多个视角识别层次化洞察，指出各角色的关注点。\n"
            "4. 若发现连续几日重复出现的议题，请归纳其演进路径与背后驱动因素。\n"
            "\n"
            "# Input Materials (Daily Hotspot Reports D1...Dn):\n\n{content}\n\n"
            "# Your Task:\n"
            "请输出结构化Markdown周报，至少包含以下模块：\n"
            "## 一、核心主题与关注度 (Top Themes)\n"
            "- 归纳3-5个跨日持续受到关注的主题，说明演进脉络与关键结论。[Sources: ...]\n"
            "\n"
            "## 二、关键洞察与趋势判断 (Insights & Trends)\n"
            "- 提炼本周出现的显著变化、潜在风险或新机会，分析成因与影响面。[Sources: ...]\n"
            "- 列举值得跟进的技术/产品/市场信号，并说明其热度演进。[Sources: ...]\n"
            "\n"
            "## 三、结构化深度分析 (Deep Dive)\n"
            "- 从供给/需求/生态/竞争格局等角度展开2-3个深度专题，解释其战略意义。[Sources: ...]\n"
            "\n"
            "## 四、角色定向行动建议 (Actionables)\n"
            "- 分别为产品经理、技术从业者、创业者/投资者提供1-2条可执行建议，说明建议依据与预期收益。[Sources: ...]\n"
        )

    # ---------- 报告生成 ----------
    def _analyze_with_llm(self, content: str, prompt_template: str, model_override: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """调用智能模型进行深度分析，失败时返回None"""
        try:
            if llm_client is None:
                return None
            # 格式化提示词
            prompt = prompt_template.format(content=content)
            # 使用智能模型进行复杂报告生成任务
            res = llm_client.call_smart_model(prompt, model_override=model_override)
            if isinstance(res, dict) and res.get('success'):
                return res
            return None
        except Exception as e:  # 兜底，避免影响主流程
            self.logger.warning(f"智能模型分析失败，将回退本地报告: {e}")
            return None

    def _make_fallback_report(
        self,
        header: str,
        posts: List[Dict[str, Any]],
        period_start: datetime,
        period_end: datetime,
        sources: List[Dict[str, Any]],
    ) -> str:
        lines: List[str] = []
        lines.append(header)
        lines.append("")
        lines.append(f"时间范围：{period_start.strftime('%Y-%m-%d %H:%M')} - {period_end.strftime('%Y-%m-%d %H:%M')}")
        lines.append("")
        lines.append("LLM分析不可用，以下为基于素材的占位报告：")
        lines.append("")
        lines.append("## 热门动态清单 (Top Materials)")
        for idx, p in enumerate(posts[:30], 1):
            title = p.get('title') or '(无标题)'
            link = p.get('link') or ''
            nickname = p.get('nickname') or p.get('jike_user_id') or '未知作者'
            lines.append(f"{idx}. {title} - @{nickname}  {link}")
        lines.append("")
        lines.append(self._render_sources_section(sources))
        report_content = "\n".join(lines)

        # 为占位报告也应用来源链接增强后处理
        report_content = self._enhance_source_links(report_content, sources)

        return report_content

    async def _generate_light_report_for_model(
        self,
        *,
        model_name: str,
        display_name: str,
        posts: List[Dict[str, Any]],
        content_md: str,
        sources: List[Dict[str, Any]],
        prompt: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """在独立线程中生成指定模型的日报资讯"""
        return await asyncio.to_thread(
            self._generate_light_report_for_model_sync,
            model_name,
            display_name,
            posts,
            content_md,
            sources,
            prompt,
            start_time,
            end_time
        )

    def _generate_light_report_for_model_sync(
        self,
        model_name: str,
        display_name: str,
        posts: List[Dict[str, Any]],
        content_md: str,
        sources: List[Dict[str, Any]],
        prompt: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """同步执行指定模型的日报资讯生成和Notion推送"""

        self.logger.info(f"[{display_name}] 模型线程启动，开始生成日报资讯")

        llm_analysis_result = self._analyze_with_llm(content_md, prompt, model_override=model_name)

        if not llm_analysis_result:
            error_msg = "LLM分析失败，未生成日报资讯"
            self.logger.warning(f"[{display_name}] {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'model': model_name,
                'model_display': display_name,
                'report_type': 'light'
            }

        llm_output = llm_analysis_result.get('content', '')
        beijing_time = self._bj_time()
        header_info = [
            f"# 📋 即刻日报资讯 - {display_name}",
            "",
            f"*报告生成时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}*  ",
            "",
            f"*数据范围: {start_time.strftime('%Y-%m-%d %H:%M:%S')} - {end_time.strftime('%Y-%m-%d %H:%M:%S')}*  ",
            "",
            f"*分析动态数: {len(posts)} 条*",
            "",
            "---",
            ""
        ]

        cleaned_llm_output = self._clean_llm_output_for_notion(llm_output)
        sources_section = self._render_sources_section(sources)

        footer_lines = ["", "---", ""]
        provider = llm_analysis_result.get('provider')
        model = llm_analysis_result.get('model')
        if provider:
            footer_lines.append(f"*分析引擎: {provider} ({model or 'unknown'})*")

        footer_lines.extend([
            "",
            f"📊 **统计摘要**: 本报告分析了 {len(posts)} 条动态",
            "",
            "*本报告由AI自动生成，仅供参考*"
        ])
        footer_section = "\n".join(footer_lines)

        report_content = "\n".join(header_info) + cleaned_llm_output + "\n\n" + sources_section + footer_section
        report_content = self._enhance_source_links(report_content, sources)

        title = f"即刻日报资讯 - {display_name} - {end_time.strftime('%Y-%m-%d %H:%M')}"
        report_row = {
            'report_type': 'daily_light',
            'scope': 'global',
            'analysis_period_start': start_time,
            'analysis_period_end': end_time,
            'items_analyzed': len(posts),
            'report_title': title,
            'report_content': report_content,
        }
        report_id = self.db.save_report(report_row)

        model_report = {
            'model': model_name,
            'model_display': display_name,
            'success': True,
            'report_id': report_id,
            'report_title': title,
            'report_content': report_content,
            'provider': llm_analysis_result.get('provider') if llm_analysis_result else None,
            'items_analyzed': len(posts)
        }

        # 尝试推送到Notion
        notion_push_info = None
        try:
            from .notion_client import jike_notion_client

            beijing_time = self._bj_time()
            time_str = beijing_time.strftime('%H:%M')
            notion_title = f"[{time_str}] [{display_name}] 即刻日报资讯 ({len(posts)}条)"

            self.logger.info(f"开始推送日报资讯到Notion ({display_name}): {notion_title}")

            notion_result = jike_notion_client.create_report_page_in_hierarchy(
                report_title=notion_title,
                report_content=report_content,
                report_date=beijing_time,
                report_type='light'
            )

            if notion_result.get('success'):
                self.logger.info(f"日报资讯成功推送到Notion ({display_name}): {notion_result.get('page_url')}")
                notion_push_info = {
                    'success': True,
                    'page_url': notion_result.get('page_url'),
                    'path': notion_result.get('path')
                }
            else:
                error_msg = notion_result.get('error', '未知错误')
                self.logger.warning(f"推送日报资讯到Notion失败 ({display_name}): {error_msg}")
                notion_push_info = {
                    'success': False,
                    'error': error_msg
                }

        except Exception as e:
            self.logger.warning(f"推送日报资讯到Notion时出错 ({display_name}): {e}")
            notion_push_info = {
                'success': False,
                'error': str(e)
            }

        if notion_push_info:
            model_report['notion_push'] = notion_push_info

        return model_report

    async def generate_light_reports(self, hours_back: Optional[int] = None) -> Dict[str, Any]:
        """生成日报资讯（Light Report），多模型并行执行

        使用light上下文模式，降低成本
        """
        hours = int(hours_back or self.analysis_cfg.get('hours_back_daily', 24))
        end_time = self._bj_time()
        start_time = end_time - timedelta(hours=hours)

        posts = self.db.get_recent_posts(hours_back=hours)
        if not posts:
            return {
                'success': False,
                'error': f'最近{hours}小时内无新增动态',
                'report_type': 'light'
            }

        # 设置为light模式
        original_mode = self.context_mode
        self.context_mode = 'light'

        content_md, sources = self._format_posts_for_llm(posts, source_prefix='T')
        prompt = self._prompt_daily_briefing()

        # 恢复原始模式
        self.context_mode = original_mode

        models_to_generate = self._get_report_models()
        if not models_to_generate:
            self.logger.warning("未配置任何可用于生成报告的模型")
            return {
                'success': False,
                'error': '未配置可用的LLM模型',
                'items_analyzed': 0,
                'report_type': 'light'
            }

        model_reports: List[Dict[str, Any]] = []
        failures: List[Dict[str, Any]] = []
        tasks = []
        task_meta: List[Dict[str, str]] = []

        for model_name in models_to_generate:
            display_name = self._get_model_display_name(model_name)
            task_meta.append({'model': model_name, 'display': display_name})
            tasks.append(
                self._generate_light_report_for_model(
                    model_name=model_name,
                    display_name=display_name,
                    posts=posts,
                    content_md=content_md,
                    sources=sources,
                    prompt=prompt,
                    start_time=start_time,
                    end_time=end_time
                )
            )

        self.logger.info(
            f"开始并行生成 {len(tasks)} 份日报资讯: {[meta['display'] for meta in task_meta]}"
        )

        task_results = await asyncio.gather(*tasks, return_exceptions=True)

        for meta, task_result in zip(task_meta, task_results):
            model_name = meta['model']
            display_name = meta['display']

            if isinstance(task_result, Exception):
                error_msg = str(task_result)
                self.logger.warning(
                    f"模型 {model_name} ({display_name}) 日报资讯生成过程中出现未处理异常: {error_msg}"
                )
                failures.append({
                    'model': model_name,
                    'model_display': display_name,
                    'error': error_msg
                })
                continue

            if task_result.get('success'):
                model_reports.append(task_result)
            else:
                failure_entry = {
                    'model': model_name,
                    'model_display': display_name,
                    'error': task_result.get('error', '报告生成失败')
                }
                failures.append(failure_entry)

        overall_success = len(model_reports) > 0
        result = {
            'success': overall_success,
            'items_analyzed': len(posts) if overall_success else 0,
            'model_reports': model_reports,
            'failures': failures,
            'report_type': 'light'
        }

        if overall_success:
            primary_report = model_reports[0]
            result['report_id'] = primary_report['report_id']
            result['title'] = primary_report['report_title']
            result['notion_push'] = primary_report.get('notion_push')
            result['report_ids'] = [mr['report_id'] for mr in model_reports]

        self.logger.info(
            f"日报资讯生成完成: 成功生成 {len(model_reports)} 份报告，失败 {len(failures)} 份"
        )

        return result

    async def _generate_deep_report_for_model(
        self,
        *,
        model_name: str,
        display_name: str,
        posts: List[Dict[str, Any]],
        content_md: str,
        sources: List[Dict[str, Any]],
        prompt: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """在独立线程中生成指定模型的深度洞察报告"""
        return await asyncio.to_thread(
            self._generate_deep_report_for_model_sync,
            model_name,
            display_name,
            posts,
            content_md,
            sources,
            prompt,
            start_time,
            end_time
        )

    def _generate_deep_report_for_model_sync(
        self,
        model_name: str,
        display_name: str,
        posts: List[Dict[str, Any]],
        content_md: str,
        sources: List[Dict[str, Any]],
        prompt: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """同步执行指定模型的深度洞察报告生成和Notion推送"""

        self.logger.info(f"[{display_name}] 模型线程启动，开始生成深度洞察报告")

        llm_analysis_result = self._analyze_with_llm(content_md, prompt, model_override=model_name)

        if not llm_analysis_result:
            error_msg = "LLM分析失败，未生成深度洞察"
            self.logger.warning(f"[{display_name}] {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'model': model_name,
                'model_display': display_name,
                'report_type': 'deep'
            }

        llm_output = llm_analysis_result.get('content', '')
        beijing_time = self._bj_time()
        header_info = [
            f"# 📊 即刻深度洞察 - {display_name}",
            "",
            f"*报告生成时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}*  ",
            "",
            f"*数据范围: {start_time.strftime('%Y-%m-%d %H:%M:%S')} - {end_time.strftime('%Y-%m-%d %H:%M:%S')}*  ",
            "",
            f"*分析动态数: {len(posts)} 条*",
            "",
            "---",
            ""
        ]

        cleaned_llm_output = self._clean_llm_output_for_notion(llm_output)
        sources_section = self._render_sources_section(sources)

        footer_lines = ["", "---", ""]
        provider = llm_analysis_result.get('provider')
        model = llm_analysis_result.get('model')
        if provider:
            footer_lines.append(f"*分析引擎: {provider} ({model or 'unknown'})*")

        footer_lines.extend([
            "",
            f"📊 **统计摘要**: 本报告分析了 {len(posts)} 条动态",
            "",
            "*本报告由AI自动生成，仅供参考*"
        ])
        footer_section = "\n".join(footer_lines)

        report_content = "\n".join(header_info) + cleaned_llm_output + "\n\n" + sources_section + footer_section
        report_content = self._enhance_source_links(report_content, sources)

        title = f"即刻深度洞察 - {display_name} - {end_time.strftime('%Y-%m-%d %H:%M')}"
        report_row = {
            'report_type': 'daily_deep',
            'scope': 'global',
            'analysis_period_start': start_time,
            'analysis_period_end': end_time,
            'items_analyzed': len(posts),
            'report_title': title,
            'report_content': report_content,
        }
        report_id = self.db.save_report(report_row)

        model_report = {
            'model': model_name,
            'model_display': display_name,
            'success': True,
            'report_id': report_id,
            'report_title': title,
            'report_content': report_content,
            'provider': llm_analysis_result.get('provider') if llm_analysis_result else None,
            'items_analyzed': len(posts)
        }

        # 尝试推送到Notion
        notion_push_info = None
        try:
            from .notion_client import jike_notion_client

            beijing_time = self._bj_time()
            time_str = beijing_time.strftime('%H:%M')
            notion_title = f"[{time_str}] [{display_name}] 即刻深度洞察 ({len(posts)}条)"

            self.logger.info(f"开始推送深度洞察到Notion ({display_name}): {notion_title}")

            notion_result = jike_notion_client.create_report_page_in_hierarchy(
                report_title=notion_title,
                report_content=report_content,
                report_date=beijing_time,
                report_type='deep'
            )

            if notion_result.get('success'):
                self.logger.info(f"深度洞察成功推送到Notion ({display_name}): {notion_result.get('page_url')}")
                notion_push_info = {
                    'success': True,
                    'page_url': notion_result.get('page_url'),
                    'path': notion_result.get('path')
                }
            else:
                error_msg = notion_result.get('error', '未知错误')
                self.logger.warning(f"推送深度洞察到Notion失败 ({display_name}): {error_msg}")
                notion_push_info = {
                    'success': False,
                    'error': error_msg
                }

        except Exception as e:
            self.logger.warning(f"推送深度洞察到Notion时出错 ({display_name}): {e}")
            notion_push_info = {
                'success': False,
                'error': str(e)
            }

        if notion_push_info:
            model_report['notion_push'] = notion_push_info

        return model_report

    async def generate_deep_reports(self, hours_back: Optional[int] = None) -> Dict[str, Any]:
        """生成深度洞察报告（Deep Report），多模型并行执行

        使用full上下文模式，保证深度分析
        """
        hours = int(hours_back or self.analysis_cfg.get('hours_back_daily', 24))
        end_time = self._bj_time()
        start_time = end_time - timedelta(hours=hours)

        posts = self.db.get_recent_posts(hours_back=hours)
        if not posts:
            return {
                'success': False,
                'error': f'最近{hours}小时内无新增动态',
                'report_type': 'deep'
            }

        # 设置为full模式
        original_mode = self.context_mode
        self.context_mode = 'full'

        content_md, sources = self._format_posts_for_llm(posts, source_prefix='T')
        prompt = self._prompt_daily()

        # 恢复原始模式
        self.context_mode = original_mode

        models_to_generate = self._get_report_models()
        if not models_to_generate:
            self.logger.warning("未配置任何可用于生成报告的模型")
            return {
                'success': False,
                'error': '未配置可用的LLM模型',
                'items_analyzed': 0,
                'report_type': 'deep'
            }

        model_reports: List[Dict[str, Any]] = []
        failures: List[Dict[str, Any]] = []
        tasks = []
        task_meta: List[Dict[str, str]] = []

        for model_name in models_to_generate:
            display_name = self._get_model_display_name(model_name)
            task_meta.append({'model': model_name, 'display': display_name})
            tasks.append(
                self._generate_deep_report_for_model(
                    model_name=model_name,
                    display_name=display_name,
                    posts=posts,
                    content_md=content_md,
                    sources=sources,
                    prompt=prompt,
                    start_time=start_time,
                    end_time=end_time
                )
            )

        self.logger.info(
            f"开始并行生成 {len(tasks)} 份深度洞察: {[meta['display'] for meta in task_meta]}"
        )

        task_results = await asyncio.gather(*tasks, return_exceptions=True)

        for meta, task_result in zip(task_meta, task_results):
            model_name = meta['model']
            display_name = meta['display']

            if isinstance(task_result, Exception):
                error_msg = str(task_result)
                self.logger.warning(
                    f"模型 {model_name} ({display_name}) 深度洞察生成过程中出现未处理异常: {error_msg}"
                )
                failures.append({
                    'model': model_name,
                    'model_display': display_name,
                    'error': error_msg
                })
                continue

            if task_result.get('success'):
                model_reports.append(task_result)
            else:
                failure_entry = {
                    'model': model_name,
                    'model_display': display_name,
                    'error': task_result.get('error', '报告生成失败')
                }
                failures.append(failure_entry)

        overall_success = len(model_reports) > 0
        result = {
            'success': overall_success,
            'items_analyzed': len(posts) if overall_success else 0,
            'model_reports': model_reports,
            'failures': failures,
            'report_type': 'deep'
        }

        if overall_success:
            primary_report = model_reports[0]
            result['report_id'] = primary_report['report_id']
            result['title'] = primary_report['report_title']
            result['notion_push'] = primary_report.get('notion_push')
            result['report_ids'] = [mr['report_id'] for mr in model_reports]

        self.logger.info(
            f"深度洞察生成完成: 成功生成 {len(model_reports)} 份报告，失败 {len(failures)} 份"
        )

        return result

    async def run_dual_report_generation(self, hours_back: Optional[int] = None) -> Dict[str, Any]:
        """运行双轨制报告生成流程（总调度方法）

        阶段1: 生成所有日报资讯
        阶段2: 生成所有深度洞察
        """
        self.logger.info("开始执行双轨制报告生成流程")

        # 阶段1: 日报资讯（使用light模式）
        self.logger.info("===== 阶段1: 生成日报资讯 =====")
        light_result = await self.generate_light_reports(hours_back=hours_back)

        # 阶段2: 深度洞察（使用full模式）
        self.logger.info("===== 阶段2: 生成深度洞察 =====")
        deep_result = await self.generate_deep_reports(hours_back=hours_back)

        # 汇总统计
        light_success_count = len(light_result.get('model_reports', []))
        deep_success_count = len(deep_result.get('model_reports', []))
        total_success = light_success_count + deep_success_count

        light_fail_count = len(light_result.get('failures', []))
        deep_fail_count = len(deep_result.get('failures', []))
        total_fail = light_fail_count + deep_fail_count

        overall_success = total_success > 0

        result = {
            'success': overall_success,
            'light_reports': light_result,
            'deep_reports': deep_result,
            'items_analyzed': light_result.get('items_analyzed', 0),
            'light_reports_count': light_success_count,
            'deep_reports_count': deep_success_count,
            'total_reports_count': total_success,
            'message': f"双轨制报告生成完成: 日报资讯 {light_success_count} 份，深度洞察 {deep_success_count} 份，失败 {total_fail} 份"
        }

        self.logger.info(result['message'])

        return result

    async def generate_daily_hotspot(self, hours_back: Optional[int] = None) -> Dict[str, Any]:
        hours = int(hours_back or self.analysis_cfg.get('hours_back_daily', 24))
        end_time = self._bj_time()
        start_time = end_time - timedelta(hours=hours)

        posts = self.db.get_recent_posts(hours_back=hours)
        if not posts:
            return {
                'success': False,
                'error': f'最近{hours}小时内无新增动态',
            }

        content_md, sources = self._format_posts_for_llm(posts, source_prefix='T')
        prompt = self._prompt_daily()

        # 获取要使用的模型列表
        models_to_generate = self._get_report_models()
        if not models_to_generate:
            self.logger.warning("未配置任何可用于生成报告的模型")
            return {
                'success': False,
                'error': '未配置可用的LLM模型',
                'items_analyzed': 0
            }

        model_reports: List[Dict[str, Any]] = []
        failures: List[Dict[str, Any]] = []
        tasks = []
        task_meta: List[Dict[str, str]] = []

        # 为每个模型创建并行任务
        for model_name in models_to_generate:
            display_name = self._get_model_display_name(model_name)
            task_meta.append({'model': model_name, 'display': display_name})
            tasks.append(
                self._generate_daily_report_for_model(
                    model_name=model_name,
                    display_name=display_name,
                    posts=posts,
                    content_md=content_md,
                    sources=sources,
                    prompt=prompt,
                    start_time=start_time,
                    end_time=end_time
                )
            )

        self.logger.info(
            f"开始并行生成 {len(tasks)} 份日报: {[meta['display'] for meta in task_meta]}"
        )

        # 并行执行所有任务
        task_results = await asyncio.gather(*tasks, return_exceptions=True)

        # 处理任务结果
        for meta, task_result in zip(task_meta, task_results):
            model_name = meta['model']
            display_name = meta['display']

            if isinstance(task_result, Exception):
                error_msg = str(task_result)
                self.logger.warning(
                    f"模型 {model_name} ({display_name}) 日报生成过程中出现未处理异常: {error_msg}"
                )
                failures.append({
                    'model': model_name,
                    'model_display': display_name,
                    'error': error_msg
                })
                continue

            if task_result.get('success'):
                model_reports.append(task_result)
            else:
                failure_entry = {
                    'model': model_name,
                    'model_display': display_name,
                    'error': task_result.get('error', '报告生成失败')
                }
                failures.append(failure_entry)

        # 构建最终结果
        overall_success = len(model_reports) > 0
        result = {
            'success': overall_success,
            'items_analyzed': len(posts) if overall_success else 0,
            'model_reports': model_reports,
            'failures': failures
        }

        if overall_success:
            # 使用第一个成功的报告作为主要结果
            primary_report = model_reports[0]
            result['report_id'] = primary_report['report_id']
            result['title'] = primary_report['report_title']
            result['notion_push'] = primary_report.get('notion_push')
            result['report_ids'] = [mr['report_id'] for mr in model_reports]

        self.logger.info(
            f"日报生成完成: 成功生成 {len(model_reports)} 份报告，失败 {len(failures)} 份"
        )

        return result

    async def generate_weekly_digest(self, days_back: Optional[int] = None) -> Dict[str, Any]:
        days = int(days_back or self.analysis_cfg.get('days_back_weekly', 7))
        daily_reports = self.db.get_recent_daily_reports(days=days)
        if not daily_reports:
            return {'success': False, 'error': f'最近{days}天内无可用日报'}

        content_md, sources = self._format_daily_reports_for_weekly(daily_reports)
        if not content_md:
            return {'success': False, 'error': '周报输入内容为空'}

        items_analyzed_total = sum((r.get('items_analyzed') or 0) for r in daily_reports)

        start_candidates = [r.get('analysis_period_start') for r in daily_reports if isinstance(r.get('analysis_period_start'), datetime)]
        end_candidates = [r.get('analysis_period_end') for r in daily_reports if isinstance(r.get('analysis_period_end'), datetime)]

        if start_candidates:
            start_time = min(start_candidates)
        else:
            start_time = self._bj_time() - timedelta(days=days)

        if end_candidates:
            end_time = max(end_candidates)
        else:
            end_time = self._bj_time()

        # 获取要使用的模型列表
        models_to_generate = self._get_report_models()
        if not models_to_generate:
            self.logger.warning("未配置任何可用于生成报告的模型")
            return {
                'success': False,
                'error': '未配置可用的LLM模型',
                'items_analyzed': 0
            }

        model_reports: List[Dict[str, Any]] = []
        failures: List[Dict[str, Any]] = []

        # 为每个模型创建并行任务
        tasks = []
        task_meta: List[Dict[str, str]] = []

        for model_name in models_to_generate:
            display_name = self._get_model_display_name(model_name)
            task_meta.append({'model': model_name, 'display': display_name})
            tasks.append(
                self._generate_weekly_report_for_model(
                    model_name=model_name,
                    display_name=display_name,
                    daily_reports=daily_reports,
                    content_md=content_md,
                    sources=sources,
                    start_time=start_time,
                    end_time=end_time,
                    items_analyzed=items_analyzed_total
                )
            )

        self.logger.info(
            f"开始并行生成 {len(tasks)} 份周报: {[meta['display'] for meta in task_meta]}"
        )

        # 并行执行所有任务
        task_results = await asyncio.gather(*tasks, return_exceptions=True)

        # 处理任务结果
        for meta, task_result in zip(task_meta, task_results):
            model_name = meta['model']
            display_name = meta['display']

            if isinstance(task_result, Exception):
                error_msg = str(task_result)
                self.logger.warning(
                    f"模型 {model_name} ({display_name}) 周报生成过程中出现未处理异常: {error_msg}"
                )
                failures.append({
                    'model': model_name,
                    'model_display': display_name,
                    'error': error_msg
                })
                continue

            if task_result.get('success'):
                model_reports.append(task_result)
            else:
                failure_entry = {
                    'model': model_name,
                    'model_display': display_name,
                    'error': task_result.get('error', '报告生成失败')
                }
                failures.append(failure_entry)

        # 构建最终结果
        overall_success = len(model_reports) > 0
        result = {
            'success': overall_success,
            'items_analyzed': items_analyzed_total if overall_success else 0,
            'model_reports': model_reports,
            'failures': failures
        }

        if overall_success:
            # 使用第一个成功的报告作为主要结果
            primary_report = model_reports[0]
            result['report_id'] = primary_report['report_id']
            result['title'] = primary_report['report_title']
            result['notion_push'] = primary_report.get('notion_push')
            result['report_ids'] = [mr['report_id'] for mr in model_reports]

        return result

    async def generate_quarterly_narrative(self, days_back: Optional[int] = None) -> Dict[str, Any]:
        days = int(days_back or self.analysis_cfg.get('days_back_quarterly', 90))
        end_time = self._bj_time()
        start_time = end_time - timedelta(days=days)

        posts = self.db.get_posts_for_analysis(days=days)
        if not posts:
            return {'success': False, 'error': f'最近{days}天内无动态可分析'}

        # 复用周报提示词，实际可更复杂
        content_md, sources = self._format_posts_for_llm(posts, source_prefix='T')

        # 获取要使用的模型列表
        models_to_generate = self._get_report_models()
        if not models_to_generate:
            self.logger.warning("未配置任何可用于生成报告的模型")
            return {
                'success': False,
                'error': '未配置可用的LLM模型',
                'items_analyzed': 0
            }

        model_reports: List[Dict[str, Any]] = []
        failures: List[Dict[str, Any]] = []
        tasks = []
        task_meta: List[Dict[str, str]] = []

        # 为每个模型创建并行任务
        for model_name in models_to_generate:
            display_name = self._get_model_display_name(model_name)
            task_meta.append({'model': model_name, 'display': display_name})
            tasks.append(
                self._generate_quarterly_report_for_model(
                    model_name=model_name,
                    display_name=display_name,
                    posts=posts,
                    content_md=content_md,
                    sources=sources,
                    start_time=start_time,
                    end_time=end_time
                )
            )

        self.logger.info(
            f"开始并行生成 {len(tasks)} 份季报: {[meta['display'] for meta in task_meta]}"
        )

        # 并行执行所有任务
        task_results = await asyncio.gather(*tasks, return_exceptions=True)

        # 处理任务结果
        for meta, task_result in zip(task_meta, task_results):
            model_name = meta['model']
            display_name = meta['display']

            if isinstance(task_result, Exception):
                error_msg = str(task_result)
                self.logger.warning(
                    f"模型 {model_name} ({display_name}) 季报生成过程中出现未处理异常: {error_msg}"
                )
                failures.append({
                    'model': model_name,
                    'model_display': display_name,
                    'error': error_msg
                })
                continue

            if task_result.get('success'):
                model_reports.append(task_result)
            else:
                failure_entry = {
                    'model': model_name,
                    'model_display': display_name,
                    'error': task_result.get('error', '报告生成失败')
                }
                failures.append(failure_entry)

        # 构建最终结果
        overall_success = len(model_reports) > 0
        result = {
            'success': overall_success,
            'items_analyzed': len(posts) if overall_success else 0,
            'model_reports': model_reports,
            'failures': failures
        }

        if overall_success:
            # 使用第一个成功的报告作为主要结果
            primary_report = model_reports[0]
            result['report_id'] = primary_report['report_id']
            result['title'] = primary_report['report_title']
            result['notion_push'] = primary_report.get('notion_push')
            result['report_ids'] = [mr['report_id'] for mr in model_reports]

        self.logger.info(
            f"季报生成完成: 成功生成 {len(model_reports)} 份报告，失败 {len(failures)} 份"
        )

        return result

    async def _generate_quarterly_report_for_model(
        self,
        *,
        model_name: str,
        display_name: str,
        posts: List[Dict[str, Any]],
        content_md: str,
        sources: List[Dict[str, Any]],
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """在独立线程中生成指定模型的季报"""
        return await asyncio.to_thread(
            self._generate_quarterly_report_for_model_sync,
            model_name,
            display_name,
            posts,
            content_md,
            sources,
            start_time,
            end_time
        )

    def _generate_quarterly_report_for_model_sync(
        self,
        model_name: str,
        display_name: str,
        posts: List[Dict[str, Any]],
        content_md: str,
        sources: List[Dict[str, Any]],
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """同步执行指定模型的季报生成和Notion推送"""

        self.logger.info(f"[{display_name}] 模型线程启动，开始生成季报")

        llm_analysis_result = self._analyze_with_llm(content_md, self._prompt_weekly(), model_override=model_name)

        if not llm_analysis_result:
            error_msg = "LLM分析失败，未生成季度报告"
            self.logger.warning(f"[{display_name}] {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'model': model_name,
                'model_display': display_name
            }

        llm_output = llm_analysis_result.get('content', '')
        # 为LLM生成的报告添加标准头部信息
        beijing_time = self._bj_time()
        q = (end_time.month - 1) // 3 + 1
        header_info = [
            f"# 🚀 即刻季度战略叙事 - {display_name} - {end_time.year} Q{q}",
            "",
            f"*报告生成时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}*  ",
            "",
            f"*数据范围: {start_time.strftime('%Y-%m-%d %H:%M:%S')} - {end_time.strftime('%Y-%m-%d %H:%M:%S')}*  ",
            "",
            f"*分析动态数: {len(posts)} 条*",
            "",
            "---",
            ""
        ]

        # 清理LLM输出中可能的格式问题
        cleaned_llm_output = self._clean_llm_output_for_notion(llm_output)

        sources_section = self._render_sources_section(sources)

        # 构建报告尾部
        footer_lines = ["", "---", ""]
        provider = llm_analysis_result.get('provider')
        model = llm_analysis_result.get('model')
        if provider:
            footer_lines.append(f"*分析引擎: {provider} ({model or 'unknown'})*")

        footer_lines.extend([
            "",
            "*本报告由AI自动生成，仅供参考*"
        ])
        footer_section = "\n".join(footer_lines)

        report_content = "\n".join(header_info) + cleaned_llm_output + "\n\n" + sources_section + footer_section

        # 应用来源链接增强后处理
        report_content = self._enhance_source_links(report_content, sources)

        # 简单季度标题
        q = (end_time.month - 1) // 3 + 1
        title = f"即刻季度战略叙事 - {display_name} - {end_time.year} Q{q}"
        report_row = {
            'report_type': 'quarterly_narrative',
            'scope': 'global',
            'analysis_period_start': start_time,
            'analysis_period_end': end_time,
            'items_analyzed': len(posts),
            'report_title': title,
            'report_content': report_content,
        }
        report_id = self.db.save_report(report_row)

        model_report = {
            'model': model_name,
            'model_display': display_name,
            'success': True,
            'report_id': report_id,
            'report_title': title,
            'provider': llm_analysis_result.get('provider') if llm_analysis_result else None,
            'items_analyzed': len(posts)
        }

        # 尝试推送到Notion
        notion_push_info = None
        try:
            from .notion_client import jike_notion_client

            # 格式化Notion标题
            beijing_time = self._bj_time()
            notion_title = f"[{display_name}] 即刻季度战略叙事 - {end_time.year}Q{q} ({len(posts)}条动态)"

            self.logger.info(f"开始推送季报到Notion ({display_name}): {notion_title}")

            notion_result = jike_notion_client.create_report_page(
                report_title=notion_title,
                report_content=report_content,
                report_date=beijing_time
            )

            if notion_result.get('success'):
                self.logger.info(f"季报成功推送到Notion ({display_name}): {notion_result.get('page_url')}")
                notion_push_info = {
                    'success': True,
                    'page_url': notion_result.get('page_url'),
                    'path': notion_result.get('path')
                }
            else:
                error_msg = notion_result.get('error', '未知错误')
                self.logger.warning(f"推送季报到Notion失败 ({display_name}): {error_msg}")
                notion_push_info = {
                    'success': False,
                    'error': error_msg
                }

        except Exception as e:
            self.logger.warning(f"推送季报到Notion时出错 ({display_name}): {e}")
            notion_push_info = {
                'success': False,
                'error': str(e)
            }

        if notion_push_info:
            model_report['notion_push'] = notion_push_info

        return model_report

    async def generate_kol_trajectory(self, kol_ids: Optional[List[str]] = None, days_back: Optional[int] = None) -> Dict[str, Any]:
        """为多个KOL生成按人维度的思想轨迹图（支持多模型并发处理）。返回统计结果。"""
        ids = kol_ids or self.analysis_cfg.get('kol_user_ids') or []
        days = int(days_back or self.analysis_cfg.get('days_back_kol', 30))
        if not ids:
            return {'success': False, 'error': '未提供KOL用户ID列表'}

        end_time_global = self._bj_time()
        start_time_global = end_time_global - timedelta(days=days)

        # 获取要使用的模型列表
        models_to_generate = self._get_report_models()
        if not models_to_generate:
            self.logger.warning("未配置任何可用于生成报告的模型")
            return {
                'success': False,
                'error': '未配置可用的LLM模型',
                'kol_reports': [],
                'total_generated': 0,
                'total_failed': 0
            }

        self.logger.info(f"开始为 {len(ids)} 个KOL生成思想轨迹图，使用模型: {[self._get_model_display_name(m) for m in models_to_generate]}")

        kol_reports = []
        total_generated = 0
        total_failed = 0

        # 为每个KOL创建并行任务
        tasks = []
        task_meta = []

        for kol_id in ids:
            task_meta.append({'kol_id': kol_id})
            tasks.append(
                self._generate_kol_trajectory_for_user(
                    kol_id=kol_id,
                    models_to_generate=models_to_generate,
                    days=days,
                    start_time=start_time_global,
                    end_time=end_time_global
                )
            )

        self.logger.info(f"开始并行生成 {len(tasks)} 个KOL的报告")

        # 并行执行所有KOL任务
        task_results = await asyncio.gather(*tasks, return_exceptions=True)

        # 处理任务结果
        for meta, task_result in zip(task_meta, task_results):
            kol_id = meta['kol_id']

            if isinstance(task_result, Exception):
                error_msg = str(task_result)
                self.logger.warning(f"KOL {kol_id} 报告生成过程中出现未处理异常: {error_msg}")
                kol_reports.append({
                    'kol_id': kol_id,
                    'success': False,
                    'error': error_msg,
                    'model_reports': [],
                    'failures': [{'error': error_msg}]
                })
                total_failed += 1
                continue

            if task_result.get('success'):
                kol_reports.append(task_result)
                total_generated += len(task_result.get('model_reports', []))
            else:
                kol_reports.append(task_result)
                total_failed += 1

        self.logger.info(f"KOL报告生成完成: 成功生成 {total_generated} 份报告，失败 {total_failed} 份")

        return {
            'success': True,
            'kol_reports': kol_reports,
            'total_generated': total_generated,
            'total_failed': total_failed,
            'models_used': [self._get_model_display_name(m) for m in models_to_generate]
        }

    async def _generate_kol_trajectory_for_user(
        self,
        *,
        kol_id: str,
        models_to_generate: List[str],
        days: int,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """为单个KOL生成多模型报告"""
        try:
            posts = self.db.get_user_posts_for_analysis(jike_user_id=kol_id, days=days)
            if not posts:
                self.logger.info(f"KOL {kol_id} 无素材，跳过")
                return {
                    'kol_id': kol_id,
                    'success': False,
                    'error': 'KOL无素材数据',
                    'model_reports': [],
                    'failures': []
                }

            content_md, sources = self._format_posts_for_llm(posts, source_prefix='T')

            model_reports = []
            failures = []
            tasks = []
            task_meta = []

            # 为每个模型创建并行任务
            for model_name in models_to_generate:
                display_name = self._get_model_display_name(model_name)
                task_meta.append({'model': model_name, 'display': display_name})
                tasks.append(
                    self._generate_kol_report_for_model(
                        kol_id=kol_id,
                        model_name=model_name,
                        display_name=display_name,
                        posts=posts,
                        content_md=content_md,
                        sources=sources,
                        start_time=start_time,
                        end_time=end_time
                    )
                )

            # 并行执行所有模型任务
            task_results = await asyncio.gather(*tasks, return_exceptions=True)

            # 处理任务结果
            for meta, task_result in zip(task_meta, task_results):
                model_name = meta['model']
                display_name = meta['display']

                if isinstance(task_result, Exception):
                    error_msg = str(task_result)
                    self.logger.warning(f"KOL {kol_id} 模型 {model_name} ({display_name}) 生成过程中出现异常: {error_msg}")
                    failures.append({
                        'model': model_name,
                        'model_display': display_name,
                        'error': error_msg
                    })
                    continue

                if task_result.get('success'):
                    model_reports.append(task_result)
                else:
                    failure_entry = {
                        'model': model_name,
                        'model_display': display_name,
                        'error': task_result.get('error', '报告生成失败')
                    }
                    failures.append(failure_entry)

            overall_success = len(model_reports) > 0
            result = {
                'kol_id': kol_id,
                'success': overall_success,
                'items_analyzed': len(posts) if overall_success else 0,
                'model_reports': model_reports,
                'failures': failures
            }

            if overall_success:
                # 使用第一个成功的报告作为主要结果
                primary_report = model_reports[0]
                result['primary_report_id'] = primary_report['report_id']
                result['primary_title'] = primary_report['report_title']
                result['report_ids'] = [mr['report_id'] for mr in model_reports]

            return result

        except Exception as e:
            error_msg = f"KOL {kol_id} 报告生成失败: {str(e)}"
            self.logger.error(error_msg)
            return {
                'kol_id': kol_id,
                'success': False,
                'error': error_msg,
                'model_reports': [],
                'failures': [{'error': error_msg}]
            }

    async def _generate_kol_report_for_model(
        self,
        *,
        kol_id: str,
        model_name: str,
        display_name: str,
        posts: List[Dict[str, Any]],
        content_md: str,
        sources: List[Dict[str, Any]],
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """在独立线程中为指定模型生成KOL报告"""
        return await asyncio.to_thread(
            self._generate_kol_report_for_model_sync,
            kol_id,
            model_name,
            display_name,
            posts,
            content_md,
            sources,
            start_time,
            end_time
        )

    def _generate_kol_report_for_model_sync(
        self,
        kol_id: str,
        model_name: str,
        display_name: str,
        posts: List[Dict[str, Any]],
        content_md: str,
        sources: List[Dict[str, Any]],
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """同步执行指定模型的KOL报告生成和Notion推送"""

        self.logger.info(f"[{display_name}] 开始为KOL {kol_id} 生成思想轨迹图")

        # 复用周报提示词
        llm_analysis_result = self._analyze_with_llm(content_md, self._prompt_weekly(), model_override=model_name)

        if not llm_analysis_result:
            error_msg = f"LLM分析失败，未生成KOL报告 ({kol_id})"
            self.logger.warning(f"[{display_name}] {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'model': model_name,
                'model_display': display_name,
                'kol_id': kol_id
            }

        llm_output = llm_analysis_result.get('content', '')
        # 为LLM生成的报告添加标准头部信息
        beijing_time = self._bj_time()
        header_info = [
            f"# 🎯 即刻KOL思想轨迹 - {display_name} - {kol_id}",
            "",
            f"*报告生成时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}*  ",
            "",
            f"*数据范围: {start_time.strftime('%Y-%m-%d %H:%M:%S')} - {end_time.strftime('%Y-%m-%d %H:%M:%S')}*  ",
            "",
            f"*分析动态数: {len(posts)} 条*",
            "",
            "---",
            ""
        ]

        # 清理LLM输出中可能的格式问题
        cleaned_llm_output = self._clean_llm_output_for_notion(llm_output)

        sources_section = self._render_sources_section(sources)

        # 构建报告尾部
        footer_lines = ["", "---", ""]
        provider = llm_analysis_result.get('provider')
        model = llm_analysis_result.get('model')
        if provider:
            footer_lines.append(f"*分析引擎: {provider} ({model or 'unknown'})*")

        footer_lines.extend([
            "",
            "*本报告由AI自动生成，仅供参考*"
        ])
        footer_section = "\n".join(footer_lines)

        report_content = "\n".join(header_info) + cleaned_llm_output + "\n\n" + sources_section + footer_section

        # 应用来源链接增强后处理
        report_content = self._enhance_source_links(report_content, sources)

        title = f"KOL思想轨迹 - {display_name} - {kol_id} - 截止 {end_time.strftime('%Y-%m-%d')}"
        report_row = {
            'report_type': 'kol_trajectory',
            'scope': f'kol:{kol_id}',
            'analysis_period_start': start_time,
            'analysis_period_end': end_time,
            'items_analyzed': len(posts),
            'report_title': title,
            'report_content': report_content,
        }
        report_id = self.db.save_report(report_row)

        model_report = {
            'model': model_name,
            'model_display': display_name,
            'success': True,
            'report_id': report_id,
            'report_title': title,
            'provider': llm_analysis_result.get('provider') if llm_analysis_result else None,
            'items_analyzed': len(posts),
            'kol_id': kol_id
        }

        # 尝试推送KOL报告到Notion
        notion_push_info = None
        try:
            from .notion_client import jike_notion_client

            # 格式化Notion标题
            beijing_time = self._bj_time()
            notion_title = f"[{display_name}] KOL思想轨迹 - {kol_id} - {beijing_time.strftime('%Y%m%d')} ({len(posts)}条动态)"

            self.logger.info(f"开始推送KOL报告到Notion ({display_name}): {notion_title}")

            notion_result = jike_notion_client.create_report_page(
                report_title=notion_title,
                report_content=report_content,
                report_date=beijing_time
            )

            if notion_result.get('success'):
                self.logger.info(f"KOL报告成功推送到Notion ({display_name}): {notion_result.get('page_url')}")
                notion_push_info = {
                    'success': True,
                    'page_url': notion_result.get('page_url'),
                    'path': notion_result.get('path')
                }
            else:
                error_msg = notion_result.get('error', '未知错误')
                self.logger.warning(f"推送KOL报告到Notion失败 ({display_name}): {error_msg}")
                notion_push_info = {
                    'success': False,
                    'error': error_msg
                }

        except Exception as e:
            self.logger.warning(f"推送KOL报告到Notion时出错 ({display_name}): {e}")
            notion_push_info = {
                'success': False,
                'error': str(e)
            }

        if notion_push_info:
            model_report['notion_push'] = notion_push_info

        return model_report


def get_report_generator() -> JKReportGenerator:
    """模块级工厂函数，供tasks延迟导入调用"""
    return JKReportGenerator()


# ===== 便捷函数，供tasks.py调用 =====

def run_light_reports(hours: Optional[int] = None) -> Dict[str, Any]:
    """生成日报资讯的便捷函数"""
    rg = get_report_generator()
    return asyncio.run(rg.generate_light_reports(hours_back=hours))


def run_deep_reports(hours: Optional[int] = None) -> Dict[str, Any]:
    """生成热点追踪的便捷函数"""
    rg = get_report_generator()
    return asyncio.run(rg.generate_deep_reports(hours_back=hours))


def run_dual_reports(hours: Optional[int] = None) -> Dict[str, Any]:
    """运行双轨制报告的便捷函数"""
    rg = get_report_generator()
    return asyncio.run(rg.run_dual_report_generation(hours_back=hours))
