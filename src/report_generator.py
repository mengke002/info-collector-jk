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
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

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

    def _bj_time(self) -> datetime:
        return datetime.now(timezone.utc) + timedelta(hours=8)

    # ---------- 数据准备与格式化 ----------
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
        """将帖子格式化为带编号的Markdown文本，包含原始内容和解读信息，返回(文本, 源映射列表)"""
        lines: List[str] = []
        sources: List[Dict[str, Any]] = []
        total_chars = 0

        for idx, p in enumerate(posts, 1):
            sid = f"{source_prefix}{idx}"
            nickname = p.get('nickname') or p.get('jike_user_id') or '未知作者'
            link = p.get('link') or ''
            title = p.get('title') or ''
            summary = p.get('summary') or ''
            pub = p.get('published_at')
            pub_str = pub.strftime('%Y-%m-%d %H:%M') if pub else ''

            # 获取解读信息
            interpretation_text = p.get('interpretation_text') or ''
            interpretation_model = p.get('interpretation_model') or ''

            # 每条摘要截断,避免单条过长
            title_t = self._truncate(title, 140)
            summary_t = self._truncate(summary, 1500)

            # 解读内容截断
            interpretation_t = self._truncate(interpretation_text, 3000) if interpretation_text else ''

            # 构建帖子块
            block = [
                f"### [{sid}] {title_t}",
                f"- 作者: {nickname}",
                f"- 时间: {pub_str}",
                f"- 链接: {link}",
                f"- 原始内容:\n{summary_t}"
            ]

            # 如果有解读内容，添加解读部分
            if interpretation_text:
                block.extend([
                    f"- AI深度解读 (模型: {interpretation_model}):\n{interpretation_t}"
                ])
            else:
                block.extend([
                    "- AI深度解读: 暂无"
                ])

            block.append("")  # 空行分隔

            block_text = "\n".join(block)
            if total_chars + len(block_text) > self.max_content_length:
                self.logger.info(f"达到最大内容限制({self.max_content_length}),截断帖子列表于第 {idx-1} 条")
                break
            lines.append(block_text)
            total_chars += len(block_text)
            sources.append({
                'sid': sid,
                'title': title_t,
                'link': link,
                'nickname': nickname,
                'excerpt': self._truncate(summary, 120)
            })

        return "\n".join(lines), sources

    def _render_sources_section(self, sources: List[Dict[str, Any]]) -> str:
        lines = ["## 📚 来源清单 (Source List)", ""]
        for s in sources:
            # 清理标题中的方括号，避免与Markdown链接冲突
            clean_title = (s['title'] or s['excerpt']).replace('[', '【').replace(']', '】')
            lines.append(f"- **【{s['sid']}】**: [@{s['nickname']}]({s['link']}): {clean_title}")
        return "\n".join(lines)

    # ---------- Prompt 模板 ----------
    def _prompt_daily(self) -> str:
        return """# Role: 资深社区战略分析师

# Context:
你正在分析一个由技术专家、产品经理、投资人和创业者组成的精英社区——'即刻'在过去24小时内发布的帖子。你的任务是基于我提供的、已编号的原始讨论材料和AI深度解读，撰写一份信息密度高、内容详尽、可读性强的情报简报。

# Core Principles:
1.  **价值导向与深度优先**: 你的核心目标是挖掘出对从业者有直接价值的信息。在撰写每个部分时，都应追求内容的**深度和完整性**，**避免过于简短的概括**。
2.  **忠于原文与可追溯性 (CRITICAL)**: 所有分析都必须基于原文，并且每一条结论都必须在句末使用 `[Source: T_n]` 或 `[Sources: T_n, T_m]` 的格式明确标注来源。这是硬性要求,绝对不能遗漏。
3.  **识别帖子类型**: 在分析时，请注意识别每个主题的潜在类型，例如：`[AI/前沿技术]`, `[产品与设计]`, `[创业与投资]`, `[个人成长与思考]`, `[行业与市场动态]`, `[工具与工作流分享]`等。这有助于你判断其核心价值。

---

# Input Data:
以下是即刻社区的帖子数据，每条帖子包含原始内容和AI深度解读（如有）。请综合利用原始内容和AI解读信息进行分析：
# 帖子数据 (原始内容 + AI解读，已编号):
{content}

---

# Your Task:
请严格按照以下结构和要求，生成一份内容丰富详实的完整Markdown报告。

**第一部分：本时段焦点速报 (Top Topics Overview)**
*   任务：通读所有材料，为每个值得关注的热门主题撰写一份**详细摘要**。
*   要求：不仅要总结主题的核心内容，还要**尽可能列出主要的讨论方向和关键回复的观点**。篇幅无需严格限制，力求全面。

**第二部分：核心洞察与趋势 (Executive Summary & Trends)**
*   任务：基于第一部分的所有信息，从全局视角提炼出关键洞察与趋势。
*   要求：
    *   **核心洞察**: **尽可能全面地**提炼你发现的重要趋势或洞察，并详细阐述，**不要局限于少数几点**。
    *   **技术风向与工具箱**: **详细列出并介绍**被热议的新技术、新框架或工具。对于每个项目，请提供更详尽的描述，包括其用途、优点、以及社区讨论中的具体评价。
    *   **社区热议与需求点**: **详细展开**社区普遍关心的话题、遇到的痛点或潜在的需求，说明其背景、当前讨论的焦点以及潜在的影响。

**第三部分：价值信息挖掘 (Valuable Information Mining)**
*   任务：深入挖掘帖子和回复中的高价值信息，并进行详细介绍。
*   要求：
    *   **高价值资源/工具**: **详细列出并介绍**讨论中出现的可以直接使用的软件、库、API、开源项目或学习资料。包括资源的链接（如果原文提供）、用途和社区评价。
    *   **有趣观点/深度讨论**: **详细阐述**那些引人深思、具有启发性的个人观点或高质量的讨论串。分析该观点为何重要或具有启发性，以及它引发了哪些后续讨论。

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
*   **详细摘要**: [详细摘要该主题的核心内容，并列出主要的讨论方向和关键回复的观点。篇幅无需严格限制，力求全面。] [Source: T_n]

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
    *   **[热议话题A]**: [详细展开一个被广泛讨论的话题，例如“大模型在特定场景下的落地成本”，包括讨论的背景、各方观点、争议点以及对未来的展望。] [Source: T5]
    *   **[普遍需求B]**: [详细总结一个普遍存在的需求，例如“需要更稳定、更便宜的GPU算力资源”，并分析该需求产生的原因和社区提出的潜在解决方案。] [Source: T10]
    *   ...(尽可能多地列出话题/需求)

---

## 三、价值信息挖掘

*   **高价值资源/工具**:
    *   **[资源/工具A]**: [详细介绍该资源/工具，包括其名称、功能、优点、潜在缺点以及社区成员分享的使用技巧或经验。例如：`XX-Agent-Framework` - 一个用于快速构建AI Agent的开源框架，社区反馈其优点是上手快、文档全，但缺点是...。] [Source: T2]
    *   **[资源/工具B]**: [同上。] [Source: T8]
    *   ...(尽可能多地列出资源/工具)

*   **有趣观点/深度讨论**:
    *   **[关于“XX”的观点]**: [详细阐述一个有启发性的观点，分析其重要性，并总结因此引发的精彩后续讨论。例如：有用户认为，当前阶段的AI应用开发，工程化能力比算法创新更重要。这一观点引发了关于“算法工程师”与“AI应用工程师”职责边界的大量讨论，主流看法是...] [Source: T4]
    *   **[关于“YY”的讨论]**: [同上。] [Source: T6]
    *   ...(尽可能多地列出观点/讨论)

---

## 四、行动建议

*   **给产品经理的建议**:
    *   [建议1：[提出具体建议]。理由与预期效果：[阐述该建议的逻辑依据，以及采纳后可能带来的好处]。例如：建议关注社区中关于“用户体验断点”的讨论，这可能是下一个产品创新的切入点。] [Sources: T2, T9]
    *   [建议2：...]

*   **给创业者/投资者的建议**:
    *   [建议1. [提出具体建议]。理由与预期效果：[阐述该建议的逻辑依据，以及采纳后可能带来的好处]。例如：社区对“小模型”的兴趣正在升温，这可能意味着在特定垂直领域存在新的创业机会。] [Source: T1]
    *   [建议2：...]

*   **给技术从业者的建议**:
    *   [建议1：[提出具体建议]。理由与预期效果：[阐述该建议的逻辑依据，以及采纳后可能带来的好处]。例如：建议深入学习社区热议的 `XXX` 框架，掌握后能显著提升项目开发能力和求职竞争力。] [Source: T3]
    *   [建议2：...]
"""

    def _prompt_weekly(self) -> str:
        return (
            "# Role: 资深社群战略顾问\n"
            "\n"
            "# Context:\n"
            "你正在为一份高端内参,分析一个由技术专家、产品经理和创业者组成的精英社区在过去一周的全部讨论。你的任务是复盘社区焦点,洞察趋势,并给出战略性预判。\n"
            "\n"
            "# Core Principles:\n"
            "1. 所有要点需结合编号来源标注 [Source: T_n] 或 [Sources: T_a, T_b]。\n"
            "2. 注重变化与趋势，而不仅是信息罗列。\n"
            "3. 关注社区结构化分层：技术/产品/创业/投资/工具/行业/文化等。\n"
            "\n"
            "# Input Data (已编号帖子)：\n\n{content}\n\n"
            "# Your Task:\n"
            "请按如下结构输出一份Markdown周报：\n"
            "## 一、关键主题回顾 (Top Topics)\n"
            "- 用3-5条总结本周最受关注的话题及结论。[Sources: ...]\n"
            "\n"
            "## 二、重要洞察与趋势 (Insights & Trends)\n"
            "- 提炼2-3条跨主题洞察，说明其成因与影响。[Sources: ...]\n"
            "- 列示本周值得关注的新技术/新工具及社区评价。[Sources: ...]\n"
            "\n"
            "## 三、结构化分析 (Deep Dive)\n"
            "- 从供给侧/需求侧/生态位/路径依赖等视角进行深挖。[Sources: ...]\n"
            "\n"
            "## 四、面向角色的建议 (Actionables)\n"
            "- 给产品经理/开发者/创业者/投资者各1-2条可执行建议。[Sources: ...]\n"
        )

    # ---------- 报告生成 ----------
    def _analyze_with_llm(self, content: str, prompt_template: str) -> Optional[Dict[str, Any]]:
        """调用智能模型进行深度分析，失败时返回None"""
        try:
            if llm_client is None:
                return None
            # 格式化提示词
            prompt = prompt_template.format(content=content)
            # 使用智能模型进行复杂报告生成任务
            res = llm_client.call_smart_model(prompt)
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
        return "\n".join(lines)

    def generate_daily_hotspot(self, hours_back: Optional[int] = None) -> Dict[str, Any]:
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
        llm_analysis_result = self._analyze_with_llm(content_md, prompt)

        if not llm_analysis_result:
            header = "# 即刻24小时热点追踪器 (占位版)"
            report_content = self._make_fallback_report(header, posts, start_time, end_time, sources)
        else:
            llm_output = llm_analysis_result.get('content', '')
            # 为LLM生成的报告添加标准头部信息
            beijing_time = self._bj_time()
            header_info = [
                f"# 📈 即刻24小时热点追踪器",
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

        title = f"即刻24h热点观察 - {end_time.strftime('%Y-%m-%d %H:%M')}"
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

        result = {
            'success': True,
            'report_id': report_id,
            'items_analyzed': len(posts),
            'title': title,
        }

        # 尝试推送到Notion
        try:
            from .notion_client import jike_notion_client

            # 格式化Notion标题
            beijing_time = self._bj_time()
            time_str = beijing_time.strftime('%H:%M')
            notion_title = f"[{time_str}] 即刻24h热点观察 ({len(posts)}条动态)"

            self.logger.info(f"开始推送日报到Notion: {notion_title}")

            notion_result = jike_notion_client.create_report_page(
                report_title=notion_title,
                report_content=report_content,
                report_date=beijing_time
            )

            if notion_result.get('success'):
                self.logger.info(f"日报成功推送到Notion: {notion_result.get('page_url')}")
                result['notion_push'] = {
                    'success': True,
                    'page_url': notion_result.get('page_url'),
                    'path': notion_result.get('path')
                }
            else:
                self.logger.warning(f"推送日报到Notion失败: {notion_result.get('error')}")
                result['notion_push'] = {
                    'success': False,
                    'error': notion_result.get('error')
                }

        except Exception as e:
            self.logger.warning(f"推送日报到Notion时出错: {e}")
            result['notion_push'] = {
                'success': False,
                'error': str(e)
            }

        return result

    def generate_weekly_digest(self, days_back: Optional[int] = None) -> Dict[str, Any]:
        days = int(days_back or self.analysis_cfg.get('days_back_weekly', 7))
        end_time = self._bj_time()
        start_time = end_time - timedelta(days=days)

        posts = self.db.get_posts_for_analysis(days=days)
        if not posts:
            return {'success': False, 'error': f'最近{days}天内无动态可分析'}

        content_md, sources = self._format_posts_for_llm(posts, source_prefix='T')
        llm_analysis_result = self._analyze_with_llm(content_md, self._prompt_weekly())
        if not llm_analysis_result:
            header = "# 即刻周度社群洞察 (占位版)"
            report_content = self._make_fallback_report(header, posts, start_time, end_time, sources)
        else:
            llm_output = llm_analysis_result.get('content', '')
            # 为LLM生成的报告添加标准头部信息
            beijing_time = self._bj_time()
            header_info = [
                f"# 📊 即刻周度社群洞察",
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

        title = f"即刻周度社群洞察 - 截止 {end_time.strftime('%Y-%m-%d')}"
        report_row = {
            'report_type': 'weekly_digest',
            'scope': 'global',
            'analysis_period_start': start_time,
            'analysis_period_end': end_time,
            'items_analyzed': len(posts),
            'report_title': title,
            'report_content': report_content,
        }
        report_id = self.db.save_report(report_row)

        result = {
            'success': True,
            'report_id': report_id,
            'items_analyzed': len(posts),
            'title': title,
        }

        # 尝试推送到Notion
        try:
            from .notion_client import jike_notion_client

            # 格式化Notion标题
            beijing_time = self._bj_time()
            notion_title = f"即刻周度社群洞察 - {beijing_time.strftime('%Y%m%d')} ({len(posts)}条动态)"

            self.logger.info(f"开始推送周报到Notion: {notion_title}")

            notion_result = jike_notion_client.create_report_page(
                report_title=notion_title,
                report_content=report_content,
                report_date=beijing_time
            )

            if notion_result.get('success'):
                self.logger.info(f"周报成功推送到Notion: {notion_result.get('page_url')}")
                result['notion_push'] = {
                    'success': True,
                    'page_url': notion_result.get('page_url'),
                    'path': notion_result.get('path')
                }
            else:
                self.logger.warning(f"推送周报到Notion失败: {notion_result.get('error')}")
                result['notion_push'] = {
                    'success': False,
                    'error': notion_result.get('error')
                }

        except Exception as e:
            self.logger.warning(f"推送周报到Notion时出错: {e}")
            result['notion_push'] = {
                'success': False,
                'error': str(e)
            }

        return result

    def generate_quarterly_narrative(self, days_back: Optional[int] = None) -> Dict[str, Any]:
        days = int(days_back or self.analysis_cfg.get('days_back_quarterly', 90))
        end_time = self._bj_time()
        start_time = end_time - timedelta(days=days)

        posts = self.db.get_posts_for_analysis(days=days)
        if not posts:
            return {'success': False, 'error': f'最近{days}天内无动态可分析'}

        # 复用周报提示词，实际可更复杂
        content_md, sources = self._format_posts_for_llm(posts, source_prefix='T')
        llm_analysis_result = self._analyze_with_llm(content_md, self._prompt_weekly())
        if not llm_analysis_result:
            header = "# 即刻季度战略叙事 (占位版)"
            report_content = self._make_fallback_report(header, posts, start_time, end_time, sources)
        else:
            llm_output = llm_analysis_result.get('content', '')
            # 为LLM生成的报告添加标准头部信息
            beijing_time = self._bj_time()
            q = (end_time.month - 1) // 3 + 1
            header_info = [
                f"# 🚀 即刻季度战略叙事 - {end_time.year} Q{q}",
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

        # 简单季度标题
        q = (end_time.month - 1) // 3 + 1
        title = f"即刻季度战略叙事 - {end_time.year} Q{q}"
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

        result = {
            'success': True,
            'report_id': report_id,
            'items_analyzed': len(posts),
            'title': title,
        }

        # 尝试推送到Notion
        try:
            from .notion_client import jike_notion_client

            # 格式化Notion标题
            beijing_time = self._bj_time()
            notion_title = f"即刻季度战略叙事 - {end_time.year}Q{q} ({len(posts)}条动态)"

            self.logger.info(f"开始推送季报到Notion: {notion_title}")

            notion_result = jike_notion_client.create_report_page(
                report_title=notion_title,
                report_content=report_content,
                report_date=beijing_time
            )

            if notion_result.get('success'):
                self.logger.info(f"季报成功推送到Notion: {notion_result.get('page_url')}")
                result['notion_push'] = {
                    'success': True,
                    'page_url': notion_result.get('page_url'),
                    'path': notion_result.get('path')
                }
            else:
                self.logger.warning(f"推送季报到Notion失败: {notion_result.get('error')}")
                result['notion_push'] = {
                    'success': False,
                    'error': notion_result.get('error')
                }

        except Exception as e:
            self.logger.warning(f"推送季报到Notion时出错: {e}")
            result['notion_push'] = {
                'success': False,
                'error': str(e)
            }

        return result

    def generate_kol_trajectory(self, kol_ids: Optional[List[str]] = None, days_back: Optional[int] = None) -> Dict[str, Any]:
        """为多个KOL生成按人维度的思想轨迹图（并发处理）。返回统计结果。"""
        ids = kol_ids or self.analysis_cfg.get('kol_user_ids') or []
        days = int(days_back or self.analysis_cfg.get('days_back_kol', 30))
        if not ids:
            return {'success': False, 'error': '未提供KOL用户ID列表'}

        end_time_global = self._bj_time()
        start_time_global = end_time_global - timedelta(days=days)

        generated, failed = 0, 0

        def _do_one(uid: str) -> bool:
            nonlocal start_time_global, end_time_global
            posts = self.db.get_user_posts_for_analysis(jike_user_id=uid, days=days)
            if not posts:
                self.logger.info(f"KOL无素材，跳过: {uid}")
                return False
            content_md, sources = self._format_posts_for_llm(posts, source_prefix='T')
            # 暂复用周报提示词
            llm_analysis_result = self._analyze_with_llm(content_md, self._prompt_weekly())
            if not llm_analysis_result:
                header = f"# 即刻KOL思想轨迹 (占位版) - {uid}"
                report_content = self._make_fallback_report(header, posts, start_time_global, end_time_global, sources)
            else:
                llm_output = llm_analysis_result.get('content', '')
                # 为LLM生成的报告添加标准头部信息
                beijing_time = self._bj_time()
                header_info = [
                    f"# 🎯 即刻KOL思想轨迹 - {uid}",
                    "",
                    f"*报告生成时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}*  ",
                    "",
                    f"*数据范围: {start_time_global.strftime('%Y-%m-%d %H:%M:%S')} - {end_time_global.strftime('%Y-%m-%d %H:%M:%S')}*  ",
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

            title = f"KOL思想轨迹 - {uid} - 截止 {end_time_global.strftime('%Y-%m-%d')}"
            row = {
                'report_type': 'kol_trajectory',
                'scope': f'kol:{uid}',
                'analysis_period_start': start_time_global,
                'analysis_period_end': end_time_global,
                'items_analyzed': len(posts),
                'report_title': title,
                'report_content': report_content,
            }
            report_id = self.db.save_report(row)

            # 尝试推送KOL报告到Notion
            try:
                from .notion_client import jike_notion_client

                # 格式化Notion标题
                beijing_time = self._bj_time()
                notion_title = f"KOL思想轨迹 - {uid} - {beijing_time.strftime('%Y%m%d')} ({len(posts)}条动态)"

                self.logger.info(f"开始推送KOL报告到Notion: {notion_title}")

                notion_result = jike_notion_client.create_report_page(
                    report_title=notion_title,
                    report_content=report_content,
                    report_date=beijing_time
                )

                if notion_result.get('success'):
                    self.logger.info(f"KOL报告成功推送到Notion: {notion_result.get('page_url')}")
                else:
                    self.logger.warning(f"推送KOL报告到Notion失败: {notion_result.get('error')}")

            except Exception as e:
                self.logger.warning(f"推送KOL报告到Notion时出错: {e}")

            return True

        with ThreadPoolExecutor(max_workers=self.max_llm_concurrency) as ex:
            futures = {ex.submit(_do_one, uid): uid for uid in ids}
            for f in as_completed(futures):
                ok = False
                try:
                    ok = bool(f.result())
                except Exception as e:
                    self.logger.warning(f"生成KOL报告失败: {futures[f]} - {e}")
                    ok = False
                generated += 1 if ok else 0
                failed += 0 if ok else 1

        return {'success': True, 'generated': generated, 'failed': failed}


def get_report_generator() -> JKReportGenerator:
    """模块级工厂函数，供tasks延迟导入调用"""
    return JKReportGenerator()
