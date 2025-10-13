"""
即刻 Notion API 客户端
用于将分析报告推送到Notion页面
"""
import logging
import requests
import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
from .config import config


class JikeNotionClient:
    """即刻 Notion API 客户端"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.base_url = "https://api.notion.com/v1"
        self.version = "2022-06-28"

        # 从配置获取Notion设置
        notion_config = config.get_notion_config()
        self.integration_token = notion_config.get('integration_token')
        self.parent_page_id = notion_config.get('parent_page_id')

        if not self.integration_token:
            self.logger.warning("Notion集成token未配置")
        if not self.parent_page_id:
            self.logger.warning("Notion父页面ID未配置")

    def _get_headers(self) -> Dict[str, str]:
        """获取API请求头"""
        return {
            "Authorization": f"Bearer {self.integration_token}",
            "Content-Type": "application/json",
            "Notion-Version": self.version
        }

    def _make_request(self, method: str, endpoint: str, data: Dict = None) -> Dict[str, Any]:
        """发送API请求"""
        url = f"{self.base_url}/{endpoint}"
        headers = self._get_headers()

        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=headers, timeout=30)
            elif method.upper() == "POST":
                response = requests.post(url, headers=headers, json=data, timeout=30)
            elif method.upper() == "PATCH":
                response = requests.patch(url, headers=headers, json=data, timeout=30)
            else:
                raise ValueError(f"不支持的HTTP方法: {method}")

            response.raise_for_status()
            return {"success": True, "data": response.json()}

        except requests.exceptions.RequestException as e:
            error_msg = str(e)

            # 尝试获取更详细的错误信息
            try:
                if hasattr(e, 'response') and e.response is not None:
                    error_detail = e.response.json()
                    if 'message' in error_detail:
                        error_msg = f"{e}: {error_detail['message']}"
                    elif 'error' in error_detail:
                        error_msg = f"{e}: {error_detail['error']}"
            except:
                pass

            self.logger.error(f"Notion API请求失败: {error_msg}")
            return {"success": False, "error": error_msg}

    def get_page_children(self, page_id: str) -> Dict[str, Any]:
        """获取页面的子页面"""
        return self._make_request("GET", f"blocks/{page_id}/children")

    def create_page(self, parent_id: str, title: str, content_blocks: List[Dict] = None) -> Dict[str, Any]:
        """创建新页面"""
        data = {
            "parent": {"page_id": parent_id},
            "properties": {
                "title": {
                    "title": [
                        {
                            "text": {
                                "content": title
                            }
                        }
                    ]
                }
            }
        }

        if content_blocks:
            data["children"] = content_blocks

        return self._make_request("POST", "pages", data)

    def find_or_create_year_page(self, year: str) -> Optional[str]:
        """查找或创建年份页面"""
        try:
            # 获取父页面的子页面
            children_result = self.get_page_children(self.parent_page_id)
            if not children_result.get("success"):
                self.logger.error(f"获取父页面子页面失败: {children_result.get('error')}")
                return None

            # 查找年份页面
            for child in children_result["data"].get("results", []):
                if child.get("type") == "child_page":
                    page_title = self._extract_page_title(child)
                    if page_title == year:
                        return child["id"]

            # 创建年份页面
            self.logger.info(f"创建年份页面: {year}")
            create_result = self.create_page(self.parent_page_id, year)
            if create_result.get("success"):
                return create_result["data"]["id"]
            else:
                self.logger.error(f"创建年份页面失败: {create_result.get('error')}")
                return None

        except Exception as e:
            self.logger.error(f"查找或创建年份页面时出错: {e}")
            return None

    def find_or_create_month_page(self, year_page_id: str, month: str) -> Optional[str]:
        """查找或创建月份页面"""
        try:
            # 获取年份页面的子页面
            children_result = self.get_page_children(year_page_id)
            if not children_result.get("success"):
                self.logger.error(f"获取年份页面子页面失败: {children_result.get('error')}")
                return None

            # 查找月份页面
            for child in children_result["data"].get("results", []):
                if child.get("type") == "child_page":
                    page_title = self._extract_page_title(child)
                    if page_title == month:
                        return child["id"]

            # 创建月份页面
            self.logger.info(f"创建月份页面: {month}")
            create_result = self.create_page(year_page_id, month)
            if create_result.get("success"):
                return create_result["data"]["id"]
            else:
                self.logger.error(f"创建月份页面失败: {create_result.get('error')}")
                return None

        except Exception as e:
            self.logger.error(f"查找或创建月份页面时出错: {e}")
            return None

    def find_or_create_day_page(self, month_page_id: str, day: str) -> Optional[str]:
        """查找或创建日期页面"""
        try:
            # 获取月份页面的子页面
            children_result = self.get_page_children(month_page_id)
            if not children_result.get("success"):
                self.logger.error(f"获取月份页面子页面失败: {children_result.get('error')}")
                return None

            # 查找日期页面
            for child in children_result["data"].get("results", []):
                if child.get("type") == "child_page":
                    page_title = self._extract_page_title(child)
                    if page_title == day:
                        return child["id"]

            # 创建日期页面
            self.logger.info(f"创建日期页面: {day}")
            create_result = self.create_page(month_page_id, day)
            if create_result.get("success"):
                return create_result["data"]["id"]
            else:
                self.logger.error(f"创建日期页面失败: {create_result.get('error')}")
                return None

        except Exception as e:
            self.logger.error(f"查找或创建日期页面时出错: {e}")
            return None

    def check_report_exists(self, day_page_id: str, report_title: str) -> Optional[Dict[str, Any]]:
        """检查报告是否已经存在"""
        try:
            # 获取日期页面的子页面
            children_result = self.get_page_children(day_page_id)
            if not children_result.get("success"):
                return None

            # 查找同名报告
            for child in children_result["data"].get("results", []):
                if child.get("type") == "child_page":
                    page_title = self._extract_page_title(child)
                    if page_title == report_title:
                        page_id = child["id"]
                        page_url = f"https://www.notion.so/{page_id.replace('-', '')}"
                        return {
                            "exists": True,
                            "page_id": page_id,
                            "page_url": page_url
                        }

            return {"exists": False}

        except Exception as e:
            self.logger.error(f"检查报告是否存在时出错: {e}")
            return None

    def _extract_page_title(self, page_data: Dict) -> str:
        """从页面数据中提取标题"""
        try:
            if page_data.get("type") == "child_page":
                title_data = page_data.get("child_page", {}).get("title", "")
                return title_data
            return ""
        except Exception:
            return ""

    def _parse_rich_text(self, text: str) -> List[Dict]:
        """解析文本中的Markdown格式，支持链接、粗体等"""
        import re

        # 检查是否包含Source引用
        source_pattern = r'\[Sources?:\s*([T\d\s,]+)\]'
        source_matches = list(re.finditer(source_pattern, text))

        if not source_matches:
            # 没有Source引用，直接处理链接和格式
            return self._parse_links_and_formatting(text)

        # 有Source引用，需要分段处理
        rich_text = []
        last_end = 0

        for match in source_matches:
            # 添加Source引用前的普通文本
            if match.start() > last_end:
                before_text = text[last_end:match.start()]
                if before_text:
                    rich_text.extend(self._parse_links_and_formatting(before_text))

            # 添加Source引用（带特殊格式和提示）
            source_text = match.group(0)  # 完整的 [Source: T1] 文本
            rich_text.append({
                "type": "text",
                "text": {"content": f"📎 {source_text}"},
                "annotations": {
                    "italic": True,
                    "color": "blue",
                    "bold": False
                }
            })

            last_end = match.end()

        # 添加剩余的普通文本
        if last_end < len(text):
            remaining_text = text[last_end:]
            if remaining_text:
                rich_text.extend(self._parse_links_and_formatting(remaining_text))

        return rich_text

    def _parse_links_and_formatting(self, text: str) -> List[Dict]:
        """解析链接和格式，不包括Source引用"""
        import re

        rich_text = []

        # 现在标题中的方括号已经替换为中文方括号，可以使用简单的正则表达式
        link_pattern = r'\[([^\]]+)\]\((https?://[^)]+)\)'

        last_end = 0
        for match in re.finditer(link_pattern, text):
            # 添加链接前的普通文本
            if match.start() > last_end:
                before_text = text[last_end:match.start()]
                if before_text:
                    rich_text.extend(self._parse_text_formatting(before_text))

            # 添加链接
            link_text = match.group(1)
            link_url = match.group(2)
            rich_text.append({
                "type": "text",
                "text": {
                    "content": link_text,
                    "link": {"url": link_url}
                }
            })

            last_end = match.end()

        # 添加剩余的普通文本
        if last_end < len(text):
            remaining_text = text[last_end:]
            if remaining_text:
                rich_text.extend(self._parse_text_formatting(remaining_text))

        # 如果没有找到任何链接，处理整个文本
        if not rich_text:
            rich_text = self._parse_text_formatting(text)

        return rich_text

    def _parse_text_formatting(self, text: str) -> List[Dict]:
        """解析文本格式（粗体、斜体等）"""
        import re

        # 按优先级处理格式：粗体 -> 斜体 -> 普通文本
        # 使用更复杂的解析来支持嵌套格式

        # 创建格式化片段列表 [(start, end, format_type, content)]
        format_segments = []

        # 查找粗体 **text**
        bold_pattern = r'\*\*([^*]+)\*\*'
        for match in re.finditer(bold_pattern, text):
            format_segments.append((match.start(), match.end(), 'bold', match.group(1)))

        # 查找斜体 *text* (但要避免与粗体冲突)
        italic_pattern = r'(?<!\*)\*([^*]+)\*(?!\*)'
        for match in re.finditer(italic_pattern, text):
            # 检查是否与已有的粗体格式重叠
            overlaps = any(
                match.start() >= seg[0] and match.end() <= seg[1]
                for seg in format_segments if seg[2] == 'bold'
            )
            if not overlaps:
                format_segments.append((match.start(), match.end(), 'italic', match.group(1)))

        # 按位置排序
        format_segments.sort(key=lambda x: x[0])

        # 构建rich_text
        rich_text = []
        last_end = 0

        for start, end, format_type, content in format_segments:
            # 添加格式前的普通文本
            if start > last_end:
                before_text = text[last_end:start]
                if before_text:
                    rich_text.append({
                        "type": "text",
                        "text": {"content": before_text}
                    })

            # 添加格式化文本
            annotations = {}
            if format_type == 'bold':
                annotations["bold"] = True
            elif format_type == 'italic':
                annotations["italic"] = True

            rich_text.append({
                "type": "text",
                "text": {"content": content},
                "annotations": annotations
            })

            last_end = end

        # 添加剩余的普通文本
        if last_end < len(text):
            remaining_text = text[last_end:]
            if remaining_text:
                rich_text.append({
                    "type": "text",
                    "text": {"content": remaining_text}
                })

        # 如果没有找到任何格式，返回普通文本
        if not rich_text:
            rich_text = [{
                "type": "text",
                "text": {"content": text}
            }]

        return rich_text

    def _parse_list_items(self, lines: List[str], start_index: int) -> tuple[List[Dict], int]:
        """解析嵌套列表项，返回块列表和处理的行数"""
        blocks = []
        i = start_index

        while i < len(lines):
            line = lines[i]
            stripped_line = line.lstrip()

            # 如果不是列表项，结束解析
            if not stripped_line.startswith(('- ', '* ')):
                break

            # 如果是空行，跳过
            if not stripped_line:
                i += 1
                continue

            # 计算缩进级别 - 支持2空格或4空格缩进
            leading_spaces = len(line) - len(stripped_line)
            indent_level = 0
            if leading_spaces >= 4:
                indent_level = leading_spaces // 4  # 4空格为一级
            elif leading_spaces >= 2:
                indent_level = leading_spaces // 2  # 2空格为一级

            # 移除列表标记
            list_content = stripped_line[2:]  # 移除 '- ' 或 '* '

            # 如果这是一个顶级项（缩进级别为0），则处理它及其所有子项
            if indent_level == 0:
                # 创建列表项块
                list_item = {
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {
                        "rich_text": self._parse_rich_text(list_content)
                    }
                }

                # 查找子项
                children, lines_processed = self._parse_nested_children(lines, i + 1, indent_level)
                if children:
                    list_item["bulleted_list_item"]["children"] = children

                blocks.append(list_item)
                i += 1 + lines_processed  # 当前行 + 处理的子项行数

            else:
                # 如果这是嵌套项但没有父项，将其作为顶级项处理
                list_item = {
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {
                        "rich_text": self._parse_rich_text(list_content)
                    }
                }
                blocks.append(list_item)
                i += 1

        processed_lines = i - start_index
        return blocks, processed_lines

    def _parse_nested_children(self, lines: List[str], start_index: int, parent_indent: int) -> tuple[List[Dict], int]:
        """解析嵌套的子项"""
        children = []
        i = start_index

        while i < len(lines):
            line = lines[i]
            stripped_line = line.lstrip()

            # 空行跳过
            if not stripped_line:
                i += 1
                continue

            # 如果不是列表项，结束解析
            if not stripped_line.startswith(('- ', '* ')):
                break

            # 计算缩进级别
            leading_spaces = len(line) - len(stripped_line)
            indent_level = 0
            if leading_spaces >= 4:
                indent_level = leading_spaces // 4
            elif leading_spaces >= 2:
                indent_level = leading_spaces // 2

            # 如果缩进级别小于等于父级，不是子项
            if indent_level <= parent_indent:
                break

            # 如果是直接子项（缩进刚好多一级）
            if indent_level == parent_indent + 1:
                child_content = stripped_line[2:]  # 移除 '- ' 或 '* '
                child_item = {
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {
                        "rich_text": self._parse_rich_text(child_content)
                    }
                }

                # 递归查找孙子项
                grandchildren, child_lines_processed = self._parse_nested_children(lines, i + 1, indent_level)
                if grandchildren:
                    child_item["bulleted_list_item"]["children"] = grandchildren

                children.append(child_item)
                i += 1 + child_lines_processed  # 当前行 + 处理的孙子项行数
            else:
                # 跳过更深层的嵌套（已经在递归中处理）
                i += 1

        processed_lines = i - start_index
        return children, processed_lines

    def markdown_to_notion_blocks(self, markdown_content: str) -> tuple[List[Dict], List[Dict]]:
        """将Markdown内容转换为Notion块，支持链接、格式和表格"""
        blocks = []
        tables_to_add = []  # 用于跟踪需要添加的表格
        lines = markdown_content.split('\n')

        i = 0
        while i < len(lines):
            line = lines[i].strip()

            if not line:
                i += 1
                continue

            try:
                # 标题处理
                if line.startswith('# '):
                    blocks.append({
                        "object": "block",
                        "type": "heading_1",
                        "heading_1": {
                            "rich_text": self._parse_rich_text(line[2:])
                        }
                    })
                elif line.startswith('## '):
                    blocks.append({
                        "object": "block",
                        "type": "heading_2",
                        "heading_2": {
                            "rich_text": self._parse_rich_text(line[3:])
                        }
                    })
                elif line.startswith('### '):
                    blocks.append({
                        "object": "block",
                        "type": "heading_3",
                        "heading_3": {
                            "rich_text": self._parse_rich_text(line[4:])
                        }
                    })
                # 分割线
                elif line.startswith('---'):
                    blocks.append({
                        "object": "block",
                        "type": "divider",
                        "divider": {}
                    })
                # 列表项 - 支持多层嵌套
                elif line.startswith(('- ', '* ')) or (line.startswith(' ') and line.lstrip().startswith(('- ', '* '))):
                    # 处理列表项，支持嵌套结构
                    list_blocks, skip_lines = self._parse_list_items(lines, i)
                    blocks.extend(list_blocks)
                    i += skip_lines - 1  # -1 因为外层循环会+1
                # 表格处理 - 新增表格支持
                elif '|' in line and line.count('|') >= 2:
                    # 收集完整的表格
                    table_lines = []
                    table_start = i

                    # 收集所有表格行
                    while i < len(lines):
                        current_line = lines[i].strip()
                        if '|' in current_line and current_line.count('|') >= 2:
                            table_lines.append(current_line)
                        elif current_line == '':
                            # 空行，继续收集
                            pass
                        else:
                            # 非表格行，退出
                            break
                        i += 1

                    # 回退一行，因为外层循环会自增
                    i -= 1

                    # 处理收集到的表格
                    if table_lines:
                        self._process_table_to_blocks(table_lines, blocks, tables_to_add)

                    continue
                # 普通段落
                else:
                    # 处理可能的多行段落
                    paragraph_lines = [line]
                    j = i + 1
                    while j < len(lines) and lines[j].strip() and not lines[j].startswith(('#', '---')) and not (lines[j].startswith(('- ', '* ')) or (lines[j].startswith(' ') and lines[j].lstrip().startswith(('- ', '* ')))) and '|' not in lines[j]:
                        paragraph_lines.append(lines[j].strip())
                        j += 1

                    paragraph_text = ' '.join(paragraph_lines)
                    if paragraph_text:
                        blocks.append({
                            "object": "block",
                            "type": "paragraph",
                            "paragraph": {
                                "rich_text": self._parse_rich_text(paragraph_text)
                            }
                        })

                    i = j - 1

            except Exception as e:
                # 如果解析失败，添加为普通文本
                self.logger.warning(f"解析Markdown行失败，使用普通文本: {line[:50]}... 错误: {e}")
                blocks.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{"type": "text", "text": {"content": line}}]
                    }
                })

            i += 1

        return blocks, tables_to_add

    def _process_table_to_blocks(self, table_lines: List[str], blocks: List[Dict], tables_to_add: List[Dict]):
        """将表格行转换为 Notion 真实表格"""
        if not table_lines:
            return

        # 解析表格数据
        table_rows = []
        headers = None

        for line in table_lines:
            # 清理表格行
            cleaned_line = line.strip()
            if not cleaned_line:
                continue

            # 跳过分隔行 (如 |---|---|---|)
            cells_check = [cell.strip() for cell in cleaned_line.split('|')[1:-1]]
            is_separator = True
            for cell in cells_check:
                if cell and not all(c in '-: ' for c in cell):
                    is_separator = False
                    break

            if is_separator and cells_check:
                continue

            # 分割单元格
            cells = [cell.strip() for cell in cleaned_line.split('|')[1:-1]]

            if cells and any(cell for cell in cells):
                if headers is None:
                    headers = cells
                else:
                    table_rows.append(cells)

        # 如果没有有效数据，跳过
        if not headers or not table_rows:
            return

        # 对于大表格（>99行），分块处理
        if len(table_rows) > 99:
            self.logger.info(f"表格行数({len(table_rows)})超过Notion限制，将分块显示")
            self._create_chunked_tables(headers, table_rows, blocks, 99, tables_to_add)
        else:
            # 创建单个表格
            self._create_single_notion_table(headers, table_rows, blocks, tables_to_add)

    def _create_single_notion_table(self, headers: List[str], table_rows: List[List[str]], blocks: List[Dict], tables_to_add: List[Dict]):
        """创建单个Notion原生表格"""
        try:
            self.logger.info(f"准备创建Notion真实表格（{len(table_rows)}行数据）")

            # 添加表格占位符
            table_placeholder = {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{
                        "type": "text",
                        "text": {"content": f"📊 表格数据（{len(table_rows)}行，{len(headers)}列）"},
                        "annotations": {"bold": True, "color": "blue"}
                    }]
                }
            }
            blocks.append(table_placeholder)

            # 记录表格信息到单独的列表中
            tables_to_add.append({
                "headers": headers,
                "rows": table_rows,
                "placeholder_index": len(blocks) - 1
            })

        except Exception as e:
            self.logger.error(f"准备表格创建失败: {e}")
            self._create_table_as_code_block(headers, table_rows, blocks)

    def _create_table_as_code_block(self, headers: List[str], table_rows: List[List[str]], blocks: List[Dict]):
        """将表格转换为代码块显示（回退方案）"""
        try:
            table_text = ""
            header_line = "| " + " | ".join(headers) + " |"
            separator_line = "|" + "|".join(["-" * (len(h) + 2) for h in headers]) + "|"
            table_text += header_line + "\n" + separator_line + "\n"

            for row in table_rows:
                while len(row) < len(headers):
                    row.append("")
                display_row = []
                for cell in row[:len(headers)]:
                    cell_content = cell or ""
                    if len(cell_content) > 100:
                        cell_content = cell_content[:97] + "..."
                    display_row.append(cell_content)
                row_line = "| " + " | ".join(display_row) + " |"
                table_text += row_line + "\n"

            blocks.append({
                "object": "block",
                "type": "code",
                "code": {
                    "caption": [],
                    "rich_text": [{"type": "text", "text": {"content": table_text}}],
                    "language": "markdown"
                }
            })

        except Exception as e:
            self.logger.error(f"创建表格代码块失败: {e}")

    def _create_chunked_tables(self, headers: List[str], table_rows: List[List[str]], blocks: List[Dict], chunk_size: int, tables_to_add: List[Dict]):
        """将大表格分成多个小表格显示"""
        total_rows = len(table_rows)
        chunks = [table_rows[i:i + chunk_size] for i in range(0, total_rows, chunk_size)]

        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{
                    "type": "text",
                    "text": {"content": f"📊 表格包含 {total_rows} 行，分为 {len(chunks)} 个部分显示："},
                    "annotations": {"bold": True}
                }]
            }
        })

        for chunk_idx, chunk in enumerate(chunks):
            blocks.append({
                "object": "block",
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [{
                        "type": "text",
                        "text": {"content": f"第 {chunk_idx + 1} 部分 (第 {chunk_idx * chunk_size + 1}-{min((chunk_idx + 1) * chunk_size, total_rows)} 行)"}
                    }]
                }
            })
            self._create_single_notion_table(headers, chunk, blocks, tables_to_add)

    def _parse_table_cell_content(self, cell_content: str) -> List[Dict]:
        """解析表格单元格内容，支持链接和格式"""
        if not cell_content:
            return [{"type": "text", "text": {"content": ""}}]

        import re
        link_pattern = r'\[([^\]]+)\]\((https?://[^)]+)\)'
        rich_text = []
        last_end = 0

        for match in re.finditer(link_pattern, cell_content):
            if match.start() > last_end:
                before_text = cell_content[last_end:match.start()]
                if before_text:
                    rich_text.append({"type": "text", "text": {"content": before_text}})

            link_text = match.group(1)
            link_url = match.group(2)
            rich_text.append({
                "type": "text",
                "text": {"content": link_text, "link": {"url": link_url}}
            })
            last_end = match.end()

        if last_end < len(cell_content):
            remaining_text = cell_content[last_end:]
            if remaining_text:
                rich_text.append({"type": "text", "text": {"content": remaining_text}})

        if not rich_text:
            rich_text = [{"type": "text", "text": {"content": cell_content}}]

        return rich_text

    def _add_real_table_to_page(self, page_id: str, headers: List[str], table_rows: List[List[str]]) -> bool:
        """向已创建的页面添加真实表格"""
        try:
            max_rows = 99
            if len(table_rows) > max_rows:
                self.logger.info(f"表格行数({len(table_rows)})超过Notion限制({max_rows})，只添加前{max_rows}行")
                table_rows = table_rows[:max_rows]

            table_children = []

            # 添加标题行
            header_cells = []
            for header in headers:
                header_rich_text = self._parse_table_cell_content(header or "")
                header_cells.append(header_rich_text)

            table_children.append({
                "type": "table_row",
                "table_row": {"cells": header_cells}
            })

            # 添加数据行
            for row in table_rows:
                while len(row) < len(headers):
                    row.append("")

                row_cells = []
                for cell in row[:len(headers)]:
                    cell_content = cell or ""
                    if len(cell_content) > 200:
                        cell_content = cell_content[:197] + "..."
                    cell_rich_text = self._parse_table_cell_content(cell_content)
                    row_cells.append(cell_rich_text)

                table_children.append({
                    "type": "table_row",
                    "table_row": {"cells": row_cells}
                })

            # 构建API请求
            table_block = {
                "children": [{
                    "object": "block",
                    "type": "table",
                    "table": {
                        "table_width": len(headers),
                        "has_column_header": True,
                        "has_row_header": False,
                        "children": table_children
                    }
                }]
            }

            result = self._make_request("PATCH", f"blocks/{page_id}/children", table_block)
            if result.get("success"):
                self.logger.info(f"真实表格添加成功 ({len(table_rows)}行数据)")
                return True
            else:
                self.logger.error(f"添加真实表格失败: {result.get('error')}")
                return False

        except Exception as e:
            self.logger.error(f"添加真实表格时出现异常: {e}")
            return False

    def _validate_and_fix_content_blocks(self, blocks: List[Dict]) -> List[Dict]:
        """验证并修复内容块，处理长度超限问题，不截断内容"""
        validated_blocks = []

        for i, block in enumerate(blocks):
            try:
                block_type = block.get("type")

                # 处理包含rich_text的块类型
                if block_type in ["paragraph", "heading_1", "heading_2", "heading_3", "bulleted_list_item"]:
                    # 检查是否需要分割此块
                    split_blocks = self._split_overlong_block(block, i + 1)
                    validated_blocks.extend(split_blocks)
                else:
                    # 其他类型的块直接添加
                    validated_blocks.append(block)

            except Exception as e:
                self.logger.warning(f"验证块{i+1}时出错，跳过: {e}")
                continue

        return validated_blocks

    def _split_overlong_block(self, block: Dict, block_index: int) -> List[Dict]:
        """将超长的块分割成多个符合Notion限制的块，保持内容完整"""
        try:
            block_type = block["type"]
            rich_text_list = block[block_type].get("rich_text", [])

            if not rich_text_list:
                return [block]

            # 首先处理每个rich_text项的内容长度
            processed_rich_text = []
            for text_item in rich_text_list:
                if not text_item.get("text", {}).get("content"):
                    processed_rich_text.append(text_item)
                    continue

                content = text_item["text"]["content"]

                # 如果单个内容超过2000字符，分割它
                if len(content) > 2000:
                    chunks = self._split_content_smartly(content, 1950)
                    for chunk in chunks:
                        chunk_item = text_item.copy()
                        chunk_item["text"] = chunk_item["text"].copy()
                        chunk_item["text"]["content"] = chunk
                        processed_rich_text.append(chunk_item)
                else:
                    processed_rich_text.append(text_item)

            # 检查rich_text数组长度是否超过100
            if len(processed_rich_text) <= 100:
                # 没有超长，直接返回修复后的块
                fixed_block = block.copy()
                fixed_block[block_type] = fixed_block[block_type].copy()
                fixed_block[block_type]["rich_text"] = processed_rich_text
                return [fixed_block]

            # rich_text数组超长，需要分割成多个块
            self.logger.info(f"块{block_index}的rich_text数组过长({len(processed_rich_text)}个元素)，分割成多个{block_type}块")

            result_blocks = []
            chunk_size = 99  # 每个块最多99个rich_text元素，留1个空间

            for i in range(0, len(processed_rich_text), chunk_size):
                chunk_rich_text = processed_rich_text[i:i + chunk_size]

                # 创建新块
                new_block = {
                    "object": "block",
                    "type": block_type,
                    block_type: {
                        "rich_text": chunk_rich_text
                    }
                }

                # 如果是列表项且有子项，只在第一个块中保留子项
                if block_type == "bulleted_list_item" and i == 0:
                    if "children" in block[block_type]:
                        new_block[block_type]["children"] = block[block_type]["children"]

                result_blocks.append(new_block)

            self.logger.debug(f"块{block_index}被分割为{len(result_blocks)}个块")
            return result_blocks

        except Exception as e:
            self.logger.warning(f"分割块{block_index}时出错: {e}")
            # 如果分割失败，返回原始块（可能会导致API错误，但不会丢失内容）
            return [block]

    def _fix_rich_text_content(self, block: Dict, block_index: int) -> Optional[Dict]:
        """修复单个块的rich_text内容长度问题"""
        try:
            block_type = block["type"]
            rich_text_list = block[block_type].get("rich_text", [])

            if not rich_text_list:
                return block

            fixed_rich_text = []

            for text_item in rich_text_list:
                if not text_item.get("text", {}).get("content"):
                    fixed_rich_text.append(text_item)
                    continue

                content = text_item["text"]["content"]

                # 如果内容长度超过限制，需要分割
                if len(content) > 2000:
                    self.logger.debug(f"块{block_index}内容超长({len(content)}字符)，开始分割")

                    # 将长内容分割成多个2000字符以内的片段
                    chunks = self._split_content_smartly(content, 1950)  # 留一些余量

                    for j, chunk in enumerate(chunks):
                        chunk_item = text_item.copy()
                        chunk_item["text"] = chunk_item["text"].copy()
                        chunk_item["text"]["content"] = chunk

                        if j == len(chunks) - 1 and len(chunks) > 1:
                            # 最后一个分片，添加提示
                            chunk_item["text"]["content"] += " ..."

                        fixed_rich_text.append(chunk_item)

                    self.logger.debug(f"块{block_index}分割为{len(chunks)}个片段")
                else:
                    fixed_rich_text.append(text_item)

            # 更新块的rich_text
            block[block_type]["rich_text"] = fixed_rich_text
            return block

        except Exception as e:
            self.logger.warning(f"修复块{block_index}的rich_text时出错: {e}")
            return block

    def _split_content_smartly(self, content: str, max_length: int) -> List[str]:
        """智能分割内容，尽量在句号、换行等位置分割"""
        if len(content) <= max_length:
            return [content]

        chunks = []
        current_pos = 0

        while current_pos < len(content):
            # 计算当前块的结束位置
            end_pos = min(current_pos + max_length, len(content))

            if end_pos == len(content):
                # 最后一块
                chunks.append(content[current_pos:end_pos])
                break

            # 尝试在合适的位置分割
            chunk_content = content[current_pos:end_pos]

            # 查找分割点的优先级：句号 > 换行 > 逗号 > 空格
            split_chars = ['。', '\n', '，', '、', ' ']
            split_pos = -1

            for char in split_chars:
                pos = chunk_content.rfind(char)
                if pos > max_length * 0.7:  # 至少要用到70%的长度才分割
                    split_pos = pos + 1
                    break

            if split_pos > 0:
                # 找到了合适的分割点
                chunks.append(content[current_pos:current_pos + split_pos])
                current_pos += split_pos
            else:
                # 没有找到合适的分割点，强制分割
                chunks.append(chunk_content)
                current_pos = end_pos

        return chunks

    def _create_large_content_page(self, parent_page_id: str, page_title: str,
                                  content_blocks: List[Dict]) -> Dict[str, Any]:
        """创建大内容页面，分批添加内容块"""
        try:
            self.logger.info(f"创建大内容页面，总共 {len(content_blocks)} 个块，需要分批处理")

            # 第一步：创建空页面，只包含前50个块（减少初始块数量）
            initial_batch_size = 50
            initial_blocks = content_blocks[:initial_batch_size]
            create_result = self.create_page(parent_page_id, page_title, initial_blocks)

            if not create_result.get("success"):
                return create_result

            page_id = create_result["data"]["id"]
            self.logger.info(f"页面创建成功，开始添加剩余 {len(content_blocks) - initial_batch_size} 个块")

            # 第二步：分批添加剩余的块
            remaining_blocks = content_blocks[initial_batch_size:]
            batch_size = 50  # 减少每批的块数量

            for i in range(0, len(remaining_blocks), batch_size):
                batch = remaining_blocks[i:i + batch_size]
                batch_num = (i // batch_size) + 2

                self.logger.info(f"添加第 {batch_num} 批内容: {len(batch)} 个块")

                # 使用 PATCH 方法添加子块，增加重试机制
                append_result = self._append_blocks_to_page_with_retry(page_id, batch, max_retries=3)

                if not append_result.get("success"):
                    self.logger.warning(f"第 {batch_num} 批内容添加失败: {append_result.get('error')}")
                    # 继续尝试添加其他批次
                else:
                    self.logger.info(f"第 {batch_num} 批内容添加成功")

                # 增加延迟避免API限制
                import time
                time.sleep(1.0)  # 增加等待时间

            page_url = f"https://www.notion.so/{page_id.replace('-', '')}"
            return {
                "success": True,
                "data": {"id": page_id},
                "page_url": page_url,
                "total_blocks": len(content_blocks)
            }

        except Exception as e:
            self.logger.error(f"创建大内容页面时出错: {e}")
            return {"success": False, "error": str(e)}

    def _append_blocks_to_page_with_retry(self, page_id: str, blocks: List[Dict], max_retries: int = 3) -> Dict[str, Any]:
        """向页面追加内容块，带重试机制"""
        import time

        for attempt in range(max_retries):
            try:
                self.logger.debug(f"尝试追加{len(blocks)}个块 (尝试 {attempt + 1}/{max_retries})")

                # 在每次尝试前验证块内容
                validated_blocks = self._validate_and_fix_content_blocks(blocks)

                result = self._append_blocks_to_page(page_id, validated_blocks)

                if result.get("success"):
                    return result
                else:
                    error_msg = result.get("error", "未知错误")
                    self.logger.warning(f"追加块失败 (尝试 {attempt + 1}/{max_retries}): {error_msg}")

                    # 如果是内容验证错误，尝试进一步分割
                    if "content.length should be" in error_msg or "2000" in error_msg:
                        self.logger.info("检测到内容长度问题，尝试进一步分割内容")
                        blocks = self._further_split_blocks(validated_blocks)

                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 2  # 递增等待: 2, 4, 6秒
                        self.logger.info(f"等待 {wait_time} 秒后重试...")
                        time.sleep(wait_time)

            except Exception as e:
                error_msg = f"追加块时发生异常 (尝试 {attempt + 1}/{max_retries}): {str(e)}"
                self.logger.error(error_msg)

                if attempt == max_retries - 1:
                    return {"success": False, "error": error_msg}
                else:
                    wait_time = (attempt + 1) * 2
                    self.logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)

        return {"success": False, "error": f"重试{max_retries}次后仍然失败"}

    def _further_split_blocks(self, blocks: List[Dict]) -> List[Dict]:
        """进一步分割内容块，处理仍然超长的内容"""
        further_split_blocks = []

        for block in blocks:
            block_type = block.get("type")

            if block_type in ["paragraph", "heading_1", "heading_2", "heading_3", "bulleted_list_item"]:
                rich_text_list = block[block_type].get("rich_text", [])

                new_rich_text = []
                for text_item in rich_text_list:
                    if text_item.get("text", {}).get("content"):
                        content = text_item["text"]["content"]
                        if len(content) > 1500:  # 更严格的长度限制
                            # 进一步分割
                            chunks = self._split_content_smartly(content, 1200)
                            for chunk in chunks:
                                chunk_item = text_item.copy()
                                chunk_item["text"] = chunk_item["text"].copy()
                                chunk_item["text"]["content"] = chunk
                                new_rich_text.append(chunk_item)
                        else:
                            new_rich_text.append(text_item)
                    else:
                        new_rich_text.append(text_item)

                # 更新块
                block[block_type]["rich_text"] = new_rich_text

            further_split_blocks.append(block)

        return further_split_blocks

    def _append_blocks_to_page(self, page_id: str, blocks: List[Dict]) -> Dict[str, Any]:
        """向页面追加内容块"""
        try:
            data = {
                "children": blocks
            }

            return self._make_request("PATCH", f"blocks/{page_id}/children", data)

        except Exception as e:
            self.logger.error(f"追加内容块时出错: {e}")
            return {"success": False, "error": str(e)}

    def find_or_create_report_type_folder(self, day_page_id: str, report_type: str) -> Optional[str]:
        """在日期页面下查找或创建报告类型文件夹（日报资讯/深度洞察/周度报告）

        Args:
            day_page_id: 日期页面ID
            report_type: 'light' (日报资讯) 或 'deep' (深度洞察) 或 'weekly' (周度报告)

        Returns:
            文件夹页面ID，失败返回None
        """
        try:
            # 确定文件夹名称
            folder_name_map = {
                'light': '日报资讯',
                'deep': '深度洞察',
                'weekly': '周度报告'
            }
            folder_name = folder_name_map.get(report_type, '未知类型')

            # 获取日期页面的子页面
            children_result = self.get_page_children(day_page_id)
            if not children_result.get("success"):
                self.logger.error(f"获取日期页面子页面失败: {children_result.get('error')}")
                return None

            # 查找文件夹页面
            for child in children_result["data"].get("results", []):
                if child.get("type") == "child_page":
                    page_title = self._extract_page_title(child)
                    if page_title == folder_name:
                        return child["id"]

            # 创建文件夹页面
            self.logger.info(f"创建报告类型文件夹: {folder_name}")
            create_result = self.create_page(day_page_id, folder_name)
            if create_result.get("success"):
                return create_result["data"]["id"]
            else:
                self.logger.error(f"创建报告类型文件夹失败: {create_result.get('error')}")
                return None

        except Exception as e:
            self.logger.error(f"查找或创建报告类型文件夹时出错: {e}")
            return None

    def create_report_page_in_hierarchy(self, report_title: str, report_content: str,
                                       report_date: datetime, report_type: str = 'deep') -> Dict[str, Any]:
        """创建报告页面，支持多轨制层级结构（年/月/日/报告类型文件夹/报告）

        Args:
            report_title: 报告标题
            report_content: 报告内容
            report_date: 报告日期
            report_type: 'light' (日报资讯) 或 'deep' (深度洞察) 或 'weekly' (周度报告)

        Returns:
            创建结果
        """
        try:
            if not self.integration_token or not self.parent_page_id:
                return {
                    "success": False,
                    "error": "Notion配置不完整"
                }

            year = str(report_date.year)
            month = f"{report_date.month:02d}月"
            day = f"{report_date.day:02d}日"

            folder_name_map = {
                'light': '日报资讯',
                'deep': '深度洞察',
                'weekly': '周度报告'
            }
            folder_name = folder_name_map.get(report_type, '未知类型')

            self.logger.info(f"开始创建{folder_name}报告页面: {year}/{month}/{day}/{folder_name} - {report_title}")

            # 1. 查找或创建年份页面
            year_page_id = self.find_or_create_year_page(year)
            if not year_page_id:
                return {"success": False, "error": "无法创建年份页面"}

            # 2. 查找或创建月份页面
            month_page_id = self.find_or_create_month_page(year_page_id, month)
            if not month_page_id:
                return {"success": False, "error": "无法创建月份页面"}

            # 3. 查找或创建日期页面
            day_page_id = self.find_or_create_day_page(month_page_id, day)
            if not day_page_id:
                return {"success": False, "error": "无法创建日期页面"}

            # 4. 查找或创建报告类型文件夹
            folder_page_id = self.find_or_create_report_type_folder(day_page_id, report_type)
            if not folder_page_id:
                return {"success": False, "error": f"无法创建{folder_name}文件夹"}

            # 5. 检查报告是否已经存在
            existing_report = self.check_report_exists(folder_page_id, report_title)
            if existing_report and existing_report.get("exists"):
                self.logger.info(f"报告已存在，跳过创建: {existing_report.get('page_url')}")
                return {
                    "success": True,
                    "page_id": existing_report.get("page_id"),
                    "page_url": existing_report.get("page_url"),
                    "path": f"{year}/{month}/{day}/{folder_name}/{report_title}",
                    "skipped": True,
                    "reason": "报告已存在"
                }

            # 6. 在文件夹下创建报告页面
            content_blocks, tables_to_add = self.markdown_to_notion_blocks(report_content)

            # 限制块数量
            max_blocks = 1000
            if len(content_blocks) > max_blocks:
                self.logger.warning(f"报告内容过长({len(content_blocks)}个块)，截断到{max_blocks}个块")
                content_blocks = content_blocks[:max_blocks]
                content_blocks.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{
                            "type": "text",
                            "text": {"content": "⚠️ 内容过长已截断，完整内容请查看数据库记录"},
                            "annotations": {"italic": True, "color": "gray"}
                        }]
                    }
                })

            # 验证并修复内容块
            validated_blocks = self._validate_and_fix_content_blocks(content_blocks)
            self.logger.info(f"内容验证完成: {len(validated_blocks)}/{len(content_blocks)} 个块通过验证")

            # 创建页面
            initial_block_limit = 50
            if len(validated_blocks) <= initial_block_limit:
                create_result = self.create_page(folder_page_id, report_title, validated_blocks)
            else:
                create_result = self._create_large_content_page(folder_page_id, report_title, validated_blocks)

            if create_result.get("success"):
                page_id = create_result["data"]["id"]
                page_url = f"https://www.notion.so/{page_id.replace('-', '')}"

                # 检查是否有需要添加的表格
                if tables_to_add:
                    self.logger.info(f"页面创建成功，开始添加 {len(tables_to_add)} 个真实表格")
                    success_count = 0
                    for i, table_info in enumerate(tables_to_add):
                        try:
                            if self._add_real_table_to_page(page_id, table_info["headers"], table_info["rows"]):
                                success_count += 1
                                self.logger.info(f"真实表格 {i+1}/{len(tables_to_add)} 添加成功")
                            else:
                                self.logger.warning(f"真实表格 {i+1}/{len(tables_to_add)} 添加失败，但页面已创建")
                        except Exception as e:
                            self.logger.error(f"添加真实表格 {i+1} 时出错: {e}")

                    if success_count > 0:
                        self.logger.info(f"成功添加 {success_count}/{len(tables_to_add)} 个真实表格")

                self.logger.info(f"{folder_name}报告页面创建成功: {page_url}")
                return {
                    "success": True,
                    "page_id": page_id,
                    "page_url": page_url,
                    "path": f"{year}/{month}/{day}/{folder_name}/{report_title}",
                    "report_type": report_type
                }
            else:
                self.logger.error(f"创建{folder_name}报告页面失败: {create_result.get('error')}")
                return {"success": False, "error": create_result.get("error")}

        except Exception as e:
            self.logger.error(f"创建{report_type}报告页面时出错: {e}")
            return {"success": False, "error": str(e)}

    def create_report_page(self, report_title: str, report_content: str,
                          report_date: datetime = None) -> Dict[str, Any]:
        """创建报告页面，按年/月/日层级组织（兼容不使用文件夹分类的版本）"""
        try:
            if not self.integration_token or not self.parent_page_id:
                return {
                    "success": False,
                    "error": "Notion配置不完整"
                }

            # 使用报告日期或当前日期
            if report_date is None:
                report_date = datetime.now(timezone.utc) + timedelta(hours=8)  # 北京时间

            year = str(report_date.year)
            month = f"{report_date.month:02d}月"
            day = f"{report_date.day:02d}日"

            self.logger.info(f"开始创建报告页面: {year}/{month}/{day} - {report_title}")

            # 1. 查找或创建年份页面
            year_page_id = self.find_or_create_year_page(year)
            if not year_page_id:
                return {"success": False, "error": "无法创建年份页面"}

            # 2. 查找或创建月份页面
            month_page_id = self.find_or_create_month_page(year_page_id, month)
            if not month_page_id:
                return {"success": False, "error": "无法创建月份页面"}

            # 3. 查找或创建日期页面
            day_page_id = self.find_or_create_day_page(month_page_id, day)
            if not day_page_id:
                return {"success": False, "error": "无法创建日期页面"}

            # 3.5. 检查报告是否已经存在
            existing_report = self.check_report_exists(day_page_id, report_title)
            if existing_report and existing_report.get("exists"):
                self.logger.info(f"报告已存在，跳过创建: {existing_report.get('page_url')}")
                return {
                    "success": True,
                    "page_id": existing_report.get("page_id"),
                    "page_url": existing_report.get("page_url"),
                    "path": f"{year}/{month}/{day}/{report_title}",
                    "skipped": True,
                    "reason": "报告已存在"
                }

            # 4. 在日期页面下创建报告页面
            content_blocks, tables_to_add = self.markdown_to_notion_blocks(report_content)

            # 虽然API单次请求限制100块，但我们可以分批处理更多内容
            max_blocks = 1000
            if len(content_blocks) > max_blocks:
                self.logger.warning(f"报告内容过长({len(content_blocks)}个块)，截断到{max_blocks}个块")
                content_blocks = content_blocks[:max_blocks]

                # 添加截断提示
                content_blocks.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{
                            "type": "text",
                            "text": {"content": "⚠️ 内容过长已截断，完整内容请查看数据库记录"},
                            "annotations": {"italic": True, "color": "gray"}
                        }]
                    }
                })
            else:
                self.logger.info(f"报告内容包含 {len(content_blocks)} 个块，在限制范围内")

            # 验证并修复每个块的内容长度
            validated_blocks = self._validate_and_fix_content_blocks(content_blocks)
            self.logger.info(f"内容验证完成: {len(validated_blocks)}/{len(content_blocks)} 个块通过验证")

            # Notion API限制：单次创建页面最多100个子块
            # 为了提高成功率，减少初始创建时的块数量
            initial_block_limit = 50
            if len(validated_blocks) <= initial_block_limit:
                # 小内容，直接创建
                create_result = self.create_page(day_page_id, report_title, validated_blocks)
            else:
                # 大内容，分批创建
                create_result = self._create_large_content_page(day_page_id, report_title, validated_blocks)

            if create_result.get("success"):
                page_id = create_result["data"]["id"]
                page_url = f"https://www.notion.so/{page_id.replace('-', '')}"

                # 检查是否有需要添加的表格
                if tables_to_add:
                    self.logger.info(f"页面创建成功，开始添加 {len(tables_to_add)} 个真实表格")
                    success_count = 0
                    for i, table_info in enumerate(tables_to_add):
                        try:
                            if self._add_real_table_to_page(page_id, table_info["headers"], table_info["rows"]):
                                success_count += 1
                                self.logger.info(f"真实表格 {i+1}/{len(tables_to_add)} 添加成功")
                            else:
                                self.logger.warning(f"真实表格 {i+1}/{len(tables_to_add)} 添加失败，但页面已创建")
                        except Exception as e:
                            self.logger.error(f"添加真实表格 {i+1} 时出错: {e}")

                    if success_count > 0:
                        self.logger.info(f"成功添加 {success_count}/{len(tables_to_add)} 个真实表格")

                self.logger.info(f"报告页面创建成功: {page_url}")
                return {
                    "success": True,
                    "page_id": page_id,
                    "page_url": page_url,
                    "path": f"{year}/{month}/{day}/{report_title}"
                }
            else:
                self.logger.error(f"创建报告页面失败: {create_result.get('error')}")
                return {"success": False, "error": create_result.get("error")}

        except Exception as e:
            self.logger.error(f"创建报告页面时出错: {e}")
            return {"success": False, "error": str(e)}


# 全局即刻Notion客户端实例
jike_notion_client = JikeNotionClient()