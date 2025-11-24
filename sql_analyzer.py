#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL分析模块
包含所有SQL解析和分析相关的方法
"""

import re
from typing import List, Optional, Dict


class SQLAnalyzer:
    """SQL分析器类"""
    
    @staticmethod
    def extract_table_name(sql: str) -> Optional[str]:
        """从SQL语句中提取表名
        
        Args:
            sql: SQL语句字符串
            
        Returns:
            提取的表名，如果无法提取则返回None
        """
        if not sql:
            return None
        
        sql_clean = sql.strip().rstrip(';')
        sql_normalized = re.sub(r'\s+', ' ', sql_clean)
        sql_upper = sql_normalized.upper()
        
        # 通用的FROM子句提取
        from_match = re.search(r'\bFROM\b\s+(.+)', sql_normalized, re.IGNORECASE)
        if from_match:
            from_clause = from_match.group(1)
            # 截断到下一个关键字
            stop_keywords = [' WHERE ', ' GROUP ', ' ORDER ', ' LIMIT ', ' HAVING ', ' UNION ', ' EXCEPT ', ' INTERSECT ']
            stop_index = len(from_clause)
            upper_clause = from_clause.upper()
            for keyword in stop_keywords:
                idx = upper_clause.find(keyword)
                if idx != -1 and idx < stop_index:
                    stop_index = idx
            from_clause = from_clause[:stop_index].strip()
            
            if from_clause:
                # 处理逗号分隔的多表语法
                first_segment = re.split(r',', from_clause, 1)[0].strip()
                # 处理JOIN语法，截取JOIN之前的第一个表
                first_segment = re.split(r'\bJOIN\b|\bLEFT\b|\bRIGHT\b|\bINNER\b|\bOUTER\b', first_segment, 1)[0].strip()
                
                # 去掉括号（处理嵌套查询别名）
                if first_segment.startswith('('):
                    first_segment = ''
                else:
                    # 去掉别名
                    tokens = first_segment.split()
                    if tokens:
                        table_token = tokens[0].strip('`')
                        if table_token and table_token.upper() not in {'SELECT', 'FROM'}:
                            return table_token
        
        # INSERT / UPDATE / DELETE 等语句的后备匹配
        patterns = [
            r'INSERT\s+INTO\s+`?([a-zA-Z0-9_]+)`?',
            r'UPDATE\s+`?([a-zA-Z0-9_]+)`?',
            r'DELETE\s+FROM\s+`?([a-zA-Z0-9_]+)`?'
        ]
        for pattern in patterns:
            match = re.search(pattern, sql_upper, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
    @staticmethod
    def extract_where_fields(sql: str) -> List[str]:
        """从SQL语句中提取WHERE条件中的字段名，包括函数字段检测
        
        Args:
            sql: SQL语句字符串
            
        Returns:
            WHERE条件中的字段名列表，包含函数字段信息
        """
        if not sql:
            return []
        
        # 移除SQL语句末尾的分号
        sql_clean = sql.strip()
        if sql_clean.endswith(';'):
            sql_clean = sql_clean[:-1].strip()
        
        # 查找WHERE子句
        where_match = re.search(r'\bWHERE\s+(.+?)(?:\s+ORDER\s+BY|\s+GROUP\s+BY|\s+LIMIT|$)', sql_clean, re.IGNORECASE | re.DOTALL)
        if not where_match:
            return []
        
        where_clause = where_match.group(1).strip()
        
        # 分离AND条件和OR条件，优先选择AND字段
        and_conditions = []
        or_conditions = []
        
        # 按OR分割，然后分别处理每个部分
        or_parts = re.split(r'\bOR\b', where_clause, flags=re.IGNORECASE)
        
        for i, part in enumerate(or_parts):
            part = part.strip()
            if i == 0:
                # 第一个部分是主条件，可能包含AND连接的条件
                if re.search(r'\bAND\b', part, re.IGNORECASE):
                    and_conditions.append(part)
                else:
                    # 单个条件也作为AND条件处理
                    and_conditions.append(part)
            else:
                # 其他部分是OR条件
                or_conditions.append(part)
        
        # 提取字段名（包括函数字段检测）
        and_fields = []
        or_fields = []
        
        # 提取AND条件中的字段
        for and_part in and_conditions:
            and_fields.extend(SQLAnalyzer.extract_fields_from_condition(and_part))
        
        # 提取OR条件中的字段
        for or_part in or_conditions:
            or_fields.extend(SQLAnalyzer.extract_fields_from_condition(or_part))
        
        # 优先选择AND字段，当AND字段不足5个时再选择OR字段
        # 去重并保持顺序
        unique_and_fields = []
        seen = set()
        for field in and_fields:
            if field not in seen:
                unique_and_fields.append(field)
                seen.add(field)
        
        unique_or_fields = []
        for field in or_fields:
            if field not in seen:
                unique_or_fields.append(field)
                seen.add(field)
        
        # 如果AND字段已经有5个或以上，只取前5个AND字段
        if len(unique_and_fields) >= 5:
            fields = unique_and_fields[:5]
        else:
            # AND字段不足5个，用OR字段补充到5个
            # 当需要选择OR字段时，优先选择f字段（如果存在）
            needed_or_count = 5 - len(unique_and_fields)
            
            # 重新排序OR字段，优先选择f字段
            prioritized_or_fields = []
            f_field = None
            other_or_fields = []
            
            for field in unique_or_fields:
                if field == 'f':
                    f_field = field
                else:
                    other_or_fields.append(field)
            
            # 优先添加f字段，然后添加其他OR字段
            if f_field:
                prioritized_or_fields.append(f_field)
            prioritized_or_fields.extend(other_or_fields)
            
            fields = unique_and_fields + prioritized_or_fields[:needed_or_count]
        
        return fields
    
    @staticmethod
    def extract_fields_from_condition(condition: str) -> List[str]:
        """从单个条件中提取字段名
        
        Args:
            condition: 单个条件字符串
            
        Returns:
            字段名列表
        """
        fields = []
        
        # 模式1：匹配函数字段，如 LOWER(name), UPPER(column)
        function_pattern = r'\b([A-Za-z_]+)\s*\(\s*([a-zA-Z_]\w*)\s*\)'
        function_matches = re.findall(function_pattern, condition)
        
        for func_name, field_name in function_matches:
            # 标记为函数字段，格式为 "函数名(字段名)"
            func_field = f"{func_name.upper()}({field_name})"
            if func_field.upper() not in ['SELECT', 'FROM', 'WHERE', 'AND', 'OR']:
                fields.append(func_field)
        
        # 模式2：匹配普通字段名，如 name = 'value'
        field_pattern = r'\b([a-zA-Z_]\w*)\s*[=<>!]+'
        matches = re.findall(field_pattern, condition)
        
        for field in matches:
            # 排除SQL关键字和已经提取的函数字段
            field_upper = field.upper()
            if field_upper not in ['SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN']:
                # 检查这个字段是否已经是函数字段的一部分
                is_in_function = False
                for func_field in fields:
                    if field in func_field:
                        is_in_function = True
                        break
                if not is_in_function:
                    fields.append(field)
        
        return fields
    
    @staticmethod
    def extract_join_fields(sql: str) -> List[str]:
        """从SQL语句中提取JOIN条件中的字段名
        
        Args:
            sql: SQL语句字符串
            
        Returns:
            JOIN条件中的字段名列表
        """
        if not sql:
            return []
        
        # 移除SQL语句末尾的分号
        sql_clean = sql.strip()
        if sql_clean.endswith(';'):
            sql_clean = sql_clean[:-1].strip()
        
        # 查找JOIN子句
        join_pattern = r'\b(?:INNER|LEFT|RIGHT|FULL)?\s*JOIN\s+\w+\s+ON\s+(.+?)(?:\s+(?:LEFT|RIGHT|INNER|JOIN|WHERE|ORDER|GROUP|LIMIT)|\s*$)'
        join_matches = re.findall(join_pattern, sql_clean, re.IGNORECASE | re.DOTALL)
        
        fields = []
        for join_condition in join_matches:
            # 提取ON条件中的字段名
            field_pattern = r'\b([a-zA-Z_]\w*)\s*[=<>!]+'
            matches = re.findall(field_pattern, join_condition)
            
            for field in matches:
                # 排除SQL关键字
                if field.upper() not in ['SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'ON']:
                    fields.append(field)
        
        return list(set(fields))  # 去重
    
    @staticmethod
    def extract_order_by_fields(sql: str) -> List[str]:
        """从SQL语句中提取ORDER BY子句中的字段名
        
        Args:
            sql: SQL语句字符串
            
        Returns:
            ORDER BY子句中的字段名列表
        """
        if not sql:
            return []
        
        # 移除SQL语句末尾的分号
        sql_clean = sql.strip()
        if sql_clean.endswith(';'):
            sql_clean = sql_clean[:-1].strip()
        
        # 查找ORDER BY子句
        order_match = re.search(r'\bORDER\s+BY\s+(.+?)(?:\s+LIMIT|$)', sql_clean, re.IGNORECASE | re.DOTALL)
        if not order_match:
            return []
        
        order_clause = order_match.group(1).strip()
        
        # 提取字段名
        fields = []
        # 匹配字段名模式：table.column 或 column (支持DESC/ASC)
        field_pattern = r'\b([a-zA-Z_]\w*)\s*(?:DESC|ASC)?\s*(?:,|$)'
        matches = re.findall(field_pattern, order_clause)
        
        for field in matches:
            # 排除SQL关键字
            if field.upper() not in ['SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'ORDER', 'BY', 'DESC', 'ASC']:
                fields.append(field)
        
        return list(set(fields))  # 去重
    
    @staticmethod
    def extract_table_name_from_sql(sql: str) -> str:
        """
        从SQL语句中智能提取表名
        
        Args:
            sql: SQL语句字符串
            
        Returns:
            提取的表名，如果无法提取则返回'未知表'
        """
        if not sql:
            return '未知表'
        
        # 清理SQL语句
        sql_clean = sql.strip()
        
        # 支持多种SQL语句类型
        
        # 1. SELECT语句：提取FROM子句后的表名
        if re.search(r'\bSELECT\b', sql_clean, re.IGNORECASE):
            # 尝试从FROM子句中提取表名
            from_match = re.search(r'FROM\s+`?(\w+)`?\b', sql_clean, re.IGNORECASE)
            if from_match:
                return from_match.group(1)
            
            # 尝试从JOIN子句中提取表名
            join_match = re.search(r'JOIN\s+`?(\w+)`?\b', sql_clean, re.IGNORECASE)
            if join_match:
                return join_match.group(1)
        
        # 2. UPDATE语句：提取UPDATE后的表名
        elif re.search(r'\bUPDATE\b', sql_clean, re.IGNORECASE):
            update_match = re.search(r'UPDATE\s+`?(\w+)`?\b', sql_clean, re.IGNORECASE)
            if update_match:
                return update_match.group(1)
        
        # 3. INSERT语句：提取INSERT INTO后的表名
        elif re.search(r'\bINSERT\b', sql_clean, re.IGNORECASE):
            insert_match = re.search(r'INSERT\s+INTO\s+`?(\w+)`?\b', sql_clean, re.IGNORECASE)
            if insert_match:
                return insert_match.group(1)
        
        # 4. DELETE语句：提取DELETE FROM后的表名
        elif re.search(r'\bDELETE\b', sql_clean, re.IGNORECASE):
            delete_match = re.search(r'DELETE\s+FROM\s+`?(\w+)`?\b', sql_clean, re.IGNORECASE)
            if delete_match:
                return delete_match.group(1)
        
        # 5. 尝试从其他常见模式中提取表名
        table_patterns = [
            r'FROM\s+`?(\w+)`?\b',
            r'JOIN\s+`?(\w+)`?\b', 
            r'UPDATE\s+`?(\w+)`?\b',
            r'INSERT\s+INTO\s+`?(\w+)`?\b',
            r'DELETE\s+FROM\s+`?(\w+)`?\b',
            r'TABLE\s+`?(\w+)`?\b',
            r'CREATE\s+TABLE\s+`?(\w+)`?\b',
            r'ALTER\s+TABLE\s+`?(\w+)`?\b',
            r'DROP\s+TABLE\s+`?(\w+)`?\b',
            r'TRUNCATE\s+TABLE\s+`?(\w+)`?\b'
        ]
        
        for pattern in table_patterns:
            match = re.search(pattern, sql_clean, re.IGNORECASE)
            if match:
                return match.group(1)
        
        # 6. 最后尝试从SQL中提取第一个看起来像表名的单词
        sql_keywords = {
            'select', 'from', 'where', 'and', 'or', 'insert', 'update', 'delete', 
            'create', 'alter', 'drop', 'truncate', 'table', 'join', 'on', 'set',
            'values', 'into', 'group', 'by', 'order', 'limit', 'having', 'like',
            'in', 'is', 'null', 'not', 'between', 'exists', 'as', 'distinct',
            'case', 'when', 'then', 'else', 'end', 'union', 'all', 'any', 'some'
        }
        
        # 提取所有单词并过滤
        words = re.findall(r'\b[a-zA-Z_]\w*\b', sql_clean)
        for word in words:
            word_lower = word.lower()
            if word_lower not in sql_keywords and len(word) > 2:
                return word
        
        return '未知表'
    
    @staticmethod
    def extract_table_aliases(sql: str) -> Dict[str, str]:
        """提取SQL中的表与别名映射"""
        alias_map: Dict[str, str] = {}
        if not sql:
            return alias_map
        
        sql_clean = sql.strip().rstrip(';')
        match = re.search(r'\bFROM\b\s+(.+)', sql_clean, re.IGNORECASE | re.DOTALL)
        if not match:
            return alias_map
        
        from_clause = match.group(1)
        stop_keywords = [' WHERE ', ' GROUP ', ' ORDER ', ' LIMIT ', ' HAVING ', ' UNION ', ' EXCEPT ', ' INTERSECT ']
        upper_clause = from_clause.upper()
        stop_index = len(from_clause)
        for keyword in stop_keywords:
            idx = upper_clause.find(keyword)
            if idx != -1 and idx < stop_index:
                stop_index = idx
        from_clause = from_clause[:stop_index]
        
        segments = re.split(r',|\bJOIN\b|\bLEFT\b|\bRIGHT\b|\bINNER\b|\bOUTER\b|\bFULL\b|\bCROSS\b', from_clause, flags=re.IGNORECASE)
        for segment in segments:
            seg = segment.strip()
            if not seg:
                continue
            tokens = seg.split()
            if not tokens:
                continue
            table_token = tokens[0].strip('`')
            if not table_token:
                continue
            alias = None
            if len(tokens) >= 2:
                next_token = tokens[1]
                if next_token.upper() not in {'ON', 'USING'}:
                    alias = next_token.strip('`')
            alias_key = alias or table_token
            alias_map[alias_key] = table_token
        
        return alias_map
    
    @staticmethod
    def sort_fields_by_priority(fields: List[str], sql_lower: str) -> List[str]:
        """
        智能排序字段优先级，基于字段类型和业务场景
        
        Args:
            fields: 字段列表
            sql_lower: 小写的SQL语句
            
        Returns:
            按优先级排序的字段列表
        """
        if not fields:
            return []
        
        # 字段优先级权重
        field_weights = {}
        
        for field in fields:
            field_lower = field.lower()
            weight = 0
            
            # 主键字段最高优先级
            if field_lower in ['id', 'pk', 'primary_key']:
                weight += 100
            elif field_lower.endswith('_id'):
                weight += 90
            
            # 时间字段较高优先级
            if field_lower in ['date', 'time', 'created', 'updated', 'timestamp']:
                weight += 80
            elif field_lower.endswith('_date') or field_lower.endswith('_time'):
                weight += 70
            
            # 状态/类型字段中等优先级
            if field_lower in ['status', 'state', 'type', 'category']:
                weight += 60
            elif field_lower.endswith('_status') or field_lower.endswith('_type'):
                weight += 50
            
            # 高频业务字段
            if field_lower in ['user', 'name', 'title', 'code']:
                weight += 40
            
            # 基础权重
            weight += len(field)  # 字段长度（短字段通常更重要）
            
            # 在SQL中的出现频率
            frequency = sql_lower.count(field_lower)
            weight += frequency * 5
            
            field_weights[field] = weight
        
        # 按权重降序排序
        sorted_fields = sorted(fields, key=lambda f: field_weights.get(f, 0), reverse=True)
        return sorted_fields
