#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据脱敏模块
处理敏感数据的脱敏操作，包括数据库名、IP地址、表名、SQL语句等
"""

import re
from typing import Dict, List


class DataMasking:
    """数据脱敏处理类"""
    
    @staticmethod
    def mask_db_name(db_name) -> str:
        """脱敏数据库名：长度小于6位时保留头两位和尾两位，其余用*替换；长度大于等于6位时保留头3位和尾3位"""
        # 处理空值或None值
        if not db_name or db_name == 'None':
            return '未知'
        
        # 确保输入是字符串
        if not isinstance(db_name, str):
            db_name = str(db_name)
        
        # 处理空字符串
        if not db_name.strip():
            return '未知'
        
        # 对于所有长度的数据库名都进行脱敏处理
        if len(db_name) <= 4:
            # 如果长度小于等于4，显示第1位和最后1位，中间用*替换
            if len(db_name) <= 2:
                return db_name  # 长度小于等于2，不脱敏
            return f"{db_name[0]}{'*' * (len(db_name) - 2)}{db_name[-1]}"
        elif len(db_name) < 6:
            # 如果长度为5，显示前2位和后2位，中间用*替换
            return f"{db_name[:2]}{'*' * (len(db_name) - 4)}{db_name[-2:]}"
        elif len(db_name) == 6:
            # 如果长度等于6，显示前2位和后2位，中间用*替换
            return f"{db_name[:2]}{'*' * (len(db_name) - 4)}{db_name[-2:]}"
        else:
            # 严格显示头3位和尾3位，中间用*替换
            return f"{db_name[:3]}{'*' * (len(db_name) - 6)}{db_name[-3:]}"
    
    @staticmethod
    def mask_ip(ip) -> str:
        """脱敏IP地址：长度小于6位时保留头两位和尾两位，其余用*替换；长度大于等于6位时保留头3位和尾3位"""
        # 确保输入是字符串
        if not isinstance(ip, str):
            ip = str(ip)
        
        if len(ip) <= 4:
            # 如果长度小于等于4，显示第1位和最后1位，中间用*替换
            if len(ip) <= 2:
                return ip  # 长度小于等于2，不脱敏
            return f"{ip[0]}{'*' * (len(ip) - 2)}{ip[-1]}"
        elif len(ip) < 6:
            # 如果长度为5，显示前2位和后2位，中间用*替换
            return f"{ip[:2]}{'*' * (len(ip) - 4)}{ip[-2:]}"
        elif len(ip) == 6:
            # 如果长度等于6，显示前2位和后2位，中间用*替换
            return f"{ip[:2]}{'*' * (len(ip) - 4)}{ip[-2:]}"
        else:
            # 严格显示头3位和尾3位，中间用*替换
            return f"{ip[:3]}{'*' * (len(ip) - 6)}{ip[-3:]}"
    
    @staticmethod
    def mask_table_name(table_name) -> str:
        """脱敏表名：长度小于6位时保留头两位和尾两位，其余用*替换；长度大于等于6位时保留头3位和尾3位"""
        # 确保输入是字符串
        if not isinstance(table_name, str):
            table_name = str(table_name)
        
        # 将表名转换为小写
        table_name = table_name.lower()
        
        if len(table_name) <= 4:
            # 如果长度小于等于4，显示第1位和最后1位，中间用*替换
            if len(table_name) <= 2:
                return table_name  # 长度小于等于2，不脱敏
            return f"{table_name[0]}{'*' * (len(table_name) - 2)}{table_name[-1]}"
        elif len(table_name) < 6:
            # 如果长度为5，显示前2位和后2位，中间用*替换
            return f"{table_name[:2]}{'*' * (len(table_name) - 4)}{table_name[-2:]}"
        elif len(table_name) == 6:
            # 如果长度等于6，显示前2位和后2位，中间用*替换
            return f"{table_name[:2]}{'*' * (len(table_name) - 4)}{table_name[-2:]}"
        else:
            # 严格显示头3位和尾3位，中间用*替换
            return f"{table_name[:3]}{'*' * (len(table_name) - 6)}{table_name[-3:]}"
    
    @staticmethod
    def mask_sql(sql) -> str:
        """脱敏SQL语句中的敏感信息，处理可能的非字符串输入"""
        # 确保输入是字符串
        if not isinstance(sql, str):
            sql = str(sql)
        
        # 脱敏表名模式匹配
        table_patterns = [
            r'(FROM|JOIN)\s+([`\[\"]?\w+[`\]\"]?)',
            r'(ALTER|CREATE|DROP|TRUNCATE)\s+TABLE\s+([`\[\"]?\w+[`\]\"]?)',
            r'(INSERT\s+INTO|UPDATE)\s+([`\[\"]?\w+[`\]\"]?)'
        ]
        
        masked_sql = sql
        
        for pattern in table_patterns:
            matches = re.finditer(pattern, masked_sql, re.IGNORECASE)
            # 从后向前替换，避免位置偏移
            replacements = []
            for match in matches:
                prefix = match.group(1)
                table_name = match.group(2)
                # 移除可能的引号
                clean_table = re.sub(r'[`\[\"]', '', table_name)
                masked_table = DataMasking.mask_table_name(clean_table)
                # 恢复引号
                if table_name.startswith('`') and table_name.endswith('`'):
                    masked_table = f'`{masked_table}`'
                elif table_name.startswith('[') and table_name.endswith(']'):
                    masked_table = f'[{masked_table}]'
                elif table_name.startswith('"') and table_name.endswith('"'):
                    masked_table = f'"{masked_table}"'
                
                replacements.append((match.start(), match.end(), f'{prefix} {masked_table}'))
            
            # 从后向前替换
            for start, end, replacement in reversed(replacements):
                masked_sql = masked_sql[:start] + replacement + masked_sql[end:]
        
        # 脱敏数据库名
        db_pattern = r'(\`\w+\`|\w+)\.(\`\w+\`|\w+)'  # 匹配db.table格式
        matches = re.finditer(db_pattern, masked_sql)
        replacements = []
        
        for match in matches:
            db_name = match.group(1)
            table_name = match.group(2)
            
            # 移除可能的引号
            clean_db = re.sub(r'[`\[\"]', '', db_name)
            clean_table = re.sub(r'[`\[\"]', '', table_name)
            
            masked_db = DataMasking.mask_db_name(clean_db)
            masked_table = DataMasking.mask_table_name(clean_table)
            
            # 恢复引号
            if db_name.startswith('`') and db_name.endswith('`'):
                masked_db = f'`{masked_db}`'
            if table_name.startswith('`') and table_name.endswith('`'):
                masked_table = f'`{masked_table}`'
            
            replacements.append((match.start(), match.end(), f'{masked_db}.{masked_table}'))
        
        # 从后向前替换
        for start, end, replacement in reversed(replacements):
            masked_sql = masked_sql[:start] + replacement + masked_sql[end:]
        
        return masked_sql
    
    @staticmethod
    def mask_table_structure(table_structure) -> str:
        """脱敏表结构信息，处理可能的非字符串输入"""
        # 确保输入是字符串
        if not isinstance(table_structure, str):
            table_structure = str(table_structure)
        
        # 脱敏表名
        masked_structure = re.sub(
            r'(CREATE\s+TABLE\s+)([`\[\"]?\w+[`\]\"]?)',
            lambda m: m.group(1) + DataMasking.mask_table_name(re.sub(r'[`\[\"]', '', m.group(2))),
            table_structure, 
            flags=re.IGNORECASE
        )
        
        return masked_structure
    
    @staticmethod
    def mask_sensitive_data(data: List[Dict]) -> List[Dict]:
        """对敏感信息进行脱敏处理"""
        masked_data = []
        
        for item in data:
            # 创建深拷贝，避免修改原始数据
            masked_item = item.copy()
            
            # 脱敏数据库名
            if 'slow_query_info' in masked_item and 'db_name' in masked_item['slow_query_info']:
                masked_item['slow_query_info']['db_name'] = DataMasking.mask_db_name(
                    masked_item['slow_query_info']['db_name']
                )
            
            # 脱敏IP地址
            if 'slow_query_info' in masked_item and 'ip' in masked_item['slow_query_info']:
                masked_item['slow_query_info']['ip'] = DataMasking.mask_ip(
                    masked_item['slow_query_info']['ip']
                )
            
            # 脱敏表名
            if 'table' in masked_item:
                masked_item['table'] = DataMasking.mask_table_name(masked_item['table'])
            
            # 脱敏SQL语句中的敏感信息（表名、数据库名等）
            if 'sql' in masked_item:
                masked_item['sql'] = DataMasking.mask_sql(masked_item['sql'])
            
            # 脱敏表结构信息
            if 'table_structure' in masked_item:
                masked_item['table_structure'] = DataMasking.mask_table_structure(
                    masked_item['table_structure']
                )
            
            masked_data.append(masked_item)
        
        return masked_data
