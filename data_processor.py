#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据处理模块
包含数据过滤、合并、转换等处理方法
"""

import re
from typing import Dict, List
from sql_analyzer import SQLAnalyzer


class DataProcessor:
    """数据处理类"""
    
    @staticmethod
    def filter_excluded_tables(queries: List[Dict], excluded_tables: List[str]) -> List[Dict]:
        """
        过滤掉包含排除表名的查询
        
        Args:
            queries: 查询列表
            excluded_tables: 需要排除的表名列表
            
        Returns:
            过滤后的查询列表
        """
        filtered_queries = []
        
        for query in queries:
            sql = query.get('sql', query.get('sql_content', '')).lower()
            # 检查SQL是否包含任何需要排除的表名
            contains_excluded_table = False
            
            for excluded_table in excluded_tables:
                # 使用正则表达式确保匹配的是完整的表名，避免部分匹配
                pattern = r'\b' + re.escape(excluded_table.lower()) + r'\b'
                if re.search(pattern, sql):
                    contains_excluded_table = True
                    break
            
            # 如果不包含排除的表名，则保留该查询
            if not contains_excluded_table:
                filtered_queries.append(query)
        
        return filtered_queries
    
    @staticmethod
    def format_deepseek_suggestions(deepseek_optimization, sql_content: str = '') -> str:
        """智能格式化DeepSeek优化建议，只给出最优的一条复合索引建议"""
        if not deepseek_optimization:
            return "暂无优化建议"
        
        # 获取原始建议
        raw_suggestions = ''
        if isinstance(deepseek_optimization, list):
            raw_suggestions = '\n'.join(deepseek_optimization)
        elif isinstance(deepseek_optimization, str):
            raw_suggestions = deepseek_optimization
        else:
            raw_suggestions = str(deepseek_optimization)
        
        # 分析SQL内容，提取字段信息
        if sql_content:
            where_fields = SQLAnalyzer.extract_where_fields(sql_content)
            join_fields = SQLAnalyzer.extract_join_fields(sql_content)
            order_by_fields = SQLAnalyzer.extract_order_by_fields(sql_content)
            
            # 合并所有字段，优先使用WHERE字段
            all_fields = where_fields[:5]  # 最多取5个WHERE字段
            if len(all_fields) < 5:
                # 如果WHERE字段不足，用JOIN字段补充
                for field in join_fields:
                    if field not in all_fields and len(all_fields) < 5:
                        all_fields.append(field)
            
            if len(all_fields) < 5:
                # 如果还不够，用ORDER BY字段补充
                for field in order_by_fields:
                    if field not in all_fields and len(all_fields) < 5:
                        all_fields.append(field)
            
            # 如果提取到了字段，尝试从建议中提取索引创建语句
            if all_fields:
                # 查找CREATE INDEX语句
                index_pattern = r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+[^\s]+\s+ON\s+[^\s]+\s*\(([^)]+)\)'
                index_matches = re.findall(index_pattern, raw_suggestions, re.IGNORECASE)
                
                if index_matches:
                    # 使用第一个匹配的索引建议
                    index_fields = index_matches[0].strip()
                    return f"建议创建复合索引: ({index_fields})"
        
        # 如果没有找到索引建议，返回原始建议的第一条
        suggestions_list = []
        if isinstance(deepseek_optimization, list):
            suggestions_list = deepseek_optimization
        elif isinstance(deepseek_optimization, str):
            # 按换行符分割字符串
            suggestions_list = [s.strip() for s in deepseek_optimization.split('\n') if s.strip()]
        
        # 只返回第一条建议
        if suggestions_list:
            return suggestions_list[0]
        
        return raw_suggestions
    
    @staticmethod
    def convert_analysis_to_queries(analysis_results: List[Dict], format_suggestions_func) -> List[Dict]:
        """将分析结果转换为查询列表格式"""
        queries = []
        for result in analysis_results:
            query = {
                'sql': result.get('sql', ''),
                'sql_content': result.get('sql', ''),
                'db_name': result.get('database', ''),
                'database': result.get('database', ''),
                'table': result.get('table', ''),
                'deepseek_optimization': result.get('deepseek_optimization', ''),
                'optimization_suggestions': format_suggestions_func(
                    result.get('deepseek_optimization', ''), 
                    result.get('sql', '')
                ),
                'table_structure': result.get('table_structure', {}),
                'explain_result': result.get('explain_result', {}),
                'analysis_time': result.get('analysis_time', ''),
                'slow_query_info': result.get('slow_query_info', {})
            }
            
            queries.append(query)
        
        return queries
    
    @staticmethod
    def merge_analysis_results_to_compare_data(compare_data: Dict, analysis_results: List[Dict], format_suggestions_func):
        """将DeepSeek分析结果合并到compare_data结构中"""
        try:
            # 将分析结果按SQL内容映射到字典，便于快速查找
            analysis_dict = {}
            for result in analysis_results:
                sql = result.get('sql', '')
                if sql:
                    analysis_dict[sql] = result
            
            # 合并到last_month的查询中
            if compare_data and 'last_month' in compare_data and 'queries' in compare_data['last_month']:
                merged_count = 0
                for query in compare_data['last_month']['queries']:
                    sql_content = query.get('sql_content', query.get('sql', ''))
                    if sql_content in analysis_dict:
                        analysis_result = analysis_dict[sql_content]
                        # 添加DeepSeek分析结果
                        query['deepseek_optimization'] = analysis_result.get('deepseek_optimization', '')
                        query['optimization_suggestions'] = format_suggestions_func(
                            analysis_result.get('deepseek_optimization', ''), 
                            sql_content
                        )
                        query['table_structure'] = analysis_result.get('table_structure', {})
                        query['explain_result'] = analysis_result.get('explain_result', {})
                        query['analysis_time'] = analysis_result.get('analysis_time', '')
                        merged_count += 1
                
                # 如果没有找到匹配的查询，直接替换整个查询列表
                if merged_count == 0:
                    compare_data['last_month']['queries'] = DataProcessor.convert_analysis_to_queries(
                        analysis_results, format_suggestions_func
                    )
            
            # 合并到previous_month的查询中
            if compare_data and 'previous_month' in compare_data and 'queries' in compare_data['previous_month']:
                merged_count = 0
                for query in compare_data['previous_month']['queries']:
                    sql_content = query.get('sql_content', query.get('sql', ''))
                    if sql_content in analysis_dict:
                        analysis_result = analysis_dict[sql_content]
                        # 添加DeepSeek分析结果
                        query['deepseek_optimization'] = analysis_result.get('deepseek_optimization', '')
                        query['optimization_suggestions'] = format_suggestions_func(
                            analysis_result.get('deepseek_optimization', ''), 
                            sql_content
                        )
                        query['table_structure'] = analysis_result.get('table_structure', {})
                        query['explain_result'] = analysis_result.get('explain_result', {})
                        query['analysis_time'] = analysis_result.get('analysis_time', '')
                        merged_count += 1
                
                # 如果没有找到匹配的查询，创建空列表
                if merged_count == 0:
                    compare_data['previous_month']['queries'] = []
            
        except Exception as e:
            print(f"合并分析结果失败: {e}")
    
    @staticmethod
    def create_compare_data_with_analysis(analysis_results: List[Dict], format_suggestions_func) -> Dict:
        """创建包含DeepSeek分析结果的compare_data结构"""
        try:
            # 计算统计信息
            total_count = len(analysis_results)
            total_execute_cnt = 0
            total_query_time = 0
            
            # 为每个分析结果创建查询结构
            queries = []
            for result in analysis_results:
                query = {
                    'sql': result.get('sql', ''),
                    'sql_content': result.get('sql', ''),
                    'db_name': result.get('database', ''),
                    'database': result.get('database', ''),
                    'table': result.get('table', ''),
                    'deepseek_optimization': result.get('deepseek_optimization', ''),
                    'optimization_suggestions': format_suggestions_func(
                        result.get('deepseek_optimization', ''), 
                        result.get('sql', '')
                    ),
                    'table_structure': result.get('table_structure', {}),
                    'explain_result': result.get('explain_result', {}),
                    'analysis_time': result.get('analysis_time', ''),
                    'slow_query_info': result.get('slow_query_info', {})
                }
                
                # 提取执行次数和查询时间（如果可用）
                if 'slow_query_info' in result:
                    slow_info = result['slow_query_info']
                    query['execute_cnt'] = slow_info.get('execute_cnt', 0)
                    query['query_time'] = slow_info.get('query_time', 0)
                    total_execute_cnt += int(query['execute_cnt'])
                    total_query_time += float(query['query_time'])
                
                queries.append(query)
            
            avg_query_time = total_query_time / total_count if total_count > 0 else 0
            
            compare_data = {
                'last_month': {
                    'name': '当前分析周期',
                    'total_count': total_count,
                    'total_execute_cnt': total_execute_cnt,
                    'avg_query_time': avg_query_time,
                    'queries': queries
                },
                'previous_month': {
                    'name': '上一周期',
                    'total_count': 0,
                    'total_execute_cnt': 0,
                    'avg_query_time': 0,
                    'queries': []
                },
                'comparison': {
                    'count_change': 0,
                    'execute_cnt_change': 0,
                    'time_change': 0,
                    'growth_rate': 0
                }
            }
            
            return compare_data
            
        except Exception as e:
            print(f"创建分析数据失败: {e}")
            # 如果失败，返回一个空的结构
            return {
                'last_month': {'name': '当前分析周期', 'total_count': 0, 'total_execute_cnt': 0, 'avg_query_time': 0, 'queries': []},
                'previous_month': {'name': '上一周期', 'total_count': 0, 'total_execute_cnt': 0, 'avg_query_time': 0, 'queries': []},
                'comparison': {'count_change': 0, 'execute_cnt_change': 0, 'time_change': 0, 'growth_rate': 0}
            }

