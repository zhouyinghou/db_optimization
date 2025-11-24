#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能优化建议生成器
提供基于多维度分析的智能数据库优化建议
"""

import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime


class IntelligentOptimizationSuggestions:
    """智能优化建议生成器"""
    
    def __init__(self, db_helper=None):
        """
        初始化智能优化建议生成器
        
        Args:
            db_helper: 数据库助手实例，用于查询数据库信息
        """
        self.db_helper = db_helper
    
    def generate_comprehensive_suggestions(
        self,
        sql_content: str,
        database: str = '',
        table: str = '',
        query: Optional[dict] = None,
        hostname: str = None
    ) -> Dict[str, any]:
        """
        生成全面的智能优化建议
        
        Args:
            sql_content: SQL语句内容
            database: 数据库名
            table: 表名
            query: 查询对象，包含慢查询信息
            hostname: 主机名
            
        Returns:
            包含多维度优化建议的字典
        """
        if not sql_content:
            return self._empty_suggestions()
        
        sql_lower = sql_content.lower()
        
        # 1. SQL结构分析
        sql_analysis = self._analyze_sql_structure(sql_content, sql_lower)
        
        # 2. 索引分析
        index_analysis = self._analyze_index_requirements(
            sql_content, sql_lower, database, table, query, hostname
        )
        
        # 3. 表结构分析
        table_analysis = self._analyze_table_structure(
            database, table, query, hostname
        )
        
        # 4. 查询模式分析
        query_pattern_analysis = self._analyze_query_pattern(
            sql_content, sql_lower, query
        )
        
        # 5. 性能瓶颈识别
        performance_bottlenecks = self._identify_performance_bottlenecks(
            sql_analysis, index_analysis, table_analysis, query_pattern_analysis, query
        )
        
        # 6. 生成优化建议
        optimization_suggestions = self._generate_optimization_suggestions(
            sql_analysis,
            index_analysis,
            table_analysis,
            query_pattern_analysis,
            performance_bottlenecks,
            sql_content,
            database,
            table,
            query,
            hostname
        )
        
        # 7. 优先级评估
        priority_assessment = self._assess_optimization_priority(
            optimization_suggestions, query
        )
        
        # 8. 预期效果评估
        expected_impact = self._evaluate_expected_impact(
            optimization_suggestions, query, sql_analysis, index_analysis
        )
        
        return {
            'sql_analysis': sql_analysis,
            'index_analysis': index_analysis,
            'table_analysis': table_analysis,
            'query_pattern_analysis': query_pattern_analysis,
            'performance_bottlenecks': performance_bottlenecks,
            'optimization_suggestions': optimization_suggestions,
            'priority_assessment': priority_assessment,
            'expected_impact': expected_impact,
            'generation_time': datetime.now().isoformat()
        }
    
    def _analyze_sql_structure(self, sql_content: str, sql_lower: str) -> Dict:
        """分析SQL结构"""
        analysis = {
            'has_select_star': 'select *' in sql_lower,
            'has_where': 'where' in sql_lower,
            'has_join': bool(re.search(r'\bjoin\b', sql_lower)),
            'has_subquery': bool(re.search(r'select\s+[^)]+\s*\(', sql_lower, re.IGNORECASE)),
            'has_order_by': 'order by' in sql_lower,
            'has_group_by': 'group by' in sql_lower,
            'has_like': 'like' in sql_lower,
            'has_in': bool(re.search(r'\bin\s*\(', sql_lower)),
            'has_or': bool(re.search(r'\bor\b', sql_lower)),
            'has_function': self._detect_sql_functions(sql_content),
            'complexity_score': 0
        }
        
        # 计算复杂度分数
        complexity_factors = [
            analysis['has_join'],
            analysis['has_subquery'],
            analysis['has_order_by'],
            analysis['has_group_by'],
            analysis['has_or'],
            len(analysis['has_function']) > 0
        ]
        analysis['complexity_score'] = sum(complexity_factors)
        
        return analysis
    
    def _detect_sql_functions(self, sql_content: str) -> List[str]:
        """检测SQL中使用的函数"""
        function_patterns = [
            r'lower\s*\(', r'upper\s*\(', r'substring\s*\(', r'concat\s*\(',
            r'length\s*\(', r'trim\s*\(', r'ltrim\s*\(', r'rtrim\s*\(',
            r'abs\s*\(', r'ceil\s*\(', r'floor\s*\(', r'round\s*\(',
            r'mod\s*\(', r'rand\s*\(', r'now\s*\(', r'curdate\s*\(',
            r'curtime\s*\(', r'date\s*\(', r'time\s*\(', r'year\s*\(',
            r'month\s*\(', r'day\s*\('
        ]
        
        detected_functions = []
        for pattern in function_patterns:
            if re.search(pattern, sql_content, re.IGNORECASE):
                func_name = pattern.replace(r'\s*\(', '')
                detected_functions.append(func_name)
        
        return detected_functions
    
    def _analyze_index_requirements(
        self,
        sql_content: str,
        sql_lower: str,
        database: str,
        table: str,
        query: Optional[dict],
        hostname: str
    ) -> Dict:
        """分析索引需求"""
        analysis = {
            'where_fields': [],
            'join_fields': [],
            'order_by_fields': [],
            'group_by_fields': [],
            'existing_indexes': set(),
            'missing_indexes': [],
            'function_fields': [],
            'index_coverage': 'unknown'
        }
        
        # 提取WHERE字段
        if 'where' in sql_lower:
            where_pattern = r'where\s+([^;]+?)(?:\s+order\s+by|\s+group\s+by|\s+limit|\s+offset|\s+$)'
            where_match = re.search(where_pattern, sql_lower, re.IGNORECASE | re.DOTALL)
            if where_match:
                where_clause = where_match.group(1)
                field_pattern = r'(\w+)\s*(?:=|>|<|>=|<=|!=|<>|like|in|is|between)'
                analysis['where_fields'] = re.findall(field_pattern, where_clause, re.IGNORECASE)
        
        # 提取JOIN字段
        if 'join' in sql_lower:
            join_pattern = r'on\s+([\w\.]+)\s*=\s*([\w\.]+)'
            join_matches = re.findall(join_pattern, sql_lower, re.IGNORECASE)
            for match in join_matches:
                analysis['join_fields'].extend([match[0].split('.')[-1], match[1].split('.')[-1]])
        
        # 提取ORDER BY字段
        if 'order by' in sql_lower:
            order_pattern = r'order\s+by\s+([\w,\s]+?)(?:\s+limit|\s+offset|$)'
            order_match = re.search(order_pattern, sql_lower, re.IGNORECASE)
            if order_match:
                order_clause = order_match.group(1)
                analysis['order_by_fields'] = [field.strip() for field in order_clause.split(',')]
        
        # 提取GROUP BY字段
        if 'group by' in sql_lower:
            group_pattern = r'group\s+by\s+([\w,\s]+?)(?:\s+order\s+by|\s+limit|\s+offset|$)'
            group_match = re.search(group_pattern, sql_lower, re.IGNORECASE)
            if group_match:
                group_clause = group_match.group(1)
                analysis['group_by_fields'] = [field.strip() for field in group_clause.split(',')]
        
        # 检测函数字段
        function_patterns = [
            r'lower\s*\(', r'upper\s*\(', r'substring\s*\(', r'concat\s*\(',
            r'length\s*\(', r'trim\s*\(', r'ltrim\s*\(', r'rtrim\s*\(',
            r'abs\s*\(', r'ceil\s*\(', r'floor\s*\(', r'round\s*\(',
            r'mod\s*\(', r'rand\s*\(', r'now\s*\(', r'curdate\s*\(',
            r'curtime\s*\(', r'date\s*\(', r'time\s*\(', r'year\s*\(',
            r'month\s*\(', r'day\s*\('
        ]
        
        for field in analysis['where_fields']:
            for pattern in function_patterns:
                func_name = pattern.replace(r'\s*\(', '')
                if re.search(r'{}\s*\(\s*{}\s*\)'.format(func_name, field), sql_content, re.IGNORECASE):
                    analysis['function_fields'].append(field)
                    break
        
        # 获取现有索引信息（如果可能）
        if query and isinstance(query, dict) and 'table_structure' in query:
            table_structure = query.get('table_structure', {})
            if isinstance(table_structure, dict) and 'indexes' in table_structure:
                indexes = table_structure['indexes']
                if isinstance(indexes, dict):
                    for index_info in indexes.values():
                        if isinstance(index_info, dict) and 'columns' in index_info:
                            for col in index_info['columns']:
                                analysis['existing_indexes'].add(col.lower())
                elif isinstance(indexes, list):
                    for index_info in indexes:
                        if isinstance(index_info, dict):
                            if 'columns' in index_info:
                                for col in index_info['columns']:
                                    analysis['existing_indexes'].add(col.lower())
                            elif 'Column_name' in index_info:
                                analysis['existing_indexes'].add(index_info['Column_name'].lower())
        
        # 分析索引覆盖情况
        all_fields = set(analysis['where_fields'] + analysis['join_fields'])
        indexed_fields = set(f.lower() for f in all_fields if f.lower() in analysis['existing_indexes'])
        
        if len(all_fields) == 0:
            analysis['index_coverage'] = 'no_conditions'
        elif len(indexed_fields) == len(all_fields):
            analysis['index_coverage'] = 'fully_covered'
        elif len(indexed_fields) > 0:
            analysis['index_coverage'] = 'partially_covered'
        else:
            analysis['index_coverage'] = 'not_covered'
        
        # 识别缺失的索引
        for field in analysis['where_fields']:
            if field.lower() not in analysis['existing_indexes'] and field not in analysis['function_fields']:
                analysis['missing_indexes'].append(field)
        
        return analysis
    
    def _analyze_table_structure(
        self,
        database: str,
        table: str,
        query: Optional[dict],
        hostname: str
    ) -> Dict:
        """分析表结构"""
        analysis = {
            'row_count': None,
            'table_size': None,
            'engine': None,
            'has_primary_key': False,
            'index_count': 0,
            'needs_partitioning': False,
            'needs_archiving': False
        }
        
        # 从query对象获取表信息
        if query and isinstance(query, dict):
            if 'table_structure' in query:
                table_structure = query.get('table_structure', {})
                if isinstance(table_structure, dict):
                    analysis['row_count'] = table_structure.get('row_count')
                    analysis['table_size'] = table_structure.get('table_size')
                    analysis['engine'] = table_structure.get('engine')
                    analysis['index_count'] = len(table_structure.get('indexes', []))
                    analysis['has_primary_key'] = table_structure.get('has_primary_key', False)
        
        # 判断是否需要分区或归档
        if analysis['row_count']:
            if analysis['row_count'] > 10000000:  # 1000万行
                analysis['needs_partitioning'] = True
            if analysis['row_count'] > 5000000:  # 500万行
                analysis['needs_archiving'] = True
        
        return analysis
    
    def _analyze_query_pattern(
        self,
        sql_content: str,
        sql_lower: str,
        query: Optional[dict]
    ) -> Dict:
        """分析查询模式"""
        analysis = {
            'query_type': 'unknown',
            'execution_frequency': 0,
            'avg_query_time': 0,
            'max_query_time': 0,
            'total_executions': 0,
            'is_hot_query': False,
            'query_pattern': 'unknown'
        }
        
        # 从query对象获取执行信息
        if query and isinstance(query, dict):
            slow_info = query.get('slow_query_info', {})
            analysis['execution_frequency'] = query.get('execute_cnt', 0) or slow_info.get('ts_cnt', 0)
            analysis['avg_query_time'] = slow_info.get('query_time', 0) or query.get('query_time', 0)
            analysis['max_query_time'] = slow_info.get('query_time_max', 0) or slow_info.get('query_time', 0)
            analysis['total_executions'] = analysis['execution_frequency']
        
        # 判断查询类型
        if 'select' in sql_lower:
            if 'count(' in sql_lower:
                analysis['query_type'] = 'count'
            elif 'sum(' in sql_lower or 'avg(' in sql_lower or 'max(' in sql_lower or 'min(' in sql_lower:
                analysis['query_type'] = 'aggregation'
            elif 'join' in sql_lower:
                analysis['query_type'] = 'join'
            else:
                analysis['query_type'] = 'select'
        
        # 判断是否为热点查询
        if analysis['execution_frequency'] > 1000 or analysis['total_executions'] > 10000:
            analysis['is_hot_query'] = True
        
        # 识别查询模式
        if 'like' in sql_lower and '%' in sql_content:
            if sql_content.find('%') < sql_content.find("'") or sql_content[sql_content.find("'")+1:sql_content.find("'", sql_content.find("'")+1)].startswith('%'):
                analysis['query_pattern'] = 'prefix_like'
            else:
                analysis['query_pattern'] = 'suffix_like'
        elif 'in' in sql_lower:
            analysis['query_pattern'] = 'in_clause'
        elif 'between' in sql_lower:
            analysis['query_pattern'] = 'range_query'
        elif len(re.findall(r'\bor\b', sql_lower)) > 2:
            analysis['query_pattern'] = 'multiple_or'
        else:
            analysis['query_pattern'] = 'standard'
        
        return analysis
    
    def _identify_performance_bottlenecks(
        self,
        sql_analysis: Dict,
        index_analysis: Dict,
        table_analysis: Dict,
        query_pattern_analysis: Dict,
        query: Optional[dict]
    ) -> List[Dict]:
        """识别性能瓶颈"""
        bottlenecks = []
        
        # 1. 索引缺失瓶颈
        if not index_analysis['where_fields'] and not index_analysis['join_fields']:
            bottlenecks.append({
                'type': 'missing_filters',
                'severity': 'high',
                'description': '查询缺少有效的过滤条件，存在全表扫描风险',
                'impact': '全表扫描会导致CPU和IO压力显著增加'
            })
        if index_analysis['index_coverage'] == 'not_covered':
            bottlenecks.append({
                'type': 'missing_index',
                'severity': 'high',
                'description': 'WHERE条件中的字段缺少索引，可能导致全表扫描',
                'impact': '查询性能严重下降，数据量越大影响越明显'
            })
        elif index_analysis['index_coverage'] == 'partially_covered':
            bottlenecks.append({
                'type': 'partial_index',
                'severity': 'medium',
                'description': '部分WHERE条件字段缺少索引',
                'impact': '查询性能可能不够理想'
            })
        
        # 2. 函数使用瓶颈
        if index_analysis['function_fields']:
            bottlenecks.append({
                'type': 'function_on_indexed_field',
                'severity': 'high',
                'description': f"字段 {', '.join(index_analysis['function_fields'])} 在函数中使用，导致索引失效",
                'impact': '即使字段有索引也无法使用，查询性能严重下降'
            })
        
        # 3. SELECT * 瓶颈
        if sql_analysis['has_select_star']:
            bottlenecks.append({
                'type': 'select_star',
                'severity': 'medium',
                'description': '使用SELECT * 查询所有字段',
                'impact': '增加数据传输量和内存使用，影响查询性能'
            })
        
        # 4. 复杂查询瓶颈
        if sql_analysis['complexity_score'] >= 4:
            bottlenecks.append({
                'type': 'complex_query',
                'severity': 'medium',
                'description': '查询结构复杂，包含多个JOIN、子查询等',
                'impact': '查询执行计划可能不够优化'
            })
        
        # 5. OR条件瓶颈
        if sql_analysis['has_or']:
            bottlenecks.append({
                'type': 'or_conditions',
                'severity': 'medium',
                'description': 'WHERE条件中使用OR，可能导致索引失效',
                'impact': '查询优化器可能无法有效使用索引'
            })
        
        # 6. 大表瓶颈
        if table_analysis['row_count'] and table_analysis['row_count'] > 1000000:
            bottlenecks.append({
                'type': 'large_table',
                'severity': 'high' if table_analysis['row_count'] > 5000000 else 'medium',
                'description': f"表数据量较大（{table_analysis['row_count']:,}行）",
                'impact': '全表扫描或低效查询的性能影响会非常明显'
            })
        
        # 7. 热点查询瓶颈
        if query_pattern_analysis['is_hot_query']:
            bottlenecks.append({
                'type': 'hot_query',
                'severity': 'high',
                'description': f"高频查询（执行{query_pattern_analysis['execution_frequency']:,}次）",
                'impact': '即使小幅性能提升也能带来显著的整体收益'
            })
        
        return bottlenecks
    
    def _generate_optimization_suggestions(
        self,
        sql_analysis: Dict,
        index_analysis: Dict,
        table_analysis: Dict,
        query_pattern_analysis: Dict,
        performance_bottlenecks: List[Dict],
        sql_content: str,
        database: str,
        table: str,
        query: Optional[dict],
        hostname: str
    ) -> Dict[str, List[str]]:
        """生成优化建议"""
        suggestions = {
            'index_optimization': [],
            'sql_structure_optimization': [],
            'table_structure_optimization': [],
            'query_pattern_optimization': [],
            'configuration_optimization': [],
            'architecture_optimization': [],
            'executable_actions': []
        }
        
        # 1. 索引优化建议
        if not index_analysis['where_fields'] and not index_analysis['join_fields']:
            suggestions['sql_structure_optimization'].append(
                "建议添加包含索引的过滤条件"
            )
        elif index_analysis['missing_indexes']:
            if len(index_analysis['missing_indexes']) == 1:
                field = index_analysis['missing_indexes'][0]
                suggestions['index_optimization'].append(
                    f"为字段 {field} 创建单列索引"
                )
                suggestions['executable_actions'].append(
                    f"CREATE INDEX idx_{field} ON {table}({field});"
                )
            else:
                # 复合索引建议（最多5个字段）
                composite_fields = index_analysis['missing_indexes'][:5]
                fields_str = ', '.join(composite_fields)
                index_name = f"idx_{'_'.join(composite_fields)}_composite"
                suggestions['index_optimization'].append(
                    f"创建复合索引覆盖字段：{fields_str}"
                )
                suggestions['executable_actions'].append(
                    f"CREATE INDEX {index_name} ON {table}({fields_str});"
                )
        
        # 2. SQL结构优化建议
        if index_analysis['function_fields']:
            for field in index_analysis['function_fields']:
                suggestions['sql_structure_optimization'].append(
                    f"字段 {field} 在函数中使用导致索引失效，建议重写查询避免函数使用"
                )
        
        if sql_analysis['has_select_star']:
            suggestions['sql_structure_optimization'].append(
                "避免使用SELECT *，只查询需要的字段"
            )
        
        if sql_analysis['has_or']:
            suggestions['sql_structure_optimization'].append(
                "考虑将OR条件改写为UNION ALL，以提高索引使用效率"
            )
        
        if sql_analysis['has_subquery']:
            suggestions['sql_structure_optimization'].append(
                "考虑将子查询改写为JOIN操作，通常性能更好"
            )
        
        # 3. 表结构优化建议
        if table_analysis['needs_partitioning']:
            suggestions['table_structure_optimization'].append(
                f"表数据量超过1000万行，建议考虑按时间分区"
            )
        
        if table_analysis['needs_archiving']:
            suggestions['table_structure_optimization'].append(
                f"表数据量较大，建议实施历史数据归档策略"
            )
        
        # 4. 查询模式优化建议
        if query_pattern_analysis['query_pattern'] == 'prefix_like':
            suggestions['query_pattern_optimization'].append(
                "LIKE查询以%开头，无法使用索引，建议使用全文索引或改写查询"
            )
        
        if query_pattern_analysis['is_hot_query']:
            suggestions['query_pattern_optimization'].append(
                "高频查询，建议优先优化，考虑添加缓存层"
            )
        
        # 5. 配置优化建议
        if table_analysis['row_count'] and table_analysis['row_count'] > 1000000:
            suggestions['configuration_optimization'].append(
                "调整innodb_buffer_pool_size为物理内存的70-80%"
            )
            suggestions['configuration_optimization'].append(
                "优化query_cache_size和join_buffer_size参数"
            )
        
        # 6. 架构优化建议
        if query_pattern_analysis['is_hot_query']:
            suggestions['architecture_optimization'].append(
                "考虑读写分离，减轻主库压力"
            )
            suggestions['architecture_optimization'].append(
                "对热点数据实施Redis缓存策略"
            )
        
        return suggestions
    
    def _assess_optimization_priority(
        self,
        optimization_suggestions: Dict,
        query: Optional[dict]
    ) -> Dict:
        """评估优化优先级"""
        priority_scores = {
            'index_optimization': 0,
            'sql_structure_optimization': 0,
            'table_structure_optimization': 0,
            'query_pattern_optimization': 0,
            'configuration_optimization': 0,
            'architecture_optimization': 0
        }
        
        # 索引优化优先级
        if optimization_suggestions['index_optimization']:
            priority_scores['index_optimization'] = 90  # 最高优先级
        
        # SQL结构优化优先级
        if optimization_suggestions['sql_structure_optimization']:
            priority_scores['sql_structure_optimization'] = 80
        
        # 查询模式优化优先级
        if optimization_suggestions['query_pattern_optimization']:
            priority_scores['query_pattern_optimization'] = 70
        
        # 表结构优化优先级
        if optimization_suggestions['table_structure_optimization']:
            priority_scores['table_structure_optimization'] = 60
        
        # 配置优化优先级
        if optimization_suggestions['configuration_optimization']:
            priority_scores['configuration_optimization'] = 50
        
        # 架构优化优先级
        if optimization_suggestions['architecture_optimization']:
            priority_scores['architecture_optimization'] = 40
        
        # 根据查询频率调整优先级
        if query and isinstance(query, dict):
            execution_frequency = query.get('execute_cnt', 0) or query.get('slow_query_info', {}).get('ts_cnt', 0)
            if execution_frequency > 1000:
                # 高频查询，所有优化建议优先级提升
                for key in priority_scores:
                    if priority_scores[key] > 0:
                        priority_scores[key] = min(100, priority_scores[key] + 10)
        
        # 确定最高优先级
        max_priority = max(priority_scores.values())
        highest_priority_category = [k for k, v in priority_scores.items() if v == max_priority][0] if max_priority > 0 else None
        
        return {
            'priority_scores': priority_scores,
            'highest_priority': highest_priority_category,
            'recommended_order': sorted(
                [k for k, v in priority_scores.items() if v > 0],
                key=lambda x: priority_scores[x],
                reverse=True
            )
        }
    
    def _evaluate_expected_impact(
        self,
        optimization_suggestions: Dict,
        query: Optional[dict],
        sql_analysis: Dict,
        index_analysis: Dict
    ) -> Dict:
        """评估预期效果"""
        base_improvement = 0
        impact_factors = []
        
        # 索引优化效果
        if optimization_suggestions['index_optimization']:
            if index_analysis['index_coverage'] == 'not_covered':
                base_improvement += 60
                impact_factors.append('索引优化预计提升60-80%')
            elif index_analysis['index_coverage'] == 'partially_covered':
                base_improvement += 40
                impact_factors.append('索引优化预计提升40-60%')
        
        # SQL结构优化效果
        if optimization_suggestions['sql_structure_optimization']:
            base_improvement += 30
            impact_factors.append('SQL结构优化预计提升20-40%')
        
        # 查询模式优化效果
        if optimization_suggestions['query_pattern_optimization']:
            base_improvement += 25
            impact_factors.append('查询模式优化预计提升20-35%')
        
        # 表结构优化效果
        if optimization_suggestions['table_structure_optimization']:
            base_improvement += 20
            impact_factors.append('表结构优化预计提升15-30%')
        
        # 配置优化效果
        if optimization_suggestions['configuration_optimization']:
            base_improvement += 15
            impact_factors.append('配置优化预计提升10-25%')
        
        # 架构优化效果
        if optimization_suggestions['architecture_optimization']:
            base_improvement += 25
            impact_factors.append('架构优化预计提升20-40%')
        
        # 计算总体预期提升
        min_improvement = max(50, base_improvement - 20)
        max_improvement = min(95, base_improvement + 25)
        
        # 获取查询时间信息
        avg_query_time_ms = 0
        if query and isinstance(query, dict):
            slow_info = query.get('slow_query_info', {})
            avg_query_time_ms = slow_info.get('query_time_max', 0) or slow_info.get('query_time', 0) or query.get('query_time', 0)
        
        avg_query_time_sec = avg_query_time_ms / 1000.0 if avg_query_time_ms > 0 else 0.02
        
        # 计算优化后的时间
        avg_improvement = (min_improvement + max_improvement) / 2.0
        improved_time_sec = avg_query_time_sec * (1 - avg_improvement / 100)
        improved_time_sec = max(0.001, improved_time_sec)
        
        # 性能提升倍数
        performance_multiplier = max(1.5, min(500, avg_query_time_sec / improved_time_sec)) if avg_query_time_sec > 0 else 5.0
        
        return {
            'performance_improvement_range': f"{min_improvement}-{max_improvement}%",
            'expected_query_time_reduction': f"{avg_query_time_sec*1000:.0f}ms -> {improved_time_sec*1000:.0f}ms",
            'performance_multiplier': f"{performance_multiplier:.1f}x",
            'impact_factors': impact_factors,
            'overall_assessment': 'high' if base_improvement >= 60 else 'medium' if base_improvement >= 30 else 'low'
        }
    
    def _empty_suggestions(self) -> Dict:
        """返回空的建议结构"""
        return {
            'sql_analysis': {},
            'index_analysis': {},
            'table_analysis': {},
            'query_pattern_analysis': {},
            'performance_bottlenecks': [],
            'optimization_suggestions': {
                'index_optimization': [],
                'sql_structure_optimization': [],
                'table_structure_optimization': [],
                'query_pattern_optimization': [],
                'configuration_optimization': [],
                'architecture_optimization': [],
                'executable_actions': []
            },
            'priority_assessment': {},
            'expected_impact': {},
            'generation_time': datetime.now().isoformat()
        }
    
    def format_suggestions_for_report(self, comprehensive_suggestions: Dict) -> str:
        """
        将综合建议格式化为报告格式的字符串
        
        Args:
            comprehensive_suggestions: 综合建议字典
            
        Returns:
            格式化的建议字符串
        """
        if not comprehensive_suggestions or not comprehensive_suggestions.get('optimization_suggestions'):
            return "暂无优化建议"
        
        parts = []
        suggestions = comprehensive_suggestions['optimization_suggestions']
        
        # 1. 智能诊断
        bottlenecks = comprehensive_suggestions.get('performance_bottlenecks', [])
        if bottlenecks:
            bottleneck_descriptions = [b['description'] for b in bottlenecks[:3]]
            parts.append(f"1. 智能诊断：{'；'.join(bottleneck_descriptions)}")
        else:
            parts.append("1. 智能诊断：SQL语句结构良好，但仍有优化空间")
        
        # 2. 智能优化建议
        parts.append("2. 智能优化建议：")
        
        # 索引优化建议
        if suggestions['index_optimization']:
            parts.append("**索引优化（最优建议）：**")
            parts.append("```sql")
            for action in comprehensive_suggestions['optimization_suggestions']['executable_actions']:
                if 'CREATE INDEX' in action:
                    parts.append(action)
            parts.append("```")
        
        # SQL结构优化建议
        if suggestions['sql_structure_optimization']:
            for suggestion in suggestions['sql_structure_optimization'][:2]:
                parts.append(f"• {suggestion}")
        
        # 查询模式优化建议
        if suggestions['query_pattern_optimization']:
            for suggestion in suggestions['query_pattern_optimization'][:2]:
                parts.append(f"• {suggestion}")
        
        # 3. 预期效果
        expected_impact = comprehensive_suggestions.get('expected_impact', {})
        if expected_impact:
            query_time_reduction = expected_impact.get('expected_query_time_reduction', '')
            performance_multiplier = expected_impact.get('performance_multiplier', '')
            if query_time_reduction and performance_multiplier:
                parts.append(f"3. 预期效果：预计查询时间从{query_time_reduction.split(' -> ')[0]}降低到{query_time_reduction.split(' -> ')[1]}，性能提升约{performance_multiplier}")
            else:
                improvement_range = expected_impact.get('performance_improvement_range', '50-80%')
                parts.append(f"3. 预期效果：预计性能提升{improvement_range}")
        
        return "\n".join(parts)

