"""
MySQL慢查询优化工具
使用LangChain和LLM分析SQL语句并提供优化建议

安装依赖:
    pip install langchain langchain-openai pymysql requests

或者使用Deepseek:
    pip install langchain langchain-community pymysql requests
"""

import json
import mysql.connector
from typing import Dict, List, Optional
import os

# 尝试导入LangChain相关模块
try:
    from langchain.prompts import PromptTemplate
    from langchain.chains import LLMChain
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False

import requests


class MySQLSlowQueryOptimizer:
    """MySQL慢查询优化器"""
    
    def __init__(self, db_config_path: str = 'db_config.json', api_key: Optional[str] = None):
        """
        初始化优化器
        
        Args:
            db_config_path: 数据库配置文件路径
            api_key: LLM API密钥（如果使用OpenAI等需要API key的服务）
        """
        self.db_config_path = db_config_path
        self.api_key = api_key or os.getenv('OPENAI_API_KEY') or os.getenv('DEEPSEEK_API_KEY')
        self.db_configs = self._load_db_configs()
        
    def _load_db_configs(self) -> List[Dict]:
        """加载数据库配置"""
        try:
            with open(self.db_config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return []
    
    def _get_db_connection(self, database: str) -> Optional[mysql.connector.connection.MySQLConnection]:
        """
        根据数据库名获取连接
        
        Args:
            database: 数据库名
            
        Returns:
            数据库连接对象，如果找不到配置则返回None
        """
        for config in self.db_configs:
            if config.get('database') == database:
                try:
                    conn = mysql.connector.connect(
                        host=config['host'],
                        port=config.get('port', 3306),
                        user=config['user'],
                        password=config['password'],
                        database=database,
                        charset='utf8mb4',
                        collation='utf8mb4_general_ci',
                        connection_timeout=10
                    )
                    return conn
                except Exception as e:
                    print(f"连接数据库 {database} 失败: {e}")
                    return None
        print(f"未找到数据库 {database} 的配置")
        return None
    
    def get_table_structure(self, database: str, table: str) -> Dict:
        """
        获取表结构信息
        
        Args:
            database: 数据库名
            table: 表名
            
        Returns:
            包含表结构信息的字典
        """
        conn = self._get_db_connection(database)
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            structure_info = {
                'columns': [],
                'indexes': [],
                'table_status': {}
            }
            
            # 获取列信息
            cursor.execute(f"SHOW COLUMNS FROM `{table}`")
            columns = cursor.fetchall()
            for col in columns:
                structure_info['columns'].append({
                    'field': col[0],
                    'type': col[1],
                    'null': col[2],
                    'key': col[3],
                    'default': col[4],
                    'extra': col[5]
                })
            
            # 获取索引信息
            cursor.execute(f"SHOW INDEX FROM `{table}`")
            indexes = cursor.fetchall()
            index_dict = {}
            for idx in indexes:
                idx_name = idx[2]
                if idx_name not in index_dict:
                    index_dict[idx_name] = {
                        'name': idx_name,
                        'unique': idx[1] == 0,
                        'columns': []
                    }
                index_dict[idx_name]['columns'].append({
                    'column': idx[4],
                    'seq': idx[3]
                })
            structure_info['indexes'] = list(index_dict.values())
            
            # 获取表状态信息（行数、大小等）
            cursor.execute(f"SHOW TABLE STATUS LIKE '{table}'")
            status = cursor.fetchone()
            if status:
                structure_info['table_status'] = {
                    'rows': status[4],
                    'data_length': status[6],
                    'index_length': status[8],
                    'engine': status[1]
                }
            
            cursor.close()
            return structure_info
            
        except Exception as e:
            print(f"获取表结构失败: {e}")
            return {}
        finally:
            conn.close()
    
    def explain_query(self, database: str, sql: str) -> Dict:
        """
        执行EXPLAIN分析SQL
        
        Args:
            database: 数据库名
            sql: SQL语句
            
        Returns:
            EXPLAIN结果
        """
        conn = self._get_db_connection(database)
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            explain_sql = f"EXPLAIN {sql}"
            cursor.execute(explain_sql)
            explain_result = cursor.fetchall()
            
            # 获取列名
            columns = [desc[0] for desc in cursor.description]
            
            # 转换为字典列表
            explain_data = []
            for row in explain_result:
                explain_data.append(dict(zip(columns, row)))
            
            cursor.close()
            return {'explain': explain_data, 'columns': columns}
            
        except Exception as e:
            print(f"EXPLAIN执行失败: {e}")
            return {}
        finally:
            conn.close()
    
    def analyze_sql_with_llm(self, sql: str, database: str, table: str, 
                            table_structure: Dict, explain_result: Dict) -> str:
        """
        使用LLM分析SQL并提供优化建议
        
        Args:
            sql: SQL语句
            database: 数据库名
            table: 表名
            table_structure: 表结构信息
            explain_result: EXPLAIN结果
            
        Returns:
            优化建议文本
        """
        # 构建提示词模板
        if LANGCHAIN_AVAILABLE:
            prompt_template = PromptTemplate(
                input_variables=["database", "table", "sql", "table_structure", "explain_result"],
                template="""你是一位资深的MySQL数据库优化专家。请分析以下SQL查询并提供详细的优化建议。

数据库名: {database}
表名: {table}

SQL语句:
{sql}

表结构信息:
{table_structure}

EXPLAIN执行计划:
{explain_result}

请从以下方面进行分析和优化建议:
1. 索引使用情况分析（是否使用了合适的索引，是否存在全表扫描）
2. WHERE子句优化（条件顺序、索引列使用）
3. JOIN优化（JOIN顺序、JOIN类型选择）
4. SELECT字段优化（是否使用了SELECT *，是否可以减少字段）
5. 子查询优化（是否可以改写为JOIN）
6. 排序和分组优化（ORDER BY、GROUP BY的索引使用）
7. LIMIT优化（分页查询优化）
8. 数据类型和函数使用优化
9. 查询重写建议（提供优化后的SQL示例）

请用中文回答，格式清晰，提供具体的优化建议和优化后的SQL示例（如果适用）。
"""
            )
        else:
            prompt_template_str = """你是一位资深的MySQL数据库优化专家。请分析以下SQL查询并提供详细的优化建议。

数据库名: {database}
表名: {table}

SQL语句:
{sql}

表结构信息:
{table_structure}

EXPLAIN执行计划:
{explain_result}

请从以下方面进行分析和优化建议:
1. 索引使用情况分析（是否使用了合适的索引，是否存在全表扫描）
2. WHERE子句优化（条件顺序、索引列使用）
3. JOIN优化（JOIN顺序、JOIN类型选择）
4. SELECT字段优化（是否使用了SELECT *，是否可以减少字段）
5. 子查询优化（是否可以改写为JOIN）
6. 排序和分组优化（ORDER BY、GROUP BY的索引使用）
7. LIMIT优化（分页查询优化）
8. 数据类型和函数使用优化
9. 查询重写建议（提供优化后的SQL示例）

请用中文回答，格式清晰，提供具体的优化建议和优化后的SQL示例（如果适用）。
"""
        
        # 格式化输入
        table_structure_str = json.dumps(table_structure, ensure_ascii=False, indent=2)
        explain_result_str = json.dumps(explain_result, ensure_ascii=False, indent=2)
        
        # 使用Deepseek API（项目默认使用Deepseek）
        try:
            api_key = self.api_key or os.getenv('DEEPSEEK_API_KEY') or "sk-0745b17c589b4074a2f9d9e88f83bb76"
            
            if LANGCHAIN_AVAILABLE:
                # 使用LangChain的PromptTemplate格式化
                prompt = prompt_template.format(
                    database=database,
                    table=table,
                    sql=sql,
                    table_structure=table_structure_str,
                    explain_result=explain_result_str
                )
            else:
                # 直接格式化字符串
                prompt = prompt_template_str.format(
                    database=database,
                    table=table,
                    sql=sql,
                    table_structure=table_structure_str,
                    explain_result=explain_result_str
                )
            
            response = requests.post(
                "https://api.deepseek.com/v1/chat/completions",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {api_key}"
                },
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {"role": "system", "content": "你是一位资深的MySQL数据库优化专家。"},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.3
                },
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()
                return result['choices'][0]['message']['content']
            else:
                return f"API调用失败: {response.status_code}, {response.text}"
                
        except Exception as e:
            return f"LLM分析失败: {str(e)}"
    
    def optimize_query(self, sql: str, database: str, table: str) -> Dict:
        """
        优化查询的主方法
        
        Args:
            sql: SQL语句
            database: 数据库名
            table: 表名
            
        Returns:
            包含优化建议的字典
        """
        # 1. 获取表结构
        table_structure = self.get_table_structure(database, table)
        if not table_structure:
            return {"error": "无法获取表结构信息"}
        
        # 2. 执行EXPLAIN
        explain_result = self.explain_query(database, sql)
        if not explain_result:
            return {"error": "EXPLAIN执行失败"}
        
        # 3. 使用LLM分析（这里不再调用，因为会在analyze_slow_queries中调用DeepSeek）
        optimization_suggestions = ""
        
        return {
            "sql": sql,
            "database": database,
            "table": table,
            "table_structure": table_structure,
            "explain_result": explain_result,
            "optimization_suggestions": optimization_suggestions
        }
    
    def print_optimization_report(self, result: Dict):
        """
        打印优化报告
        
        Args:
            result: optimize_query返回的结果
        """
        if "error" in result:
            print(f"错误: {result['error']}")
            return
        
        print("=" * 80)
        print("MySQL慢查询优化报告")
        print("=" * 80)
        print(f"\n数据库: {result['database']}")
        print(f"表名: {result['table']}")
        print(f"\n原始SQL:\n{result['sql']}\n")
        
        print("-" * 80)
        print("表结构信息:")
        print("-" * 80)
        structure = result['table_structure']
        print(f"引擎: {structure.get('table_status', {}).get('engine', 'N/A')}")
        print(f"行数: {structure.get('table_status', {}).get('rows', 'N/A')}")
        print(f"\n字段列表:")
        for col in structure.get('columns', []):
            key_info = f" [{col['key']}]" if col['key'] else ""
            print(f"  - {col['field']}: {col['type']}{key_info}")
        
        print(f"\n索引列表:")
        for idx in structure.get('indexes', []):
            idx_cols = ', '.join([c['column'] for c in sorted(idx['columns'], key=lambda x: x['seq'])])
            unique_str = "UNIQUE " if idx['unique'] else ""
            print(f"  - {unique_str}{idx['name']}: ({idx_cols})")
        
        print("\n" + "-" * 80)
        print("EXPLAIN执行计划:")
        print("-" * 80)
        explain_data = result.get('explain_result', {}).get('explain', [])
        if explain_data:
            for i, plan in enumerate(explain_data, 1):
                print(f"\n步骤 {i}:")
                for key, value in plan.items():
                    print(f"  {key}: {value}")
        
        print("\n" + "=" * 80)
        print("AI优化建议:")
        print("=" * 80)
        print(result['optimization_suggestions'])
        print("=" * 80)


def parse_sql_file(file_path: str) -> List[Dict]:
    """
    从文件解析SQL语句列表
    文件格式：每行一条SQL，格式为: SQL语句|数据库名|表名
    
    Args:
        file_path: SQL文件路径
        
    Returns:
        SQL语句列表，每个元素包含sql, database, table
    """
    sql_list = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):  # 跳过空行和注释
                    continue
                
                # 支持两种格式：
                # 1. SQL语句|数据库名|表名
                # 2. SQL语句（如果只有SQL，需要从SQL中提取表名）
                parts = line.split('|')
                if len(parts) >= 3:
                    sql = parts[0].strip()
                    database = parts[1].strip()
                    table = parts[2].strip()
                elif len(parts) == 1:
                    # 只有SQL，尝试从SQL中提取表名（简单提取）
                    sql = parts[0].strip()
                    # 尝试提取FROM后的表名
                    import re
                    match = re.search(r'FROM\s+`?(\w+)`?', sql, re.IGNORECASE)
                    if match:
                        table = match.group(1)
                        # 默认使用第一个数据库配置
                        database = ""
                    else:
                        print(f"警告: 第{line_num}行无法提取表名，跳过: {sql}")
                        continue
                else:
                    print(f"警告: 第{line_num}行格式错误，跳过: {line}")
                    continue
                
                if sql and table:
                    sql_list.append({
                        'sql': sql,
                        'database': database,
                        'table': table,
                        'line_num': line_num
                    })
    except FileNotFoundError:
        print(f"错误: 文件 {file_path} 不存在")
    except Exception as e:
        print(f"错误: 读取文件失败: {e}")
    
    return sql_list


def main():
    """主函数 - 支持单条或多条SQL语句批量处理"""
    import sys
    
    optimizer = MySQLSlowQueryOptimizer()
    
    # 检查是否从文件读取
    if len(sys.argv) == 2 and (sys.argv[1].endswith('.txt') or sys.argv[1].endswith('.sql')):
        # 从文件读取SQL列表
        file_path = sys.argv[1]
        print("=" * 80)
        print("MySQL慢查询优化工具 - 批量处理模式")
        print("=" * 80)
        print(f"\n从文件读取SQL: {file_path}\n")
        
        sql_list = parse_sql_file(file_path)
        if not sql_list:
            print("未找到有效的SQL语句")
            sys.exit(1)
        
        print(f"找到 {len(sql_list)} 条SQL语句，开始批量处理...\n")
        
        # 循环处理每条SQL
        for idx, sql_info in enumerate(sql_list, 1):
            sql = sql_info['sql']
            database = sql_info['database']
            table = sql_info['table']
            line_num = sql_info.get('line_num', idx)
            
            print("\n" + "=" * 80)
            print(f"处理第 {idx}/{len(sql_list)} 条SQL (文件第{line_num}行)")
            print("=" * 80)
            
            # 如果没有指定数据库，使用第一个配置的数据库
            if not database and optimizer.db_configs:
                database = optimizer.db_configs[0]['database']
            
            if not database:
                print(f"错误: 未指定数据库，跳过此SQL")
                continue
            
            # 执行优化分析
            result = optimizer.optimize_query(sql, database, table)
            
            # 打印报告
            optimizer.print_optimization_report(result)
            
            # 添加分隔符
            if idx < len(sql_list):
                print("\n" + "=" * 80)
                print("继续处理下一条SQL...")
                print("=" * 80 + "\n")
        
        print("\n" + "=" * 80)
        print(f"批量处理完成！共处理 {len(sql_list)} 条SQL语句")
        print("=" * 80)
        
    elif len(sys.argv) >= 4:
        # 单条SQL处理模式
        sql = sys.argv[1]
        database = sys.argv[2]
        table = sys.argv[3]
        
        # 执行优化分析
        result = optimizer.optimize_query(sql, database, table)
        
        # 打印报告
        optimizer.print_optimization_report(result)
        
    else:
        # 显示帮助信息
        print("=" * 80)
        print("MySQL慢查询优化工具")
        print("=" * 80)
        print("\n用法1 - 单条SQL:")
        print('  python mysql_slow_query_optimizer.py "<SQL语句>" <数据库名> <表名>')
        print("\n用法2 - 批量处理（从文件读取）:")
        print('  python mysql_slow_query_optimizer.py <SQL文件路径>')
        print("\n示例 - 单条SQL:")
        print('  python mysql_slow_query_optimizer.py "SELECT * FROM users WHERE age > 30" mydb users')
        print('  python mysql_slow_query_optimizer.py "SELECT id, name FROM orders WHERE status = \'pending\' ORDER BY created_at DESC LIMIT 10" mydb orders')
        print("\n示例 - 批量处理:")
        print('  python mysql_slow_query_optimizer.py sql_queries.txt')
        print("\nSQL文件格式（每行一条SQL）:")
        print('  SQL语句|数据库名|表名')
        print('  # 这是注释行')
        print('  SELECT * FROM users WHERE age > 30|mydb|users')
        print('  SELECT id, name FROM orders WHERE status = \'pending\'|mydb|orders')
        print("\n注意:")
        print("  - SQL语句需要用引号括起来（单条模式）")
        print("  - 数据库名和表名需要在db_config.json中配置")
        print("  - 需要设置DEEPSEEK_API_KEY环境变量或使用默认API key")
        print("  - 批量处理时，如果未指定数据库名，将使用db_config.json中的第一个数据库")
        sys.exit(1)


if __name__ == '__main__':
    main()
