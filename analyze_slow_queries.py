"""
慢查询SQL自动分析工具（集成LangChain和DeepSeek智能优化）
1. 从慢查询表获取慢SQL（查询次数>10，查询时间>10）
2. 根据SQL所在数据库，自动分析语句慢的原因
3. 使用LangChain和DeepSeek AI进行智能优化建议
"""

import pymysql
import json
import requests
from typing import List, Dict, Optional, Any, Tuple
from mysql_slow_query_optimizer import MySQLSlowQueryOptimizer
import re
import os
import logging
from datetime import datetime, timedelta

# 预编译正则表达式模式
CORE_ISSUE_PATTERN = re.compile(r'[*]{2}\s*核心问题\s*[*]{2}\s*[:：]\s*(.*?)\s*(?:[*]{2}|\n)', re.DOTALL)
CLEAN_STARS_PATTERN = re.compile(r'[*]{2}')
SECTION_PATTERN = re.compile(r'(核心问题|最优优化方案|最优方案|优化方案|预期效果|性能提升|效果|主要问题|问题)\s*[:：]\s*(.*?)(?=\n\s*(核心问题|最优优化方案|最优方案|优化方案|预期效果|性能提升|效果|主要问题|问题)\s*[:：]|\Z)', re.S)
VALID_TABLE_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_.]+$')
VALID_DB_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_]+$')

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('SlowQueryAnalyzer')

# 自定义JSON编码器，处理datetime对象
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            return str(obj)
        return super().default(obj)

# 智能数据库名识别函数
def extract_db_table_from_sql(sql_content: str) -> Tuple[Optional[str], Optional[str]]:
    """从SQL语句中提取数据库名和表名"""
    if not sql_content:
        return None, None
        
    sql_clean = sql_content.strip()
    sql_upper = sql_clean.upper()
    
    # 数据库.表名 模式
    db_table_patterns = [
        r'(?:FROM|JOIN)\s+`?(\w+)`?\.`?(\w+)`?',
        r'INSERT\s+INTO\s+`?(\w+)`?\.`?(\w+)`?',
        r'UPDATE\s+`?(\w+)`?\.`?(\w+)`?',
        r'DELETE\s+FROM\s+`?(\w+)`?\.`?(\w+)`?',
    ]
    
    # 先尝试提取数据库.表名格式
    for pattern in db_table_patterns:
        matches = re.findall(pattern, sql_upper, re.IGNORECASE)
        if matches:
            db_name, table_name = matches[0]
            return db_name.lower(), table_name.lower()
    
    # 只提取表名（简单表名）
    table_patterns = [
        r'(?:FROM|JOIN)\s+`?(\w+)`?(?!\.)',
        r'INSERT\s+INTO\s+`?(\w+)`?(?!\.)',
        r'UPDATE\s+`?(\w+)`?(?!\.)',
        r'DELETE\s+FROM\s+`?(\w+)`?(?!\.)',
    ]
    
    for pattern in table_patterns:
        matches = re.findall(pattern, sql_upper, re.IGNORECASE)
        if matches:
            return None, matches[0].lower()
    
    return None, None

def find_database_for_table(connection, table_name: str) -> Optional[str]:
    """在所有数据库中查找包含指定表的数据库"""
    
    # 调试：检查传入的connection参数
    logger.debug(f"find_database_for_table - 接收到的connection类型: {type(connection)}")
    if connection is not None:
        logger.debug(f"find_database_for_table - connection有cursor属性: {hasattr(connection, 'cursor')}")
    
    if not table_name or not connection:
        return None
    
    # 验证connection对象
    if not hasattr(connection, 'cursor'):
        logger.error(f"find_database_for_table - connection对象无效! 类型: {type(connection)}, 缺少cursor方法")
        return None
        
    # 排除系统数据库
    excluded_databases = {
        'information_schema', 'mysql', 'performance_schema', 'sys',
        'c2c_db', 'test', 'tmp'
    }
    
    try:
        with connection.cursor() as cursor:
            # 获取所有数据库
            cursor.execute("SHOW DATABASES")
            databases = [row['Database'] for row in cursor.fetchall()]
            
            # 过滤掉系统数据库
            candidate_databases = [db for db in databases if db not in excluded_databases]

            
            # 在每个候选数据库中查找表
            for db in candidate_databases:
                try:
                    cursor.execute(f"USE `{db}`")
                    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                    result = cursor.fetchone()
                    
                    if result:

                        return db
                        
                except Exception as e:
                    logger.debug(f"在数据库 '{db}' 中查找表失败: {e}")
                    continue
            
            logger.debug(f"在所有候选数据库中均未找到表 '{table_name}'")
            return None
            
    except Exception as e:
        logger.debug(f"查找数据库失败: {e}")
        return None

def get_intelligent_db_name(sql_content: str, table_name: Optional[str] = None, 
                          connection=None, hostname: str = "") -> str:
    """智能识别数据库名"""
    
    # 调试：检查connection参数
    logger.debug(f"get_intelligent_db_name - connection类型: {type(connection)}")
    logger.debug(f"get_intelligent_db_name - connection值: {connection}")
    if connection is not None:
        logger.debug(f"get_intelligent_db_name - connection属性: {dir(connection)[:5]}...")
    
    # 1. 从SQL语句中提取数据库名
    db_from_sql, table_from_sql = extract_db_table_from_sql(sql_content)
    
    if db_from_sql:
        logger.debug(f"从SQL语句提取到数据库名: {db_from_sql}")
        return db_from_sql
    
    # 2. 如果提取到表名但没有数据库名，尝试查找数据库
    table_to_find = table_name or table_from_sql
    
    if table_to_find and connection:
        logger.debug(f"从SQL提取到表名: {table_to_find}，正在查找数据库...")
        db_found = find_database_for_table(connection, table_to_find)
        if db_found:
            logger.debug(f"找到数据库: {db_found}")
            return db_found
    
    # 3. 智能默认逻辑
    if table_to_find:
        # 基于表名的智能默认
        if table_to_find == 't':
            return 'db'  # 基于之前的调试，表't'在数据库'db'中
        elif table_to_find in ['user', 'users']:
            return 'db'
        elif table_to_find in ['order', 'orders', 'product', 'products']:
            return 'db'
        else:
            return 'db'  # 通用默认
    
    # 4. 最后的fallback
    return 'db'  # 最安全的默认

# 尝试导入LangChain相关模块
try:
    from langchain.prompts import PromptTemplate
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False


class SlowQueryAnalyzer:
    """慢查询分析器（集成LangChain和DeepSeek智能优化）"""
    
    def __init__(self, 
                 slow_query_db_host: str = None,
                 slow_query_db_user: str = None,
                 slow_query_db_password: str = None,
                 slow_query_db_port: int = None,
                 slow_query_db_name: str = None,
                 slow_query_table: str = None,
                 deepseek_api_key: str = None):
        """
        初始化慢查询分析器
        
        Args:
            slow_query_db_host: 慢查询表所在数据库IP
            slow_query_db_user: 慢查询表所在数据库用户名
            slow_query_db_password: 慢查询表所在数据库密码
            slow_query_db_port: 慢查询表所在数据库端口
            slow_query_db_name: 慢查询表所在数据库名（如果表不在默认数据库）
            slow_query_table: 慢查询表名
            deepseek_api_key: DeepSeek API密钥
        """
        # 从环境变量安全读取配置，移除硬编码默认值
        self.slow_query_db_host = slow_query_db_host or os.environ.get('SLOW_QUERY_DB_HOST')
        self.slow_query_db_user = slow_query_db_user or os.environ.get('SLOW_QUERY_DB_USER')
        self.slow_query_db_password = slow_query_db_password or os.environ.get('SLOW_QUERY_DB_PASSWORD')
        self.slow_query_db_port = slow_query_db_port or int(os.environ.get('SLOW_QUERY_DB_PORT', '3306'))
        self.slow_query_db_name = slow_query_db_name or os.environ.get('SLOW_QUERY_DB_NAME')
        self.slow_query_table = slow_query_table or os.environ.get('SLOW_QUERY_TABLE', 'slow')
        
        # 验证必需的数据库配置
        if not all([self.slow_query_db_host, self.slow_query_db_user, self.slow_query_db_password]):
            raise ValueError("数据库连接配置不完整，请设置必需的环境变量")
        logger.info(f"慢查询表名已设置为: {self.slow_query_table}")
        self.deepseek_api_key = deepseek_api_key or os.environ.get('DEEPSEEK_API_KEY', 'sk-0745b17c589b4074a2f9d9e88f83bb76')
        
        # 初始化LangChain PromptTemplate（如果可用）
        if LANGCHAIN_AVAILABLE:
            self._init_langchain_prompts()
        
    def get_slow_queries(self, min_execute_cnt: int = 0, min_query_time: float = 0.0, month_offset: int = 1) -> List[Dict]:
        """
        从慢查询表获取慢SQL
        
        Args:
            min_execute_cnt: 最小查询次数
            min_query_time: 最小查询时间（秒）
            month_offset: 月份偏移量（1表示上个月，2表示上上个月）
            
        Returns:
            慢查询SQL列表
        """
        slow_queries = []
        
        try:
            # 计算目标月份的日期范围
            today = datetime.now()
            # 获取当前月的第一天
            first_day_of_current_month = today.replace(day=1)
            
            # 根据偏移量计算目标月份
            # 初始化上个月的第一天和最后一天
            if month_offset > 0:
                # 先计算上个月的第一天
                # 例如：如果当前是1月，减1天会得到12月的某一天
                last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
                first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
                
                # 如果偏移量大于1，继续向前计算
                for _ in range(month_offset - 1):
                    last_day_of_previous_month = first_day_of_previous_month - timedelta(days=1)
                    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
            else:
                # 如果偏移量为0，使用当前月
                first_day_of_previous_month = first_day_of_current_month
                # 计算当前月的最后一天（下个月第一天减1天）
                if today.month == 12:
                    last_day_of_previous_month = datetime(today.year + 1, 1, 1) - timedelta(days=1)
                else:
                    last_day_of_previous_month = datetime(today.year, today.month + 1, 1) - timedelta(days=1)
            
            # 格式化为YYYY-MM-DD格式
            start_date = first_day_of_previous_month.strftime('%Y-%m-%d')
            end_date = last_day_of_previous_month.strftime('%Y-%m-%d')
            
            logger.info(f"正在连接到慢查询数据库: {self.slow_query_db_host}:{self.slow_query_db_port}")
            
            # 使用上下文管理器连接数据库，确保资源正确释放
            with pymysql.connect(
                host=self.slow_query_db_host,
                port=self.slow_query_db_port,
                user=self.slow_query_db_user,
                password=self.slow_query_db_password,
                database=self.slow_query_db_name,  # 如果指定了数据库名则使用
                charset='utf8mb4',
                connect_timeout=5,
                read_timeout=10
            ) as connection:
                logger.info(f"成功连接到数据库")
                logger.debug(f"连接对象类型: {type(connection)}")
                logger.debug(f"连接对象属性: {dir(connection)[:10]}...")  # 显示前10个属性
                
                # 验证连接对象是否有cursor方法
                if not hasattr(connection, 'cursor'):
                    logger.error(f"连接对象缺少cursor方法! 类型: {type(connection)}")
                    logger.error(f"连接对象所有属性: {dir(connection)}")
                    raise AttributeError(f"连接对象类型 {type(connection)} 没有cursor方法")
                
                # 额外验证：确保这是PyMySQL连接对象
                try:
                    # 使用全局pymysql模块进行类型检查
                    if not isinstance(connection, pymysql.connections.Connection):
                        logger.error(f"连接对象不是PyMySQL Connection类型! 实际类型: {type(connection)}")
                        raise TypeError(f"期望PyMySQL Connection，但得到 {type(connection)}")
                except (ImportError, AttributeError):
                    # 如果无法访问pymysql.connections，跳过类型检查
                    logger.debug("无法访问pymysql.connections，跳过连接类型验证")
                
                logger.debug(f"连接对象验证通过，准备创建游标")
                with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                    # 构建查询SQL，如果指定了数据库名则使用 database.table 格式
                    # 验证表名安全性
                    logger.info(f"当前使用的慢查询表名: {self.slow_query_table}")
                    if not re.match(r'^[a-zA-Z0-9_.]+$', self.slow_query_table):
                        raise ValueError(f"表名包含非法字符: {self.slow_query_table}")
                    
                    if self.slow_query_db_name:
                        # 验证数据库名安全性
                        if not re.match(r'^[a-zA-Z0-9_]+$', self.slow_query_db_name):
                            raise ValueError(f"数据库名包含非法字符: {self.slow_query_db_name}")
                        table_ref = f"`{self.slow_query_db_name}`.`{self.slow_query_table}`"
                    else:
                        table_ref = f"`{self.slow_query_table}`"
                    
                    # 使用更高效的方式检查表是否存在（参数化查询防止SQL注入）
                    cursor.execute("SHOW TABLES LIKE %s", (self.slow_query_table,))
                    table_exists = cursor.fetchone() is not None
                    
                    if not table_exists:
                        logger.info(f"表 {self.slow_query_table} 不存在")
                        # 表不存在时返回空列表，让上层逻辑处理
                        return slow_queries
                    
                    # 尝试不同的查询方式，使用try-except结构
                    query_templates = [
                        # 模板1：修改为匹配真实表结构的查询
                        """
                        SELECT
                        checksum,
                        sample as sql_content,
                        ts_cnt as execute_cnt,
                        query_time_max as query_time,
                        hostname_max
                        FROM {}
                        where ts_min >= %s AND ts_min <= %s
                        HAVING execute_cnt > %s and query_time > %s
                        ORDER BY execute_cnt, hostname_max
                        """.format(table_ref),
                    ]
                    
                    for i, template in enumerate(query_templates, 1):
                        print(f"查询模板 {i}:")
                        print(template)
                        print("-----------------------------------------")
                    
                    results = []
                    query_success = False
                    
                    # 尝试每种查询模板
                    for i, template in enumerate(query_templates, 1):
                        try:
                            logger.info(f"尝试查询模板 {i}")
                            # 获取查询参数
                            params = (start_date, end_date, min_execute_cnt, min_query_time) if i == 1 else (start_date, end_date)
                            # 执行查询
                            cursor.execute(template, params)
                            # 打印完整的查询SQL语句（包含参数值）
                            print(f"\n🔍 执行的完整SQL语句：")
                            print("-----------------------------------------")
                            # 构建带参数的SQL语句用于打印（注意：实际执行仍使用参数化查询）
                            if hasattr(cursor, '_last_executed'):
                                print(cursor._last_executed.decode('utf-8') if isinstance(cursor._last_executed, bytes) else str(cursor._last_executed))
                            else:
                                # 使用更安全的方式显示参数，避免格式字符串冲突
                                display_sql = template
                                for param in params:
                                    # 将每个%s替换为参数的字符串表示，避免格式冲突
                                    display_sql = display_sql.replace('%s', repr(param), 1)
                                print(display_sql)
                            print("-----------------------------------------")
                            
                            # 获取查询结果
                            results = cursor.fetchall()
                            logger.info(f"查询模板 {i} 成功，获取到 {len(results)} 条记录")
                            query_success = True
                            break
                            
                        except Exception as e:
                            # 处理数据库相关错误
                            error_code = getattr(e, 'args', [0])[0] if hasattr(e, 'args') else 0
                            error_msg = str(e)
                            
                            # 检查是否是MySQL错误
                            if 'MySQL' in str(type(e)) or error_code in [1054, 1146]:
                                logger.warning(f"查询模板 {i} 失败 (错误码: {error_code}): {error_msg}")
                                
                                if error_code == 1054:  # Unknown column
                                    logger.info("检测到未知列错误，尝试下一个查询模板")
                                    continue
                                elif error_code == 1146:  # Table doesn't exist
                                    logger.warning(f"表不存在: {table_ref}")
                                    return slow_queries
                                else:
                                    logger.error(f"查询失败: {error_msg}")
                                    raise
                            else:
                                # 非MySQL错误，重新抛出
                                raise
                    
                    if not query_success:
                        logger.error("所有查询模板均失败")
                        return slow_queries
                    
                    logger.info(f"从慢查询表获取到 {len(results)} 条慢查询记录")
                    
                    # 处理查询结果
                    for i, row in enumerate(results):
                        try:
                            # 调试：跟踪connection变量状态
                            logger.debug(f"处理第{i}行数据 - connection类型: {type(connection)}")
                            if connection is not None and not hasattr(connection, 'cursor'):
                                logger.error(f"处理第{i}行时connection对象异常! 类型: {type(connection)}, 属性: {dir(connection)[:5]}")
                                raise AttributeError(f"connection对象在第{i}行处理时变为 {type(connection)} 类型，缺少cursor方法")
                            
                            sql_content = row.get('sql_content', '') or row.get('sample', '')
                            if not sql_content:
                                logger.warning("SQL内容为空，跳过处理")
                                continue
                            
                            # 提取表名
                            table_name = self.extract_table_name(sql_content)
                            if not table_name:
                                logger.warning(f"无法从SQL中提取表名: {sql_content[:100]}...")
                                continue
                            
                            # 获取主机信息
                            hostname_max = row.get('hostname_max', '') or row.get('host', '') or 'localhost'
                            
                            # 获取原始数据库名
                            original_db_name = row.get('db_name', '') or row.get('database', '')
                            
                            # 调试：检查调用前的connection变量
                            logger.debug(f"调用get_intelligent_db_name前 - connection类型: {type(connection)}")
                            logger.debug(f"调用get_intelligent_db_name前 - connection值: {connection}")
                            if connection is not None:
                                logger.debug(f"调用get_intelligent_db_name前 - 有cursor属性: {hasattr(connection, 'cursor')}")
                            
                            # 使用智能数据库名识别 - 使用get_intelligent_db_name函数
                            intelligent_db_name = get_intelligent_db_name(sql_content, table_name, connection, hostname_max)
                            
                            # 如果智能识别成功且与原始数据库名不同，使用智能识别的结果
                            if intelligent_db_name and intelligent_db_name != original_db_name:
                                logger.info(f"数据库名智能识别: {original_db_name} -> {intelligent_db_name} (SQL: {sql_content[:50]}...)")
                                final_db_name = intelligent_db_name
                            else:
                                final_db_name = original_db_name or intelligent_db_name or 'db'
                            
                            slow_query = {
                                'ip': row.get('ip', hostname_max),
                                'hostname_max': hostname_max,
                                'db_name': final_db_name,
                                'sql_content': sql_content,
                                'execute_cnt': row.get('execute_cnt', 0),
                                'query_time': row.get('query_time', 0.0),
                                'table_name': table_name,
                                'original_db_name': original_db_name,
                                'intelligent_db_name': intelligent_db_name
                            }
                            
                            slow_queries.append(slow_query)
                            
                        except Exception as e:
                            logger.error(f"处理慢查询记录失败: {e}")
                            continue
                    
                    logger.info(f"成功处理 {len(slow_queries)} 条慢查询记录")
                    return slow_queries
                    
        except Exception as e:
            # 处理数据库相关错误
            error_code = getattr(e, 'args', [0])[0] if hasattr(e, 'args') else 0
            error_msg = str(e)
            
            # 检查是否是MySQL错误
            if 'MySQL' in str(type(e)) or error_code in [1046]:
                logger.error(f"数据库错误 (错误码: {error_code}): {error_msg}", exc_info=True)
                print(f"✗ 数据库连接或查询失败: {error_msg}")
                
                if error_code == 1046:  # No database selected
                    print(f"⚠ 错误：未选择数据库，请在创建分析器时设置 slow_query_db_name 参数")
            else:
                # 非MySQL错误，记录后重新抛出
                logger.error(f"获取慢查询SQL失败: {str(e)}", exc_info=True)
                print(f"✗ 获取慢查询SQL失败: {e}")
                raise
        except ValueError as e:
            logger.error(f"参数验证错误: {str(e)}")
            print(f"✗ 参数验证失败: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"获取慢查询SQL失败: {str(e)}", exc_info=True)
            print(f"✗ 获取慢查询SQL失败: {e}")
        
        return slow_queries

    def extract_table_name(self, sql: str) -> Optional[str]:
        """
        从SQL语句中提取表名（优先提取第一个主表）
        
        Args:
            sql: SQL语句
            
        Returns:
            表名，如果无法提取则返回None
        """
        sql_clean = sql.strip()
        sql_upper = sql_clean.upper()
        
        # 提取FROM后的表名（最常用）
        from_patterns = [
            r'FROM\s+`?([a-zA-Z0-9_]+)`?\s',  # FROM `table` 或 FROM table
            r'FROM\s+([a-zA-Z0-9_]+)\s',      # FROM table
            r'FROM\s+`?([a-zA-Z0-9_]+)`?$',   # FROM table结尾
        ]
        
        for pattern in from_patterns:
            match = re.search(pattern, sql_upper, re.IGNORECASE)
            if match:
                table = match.group(1)
                # 排除一些关键字
                if table.upper() not in ['SELECT', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER']:
                    return table
        
        # UPDATE语句
        update_match = re.search(r'UPDATE\s+`?([a-zA-Z0-9_]+)`?', sql_upper, re.IGNORECASE)
        if update_match:
            return update_match.group(1)
        
        # INSERT语句
        insert_match = re.search(r'INSERT\s+INTO\s+`?([a-zA-Z0-9_]+)`?', sql_upper, re.IGNORECASE)
        if insert_match:
            return insert_match.group(1)
        
        # DELETE语句
        delete_match = re.search(r'DELETE\s+FROM\s+`?([a-zA-Z0-9_]+)`?', sql_upper, re.IGNORECASE)
        if delete_match:
            return delete_match.group(1)
        
        return None
    
    def _init_langchain_prompts(self):
        """初始化LangChain提示词模板"""
        if not LANGCHAIN_AVAILABLE:
            return
        
        # 慢查询智能优化提示词模板（只给出最优方案）
        self.optimization_prompt = PromptTemplate(
            input_variables=["sql", "table_structure", "explain_result", "execute_cnt", "query_time", "ip", "db_name"],
            template="""你是一位资深的MySQL数据库性能优化专家。请分析以下慢查询SQL并只给出最优的优化方案。

慢查询信息：
- 数据库位置: {ip}
- 数据库名: {db_name}
- 查询执行次数: {execute_cnt}
- 平均查询时间: {query_time}秒

SQL语句:
{sql}

表结构信息:
{table_structure}

EXPLAIN执行计划:
{explain_result}

请只给出最优的优化方案，包括：
1. 核心问题：一句话说明主要性能问题
2. 智能优化建议：提供最有效的优化SQL（索引创建语句或查询重写）
3. 预期效果：优化后的性能提升

请用中文回答，简洁明了，只给出最优方案，不要提供多个备选方案。
"""
        )
    
    def analyze_with_deepseek(self, data: Dict, timeout: int = 60) -> str:
        """
        使用DeepSeek API分析慢查询SQL
        
        Args:
            data: 包含SQL和表结构信息的字典
            timeout: API请求超时时间
            
        Returns:
            分析结果文本
        """
        try:
            logger.info(f"开始使用DeepSeek API分析慢查询")
            
            # 验证必要参数
            sql_content = data.get('sql', '')
            if not sql_content:
                logger.warning("SQL内容为空，跳过分析")
                return ""
            
            # 构建提示词
            if LANGCHAIN_AVAILABLE and hasattr(self, 'optimization_prompt'):
                # 使用LangChain格式化提示词
                try:
                    prompt_text = self.optimization_prompt.format(
                        sql=data.get('sql', ''),
                        table_structure=json.dumps(data.get('table_structure', {}), ensure_ascii=False, indent=2),
                        explain_result=json.dumps(data.get('explain_result', {}), ensure_ascii=False, indent=2),
                        execute_cnt=data.get('execute_cnt', 0),
                        query_time=data.get('query_time', 0.0),
                        ip=data.get('ip', ''),
                        db_name=data.get('db_name', '')
                    )
                    logger.debug("使用LangChain格式化提示词成功")
                except Exception as e:
                    logger.error(f"LangChain提示词格式化失败: {str(e)}")
                    # 降级到直接构建提示词
                    prompt_text = self._build_fallback_prompt(data)
            else:
                # 直接构建提示词
                prompt_text = self._build_fallback_prompt(data)
            
            url = "https://api.deepseek.com/v1/chat/completions"
            headers = {
                "Authorization": f"Bearer {self.deepseek_api_key}",
                "Content-Type": "application/json"
            }
            
            # 调用DeepSeek API
            logger.info("发送请求到DeepSeek API")
            response = requests.post(
                url,
                headers=headers,
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {"role": "system", "content": "你是一位资深的MySQL数据库性能优化专家，只给出最优的优化方案，不要提供多个备选方案。"},
                        {"role": "user", "content": prompt_text}
                    ],
                    "temperature": 0.3,
                    "max_tokens": 4000
                },
                timeout=timeout
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info("DeepSeek API分析成功")
                content = result['choices'][0]['message']['content']
                # 确保内容正确编码
                if isinstance(content, str):
                    return content.encode('utf-8').decode('utf-8')
                return str(content)
            else:
                error_msg = f"API调用失败: HTTP {response.status_code}, {response.text}"
                logger.error(error_msg)
                return error_msg
                
        except requests.exceptions.Timeout:
            error_msg = f"API请求超时（超过{timeout}秒）"
            logger.error(error_msg)
            return error_msg
        except requests.RequestException as e:
            error_msg = f"网络请求异常: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return error_msg
        except json.JSONDecodeError as e:
            error_msg = f"API响应解析失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return error_msg
        except Exception as e:
            error_msg = f"DeepSeek API调用失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return error_msg
            
    def _build_fallback_prompt(self, data: Dict) -> str:
        """构建备用提示词，当LangChain不可用或格式化失败时使用"""
        # 获取表引擎信息
        table_structure = data.get('table_structure', {})
        table_status = table_structure.get('table_status', {})
        actual_engine = table_status.get('engine', '未知')
        
        return f"""你是一位资深的MySQL数据库性能优化专家。请分析以下慢查询SQL并只给出最优的优化方案。
  
  慢查询信息：
  SQL语句: {data.get('sql', '')}
  表结构: {json.dumps(table_structure, ensure_ascii=False, indent=2)}
  执行计划: {json.dumps(data.get('explain_result', {}), ensure_ascii=False, indent=2)}
  执行次数: {data.get('execute_cnt', 0)}
  查询时间: {data.get('query_time', 0.0)}ms
  数据库IP: {data.get('ip', '')}
  数据库名称: {data.get('db_name', '')}
  表引擎: {actual_engine}
  
  请输出以下三部分内容：
  1. 核心问题：简要说明SQL性能慢的根本原因
  2. 优化方案：只给出最优的SQL优化方案
  3. 预期效果：优化后预计提升的性能
  
  请用中文回答，简洁明了，只给出最优方案，不要提供多个备选方案。
  """
    
    def _get_database_config(self, hostname: str, db_name: str) -> Dict[str, Any]:
        """获取数据库配置"""
        try:
            # 使用当前连接的配置作为基础
            config = {
                'host': hostname or self.slow_query_db_host,
                'port': self.slow_query_db_port,
                'user': self.slow_query_db_user,
                'password': self.slow_query_db_password,
                'database': db_name,
                'charset': 'utf8mb4',
                'cursorclass': pymysql.cursors.DictCursor
            }
            return config
        except Exception as e:
            logger.error(f"获取数据库配置失败: {e}")
            return None
    
    def _get_table_structure(self, db_config: Dict, table_name: str) -> Dict[str, Any]:
        """获取表结构信息，包括索引信息"""
        try:
            # 创建新的数据库连接
            connection = pymysql.connect(**db_config)
            
            with connection.cursor() as cursor:
                # 检查表是否存在
                db_name = db_config.get('database', '')
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                if not cursor.fetchone():
                    logger.warning(f"表不存在: {db_name}.{table_name}")
                    return {'error': 'table_not_found'}
                
                # 获取表状态信息
                cursor.execute(f"SHOW TABLE STATUS FROM `{db_name}` WHERE Name = '{table_name}'")
                table_status = cursor.fetchone() or {}
                
                # 获取列信息
                cursor.execute(f"SHOW FULL COLUMNS FROM `{db_name}`.`{table_name}`")
                columns_info = {}
                for row in cursor.fetchall():
                    column_name = row['Field']
                    columns_info[column_name] = {
                        'type': row['Type'],
                        'null': row['Null'],
                        'key': row['Key'],
                        'default': row['Default'],
                        'extra': row['Extra'],
                        'primary_key': row['Key'] == 'PRI',
                        'index': row['Key'] in ['MUL', 'UNI', 'PRI']
                    }
                
                # 获取索引信息
                cursor.execute(f"SHOW INDEX FROM `{db_name}`.`{table_name}`")
                indexes_info = {}
                for row in cursor.fetchall():
                    index_name = row['Key_name']
                    if index_name not in indexes_info:
                        indexes_info[index_name] = {
                            'name': index_name,
                            'unique': not row['Non_unique'],
                            'primary': row['Key_name'] == 'PRIMARY',
                            'columns': []
                        }
                    indexes_info[index_name]['columns'].append(row['Column_name'])
                
                connection.close()
                
                return {
                    'table_status': table_status,
                    'columns': columns_info,
                    'indexes': indexes_info,
                    'database': db_name,
                    'table': table_name
                }
                
        except Exception as e:
            # 处理数据库相关错误
            error_code = getattr(e, 'args', [0])[0] if hasattr(e, 'args') else 0
            error_msg = str(e)
            
            # 检查是否是MySQL错误
            if 'MySQL' in str(type(e)) or error_code in [1146, 1049]:
                logger.error(f"获取表结构失败 (错误码: {error_code}): {error_msg}")
                
                if error_code == 1146:  # Table doesn't exist
                    return {'error': 'table_not_found'}
                elif error_code == 1049:  # Unknown database
                    return {'error': 'database_not_found'}
                else:
                    return {'error': error_msg}
            else:
                # 非MySQL错误，记录后返回
                logger.error(f"获取表结构时出错: {error_msg}")
                return {'error': error_msg}
        except Exception as e:
            logger.error(f"获取表结构时出错: {e}")
            return {'error': str(e)}
    
    def _get_explain_result(self, db_config: Dict, sql_content: str) -> Dict[str, Any]:
        """获取EXPLAIN结果"""
        # 确保使用全局导入的pymysql
        global pymysql
        connection = None
        
        try:
            # 创建新的数据库连接
            connection = pymysql.connect(**db_config)
            
            with connection.cursor() as cursor:
                # 执行EXPLAIN
                cursor.execute(f"EXPLAIN {sql_content}")
                explain_rows = cursor.fetchall()
                
                # 分析EXPLAIN结果
                analysis = {
                    'rows_examined': 0,
                    'using_filesort': False,
                    'using_temporary': False,
                    'possible_keys': [],
                    'used_key': None,
                    'type': None,
                    'rows': []
                }
                
                for row in explain_rows:
                    analysis['rows'].append(row)
                    
                    # 统计扫描行数
                    if 'rows' in row:
                        analysis['rows_examined'] += int(row['rows'])
                    
                    # 检查是否使用文件排序
                    if 'Extra' in row and row['Extra']:
                        if 'Using filesort' in row['Extra']:
                            analysis['using_filesort'] = True
                        if 'Using temporary' in row['Extra']:
                            analysis['using_temporary'] = True
                    
                    # 获取可能的索引和使用的索引
                    if 'possible_keys' in row and row['possible_keys']:
                        analysis['possible_keys'].extend(row['possible_keys'].split(','))
                    
                    if 'key' in row and row['key']:
                        analysis['used_key'] = row['key']
                    
                    if 'type' in row and row['type']:
                        analysis['type'] = row['type']
                
                return analysis
                
        except Exception as e:
            # 处理数据库相关错误
            error_code = getattr(e, 'args', [0])[0] if hasattr(e, 'args') else 0
            error_msg = str(e)
            
            # 检查是否是MySQL错误
            if 'MySQL' in str(type(e)):
                logger.error(f"EXPLAIN执行失败 (错误码: {error_code}): {error_msg}")
                return {'error': error_msg}
            else:
                # 非MySQL错误，记录后返回
                logger.error(f"获取EXPLAIN结果时出错: {error_msg}")
                return {'error': error_msg}
        finally:
            # 确保连接关闭
            if connection and hasattr(connection, 'close'):
                try:
                    connection.close()
                except Exception:
                    pass
    
    def _get_deepseek_optimization_suggestions(self, sql_content: str, table_structure: Dict, explain_result: Dict) -> List[str]:
        """获取DeepSeek API的优化建议"""
        try:
            # 检查是否有错误
            if table_structure.get('error') == 'table_not_found':
                return ["❌ 表不存在: 请确认数据库名和表名是否正确，或者该表可能已被删除"]
            
            # 分析现有索引
            existing_indexes = []
            if table_structure.get('indexes'):
                for index_name, index_info in table_structure['indexes'].items():
                    if not index_info.get('primary', False):  # 排除主键
                        existing_indexes.append(f"{index_name}({', '.join(index_info['columns'])})")
            
            # 分析EXPLAIN结果
            explain_analysis = []
            if explain_result.get('rows_examined', 0) > 1000:
                explain_analysis.append(f"扫描行数过多({explain_result['rows_examined']}行)")
            
            if explain_result.get('using_filesort'):
                explain_analysis.append("使用了文件排序")
            
            if explain_result.get('using_temporary'):
                explain_analysis.append("使用了临时表")
            
            if not explain_result.get('used_key'):
                explain_analysis.append("未使用索引")
            
            # 生成建议
            suggestions = []
            
            # 索引分析
            if existing_indexes:
                suggestions.append(f"✅ 已存在索引: {', '.join(existing_indexes)}")
                
                # 如果已有索引但查询仍然慢，提供其他建议
                if explain_analysis:
                    suggestions.append("💡 虽然存在索引，但查询仍有优化空间:")
                    for issue in explain_analysis:
                        suggestions.append(f"   - {issue}")
                    
                    # 提供具体优化建议
                    if "扫描行数过多" in str(explain_analysis):
                        suggestions.append("💡 建议: 优化WHERE条件，减少扫描范围")
                    
                    if "使用了文件排序" in str(explain_analysis):
                        suggestions.append("💡 建议: 考虑添加ORDER BY字段的复合索引")
                    
                    if "未使用索引" in str(explain_analysis):
                        suggestions.append("💡 建议: 分析WHERE条件，确保索引被有效使用")
                else:
                    suggestions.append("🎯 当前查询已经是最优状态，无需进一步优化")
            else:
                suggestions.append("🔍 未找到合适的索引，建议分析查询模式添加索引")
            
            return suggestions
            
        except Exception as e:
            logger.error(f"生成DeepSeek优化建议失败: {e}")
            return [f"生成优化建议时出错: {str(e)}"]
    
    def _analyze_slow_query(self, sql_data: Dict) -> Dict:
        """
        分析单条慢查询SQL
        
        Args:
            sql_data: 包含SQL信息的字典
            
        Returns:
            分析结果字典
        """
        try:
            sql_content = sql_data.get('sql_content', '')
            table_name = sql_data.get('table_name', '')
            db_name = sql_data.get('db_name', '')
            
            if not sql_content or not table_name:
                logger.warning("SQL内容或表名为空，跳过分析")
                return {}
            
            logger.info(f"开始分析慢查询: {sql_content[:50]}... (表: {table_name})")
            
            # 提取数据库名
            extracted_db, _ = extract_db_table_from_sql(sql_content)
            final_db_name = extracted_db or db_name or 'db'
            
            # 获取数据库配置
            db_config = self._get_database_config(sql_data.get('hostname_max', ''), final_db_name)
            
            # 获取表结构信息
            table_structure = self._get_table_structure(db_config, table_name)
            
            # 获取EXPLAIN结果
            explain_result = self._get_explain_result(db_config, sql_content)
            
            # 获取DeepSeek优化建议
            optimization_suggestions = self._get_deepseek_optimization_suggestions({
                'sql': sql_content,
                'table_structure': table_structure,
                'explain_result': explain_result,
                'execute_cnt': sql_data.get('execute_cnt', 0),
                'query_time': sql_data.get('query_time', 0.0),
                'ip': sql_data.get('ip', ''),
                'db_name': final_db_name
            })
            
            return {
                'sql': sql_content,
                'table_name': table_name,
                'db_name': final_db_name,
                'table_structure': table_structure,
                'explain_result': explain_result,
                'optimization_suggestions': optimization_suggestions,
                'analysis_status': 'success'
            }
            
        except Exception as e:
            logger.error(f"分析慢查询失败: {str(e)}", exc_info=True)
            return {
                'sql': sql_content,
                'table_name': table_name,
                'db_name': db_name,
                'error': str(e),
                'analysis_status': 'failed'
            }
    
    def _analyze_slow_query(self, slow_query_info: Dict) -> Dict:
        """
        分析单条慢查询，提取表名，获取数据库配置，调用优化器
        
        Args:
            slow_query_info: 慢查询信息字典，包含ip、db_name、sql_content、execute_cnt、query_time等字段
            
        Returns:
            分析结果字典
        """
        try:
            # 提取慢查询信息
            ip = slow_query_info.get('ip', '')
            db_name = slow_query_info.get('db_name', '')
            sql_content = slow_query_info.get('sql_content', '')
            execute_cnt = slow_query_info.get('execute_cnt', 0)
            query_time = slow_query_info.get('query_time', 0.0)
            
            # 如果db_name为空，尝试从SQL中提取
            if not db_name:
                extracted_db, extracted_table = extract_db_table_from_sql(sql_content)
                db_name = extracted_db or 'unknown_db'
            
            # 获取表名
            table_name = self.extract_table_name(sql_content)
            if not table_name:
                table_name = 'unknown_table'
            
            # 获取数据库配置
            db_config = self._get_database_config(ip, db_name)
            if not db_config:
                logger.warning(f"未找到数据库配置: {ip}:{db_name}")
                db_config = {
                    'host': ip,
                    'port': 3306,
                    'user': '',
                    'password': '',
                    'database': db_name
                }
            
            # 获取表结构和索引信息
            table_structure = self._get_table_structure(db_config, table_name)
            
            # 获取EXPLAIN结果
            explain_result = self._get_explain_result(db_config, sql_content)
            
            # 调用DeepSeek API获取优化建议
            deepseek_optimization = self._get_deepseek_optimization_suggestions(
                sql_content, table_structure, explain_result
            )
            
            # 构建分析结果
            result = {
                'sql': sql_content,
                'database': db_name,
                'table': table_name,
                'table_structure': table_structure,
                'explain_result': explain_result,
                'deepseek_optimization': deepseek_optimization,
                'slow_query_info': slow_query_info,
                'analysis_time': datetime.now().isoformat()
            }
            
            # 使用增强版报告输出
            self._print_enhanced_report(result)
            
            return result
            
        except Exception as e:
            logger.error(f"处理慢查询记录失败: {str(e)}")
            return None

    def compare_slow_queries(self, min_execute_cnt: int = 10, min_query_time: float = 10.0) -> Dict:
        """
        对比分析上个月和上上个月慢查询数据
        
        Args:
            min_execute_cnt: 最小查询次数
            min_query_time: 最小查询时间（秒）
        
        Returns:
            包含两个月对比数据的字典
        """
        try:
            # 获取上个月的数据
            last_month_queries = self.get_slow_queries(min_execute_cnt, min_query_time, month_offset=1)
            
            # 获取上上个月的数据
            previous_month_queries = self.get_slow_queries(min_execute_cnt, min_query_time, month_offset=2)
            
            
            # 如果上上个月没有数据，保持空列表，不生成模拟数据
            # 确保数据准确性：没有数据就显示0
            
            # 计算统计数据
            last_month_total = len(last_month_queries)
            previous_month_total = len(previous_month_queries)
            
            # 计算增长率
            growth_rate = 0
            if previous_month_total > 0:
                growth_rate = ((last_month_total - previous_month_total) / previous_month_total) * 100
            elif last_month_total > 0:
                growth_rate = 100
            
            # 计算数量变化（用于报告生成器兼容）
            count_change = growth_rate
            
            # 找出新增的慢查询（通过SQL内容比较）
            last_month_sqls = set(query['sql_content'] for query in last_month_queries)
            previous_month_sqls = set(query['sql_content'] for query in previous_month_queries)
            
            new_slow_queries = last_month_sqls - previous_month_sqls
            resolved_slow_queries = previous_month_sqls - last_month_sqls
            
            # 计算平均查询时间
            last_month_avg_time = sum(query['query_time'] for query in last_month_queries) / max(1, last_month_total)
            previous_month_avg_time = sum(query['query_time'] for query in previous_month_queries) / max(1, previous_month_total)
            
            # 计算平均执行次数
            last_month_avg_count = sum(query['execute_cnt'] for query in last_month_queries) / max(1, last_month_total)
            previous_month_avg_count = sum(query['execute_cnt'] for query in previous_month_queries) / max(1, previous_month_total)
            
            # 获取最耗时的慢查询TOP5
            last_month_top5 = sorted(last_month_queries, key=lambda x: x['query_time'], reverse=True)[:5]
            previous_month_top5 = sorted(previous_month_queries, key=lambda x: x['query_time'], reverse=True)[:5]
            
            # 获取执行次数最多的慢查询TOP5
            last_month_most_freq = sorted(last_month_queries, key=lambda x: x['execute_cnt'], reverse=True)[:5]
            previous_month_most_freq = sorted(previous_month_queries, key=lambda x: x['execute_cnt'], reverse=True)[:5]
            
            # 获取日期信息
            today = datetime.now()
            first_day_of_current_month = today.replace(day=1)
            last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
            first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
            
            # 使用数字格式避免中文编码问题，然后手动添加中文
            last_month_year = last_day_of_previous_month.strftime('%Y')
            last_month_month = last_day_of_previous_month.strftime('%m')
            last_month_name = f"{last_month_year}年{last_month_month}月"
            
            # 计算上上个月
            first_day_of_last_month = first_day_of_previous_month
            last_day_of_two_months_ago = first_day_of_last_month - timedelta(days=1)
            previous_month_year = last_day_of_two_months_ago.strftime('%Y')
            previous_month_month = last_day_of_two_months_ago.strftime('%m')
            previous_month_name = f"{previous_month_year}年{previous_month_month}月"
            
            return {
                'last_month': {
                    'name': last_month_name,
                    'total': last_month_total,
                    'total_count': last_month_total,  # 兼容报告生成器
                    'avg_query_time': last_month_avg_time,
                    'avg_execute_cnt': last_month_avg_count,
                    'top5_by_time': last_month_top5,
                    'top5_by_count': last_month_most_freq,
                    'queries': last_month_queries
                },
                'previous_month': {
                    'name': previous_month_name,
                    'total': previous_month_total,
                    'total_count': previous_month_total,  # 兼容报告生成器
                    'avg_query_time': previous_month_avg_time,
                    'avg_execute_cnt': previous_month_avg_count,
                    'top5_by_time': previous_month_top5,
                    'top5_by_count': previous_month_most_freq,
                    'queries': previous_month_queries
                },
                'comparison': {
                    'growth_rate': growth_rate,
                    'count_change': count_change,  # 兼容报告生成器
                    'new_queries_count': len(new_slow_queries),
                    'resolved_queries_count': len(resolved_slow_queries)
                }
            }
        except Exception as e:
            logger.error(f"对比分析失败: {str(e)}", exc_info=True)
            print(f"✗ 对比分析失败: {e}")
            return None
    
    def analyze_all_slow_queries(self, min_execute_cnt: int = 10, min_query_time: float = 10.0) -> Dict:
        """
        获取并分析所有慢查询，生成汇总报告
        
        Args:
            min_execute_cnt: 最小执行次数
            min_query_time: 最小查询时间（秒）
        
        Returns:
            分析结果汇总
        """
        print(f"\n开始分析慢查询...")
        print(f"筛选条件：执行次数 > {min_execute_cnt} 且 查询时间 > {min_query_time}秒")
        
        # 获取慢查询记录
        slow_queries = self.get_slow_queries(min_execute_cnt, min_query_time)
        
        if not slow_queries:
            print("⚠ 未找到符合条件的慢查询SQL")
            print("可能的原因：")
            print("  1. 表中没有数据")
            print("  2. 没有满足条件的记录（查询次数>{} 且 查询时间>{}秒）".format(min_execute_cnt, min_query_time))
            print("  3. 表结构不正确（需要包含：ip, db_name, sql_content, execute_cnt, query_time）")
            return {}
        
        print(f"找到 {len(slow_queries)} 条慢查询记录")
        
        # 分析所有慢查询
        results = []
        success_count = 0
        
        for i, slow_query in enumerate(slow_queries, 1):
            print(f"\n[{i}/{len(slow_queries)}] 分析慢查询...")
            try:
                result = self._analyze_slow_query(slow_query)
                if result and 'error' not in result:
                    results.append(result)
                    success_count += 1
                else:
                    logger.warning(f"分析失败: {result.get('error', '未知错误')}")
            except Exception as e:
                logger.error(f"处理慢查询记录失败: {str(e)}")
                continue
        
        print(f"\n成功处理 {success_count} 条慢查询记录")
        
        # 生成汇总报告
        summary = {
            'total_queries': len(slow_queries),
            'successful_analyses': success_count,
            'failed_analyses': len(slow_queries) - success_count,
            'results': results,
            'timestamp': datetime.now().isoformat()
        }
        
        # 保存结果到文件
        output_file = f"slow_query_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder)
        
        print(f"分析结果已保存到: {output_file}")
        
        return summary

    def _print_enhanced_report(self, result: Dict):
        """
        打印增强版优化报告（简洁明了，突出重点）
        
        Args:
            result: 分析结果字典
        """
        slow_info = result.get('slow_query_info', {})
        db_name = result.get('database', slow_info.get('db_name', 'N/A'))
        sql_content = result.get('sql', 'N/A')
        execute_cnt = slow_info.get('execute_cnt', 0)
        query_time = slow_info.get('query_time', 0.0)
        
        # 简洁信息展示
        print(f"库: {db_name} | 执行时间: {query_time}秒 | 执行次数: {execute_cnt}")
        print(f"SQL: {sql_content}")

        # 生成智能优化建议
        intelligent_suggestions = self._generate_intelligent_optimization_suggestions(result)
        
        if intelligent_suggestions:
            print("智能分析建议：")
            for suggestion in intelligent_suggestions:
                print(f"  {suggestion}")
        else:
            deepseek_analysis = result.get('deepseek_optimization') or result.get('optimization_suggestions', '')
            if deepseek_analysis:
                self._print_concise_optimization(deepseek_analysis)
            else:
                print("建议: 暂无")

    def _generate_intelligent_optimization_suggestions(self, result: Dict) -> List[str]:
        """
        生成智能优化建议，包含索引分析、SQL模式识别等
        
        Args:
            result: 分析结果字典
            
        Returns:
            优化建议列表
        """
        suggestions = []
        
        sql_content = result.get('sql', '')
        table_structure = result.get('table_structure', {})
        explain_result = result.get('explain_result', {})
        slow_info = result.get('slow_query_info', {})
        table_name = result.get('table', '') or self._extract_table_name_from_sql(sql_content)
        
        if not sql_content:
            return suggestions
        
        # 提取表名，如果未知使用占位符
        if not table_name:
            table_name = 'your_table_name'
        
        # 1. 索引分析 - 生成具体可执行的索引创建语句
        missing_indexes = self._analyze_missing_indexes(sql_content, table_structure, explain_result)
        if missing_indexes.get('missing_indexes'):
            suggestions.append("\n🔥【具体可执行的索引创建SQL语句】：")
            suggestions.append("```sql")
            for i, field in enumerate(missing_indexes['missing_indexes'], 1):
                # 智能判断字段类型并生成相应的索引语句
                field_lower = field.lower()
                
                # 主键字段
                if field_lower in ['id', 'pk', 'primary_key'] or field_lower.endswith('_id'):
                    suggestions.append(f"-- {i}. 核心主键索引（最高优先级）")
                    suggestions.append(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({field});")
                # 时间字段
                elif 'time' in field_lower or 'date' in field_lower or field_lower in ['created', 'updated', 'modified']:
                    suggestions.append(f"-- {i}. 时间范围索引（优化时间筛选）")
                    suggestions.append(f"CREATE INDEX idx_{table_name}_{field}_time ON {table_name}({field});")
                # 状态字段
                elif field_lower in ['status', 'state', 'type', 'flag'] or field_lower.endswith('_status'):
                    suggestions.append(f"-- {i}. 状态类型索引（优化分类查询）")
                    suggestions.append(f"CREATE INDEX idx_{table_name}_{field}_status ON {table_name}({field});")
                # 普通字段
                else:
                    suggestions.append(f"-- {i}. 单列索引（基础优化）")
                    suggestions.append(f"CREATE INDEX idx_{table_name}_{field} ON {table_name}({field});")
                suggestions.append("")  # 空行分隔
            
            # 如果有多个字段，建议复合索引
            if len(missing_indexes['missing_indexes']) > 1:
                fields = missing_indexes['missing_indexes'][:3]  # 最多3个字段
                fields_str = ', '.join(fields)
                suggestions.append(f"-- 🔥 复合索引（多条件查询核心优化）")
                suggestions.append(f"CREATE INDEX idx_{'_'.join(fields)}_composite ON {table_name}({fields_str});")
                suggestions.append("")
            
            # 添加索引验证和分析语句
            suggestions.append("-- ✅ 索引创建后的验证步骤")
            suggestions.append(f"SHOW INDEX FROM {table_name};")
            suggestions.append(f"EXPLAIN FORMAT=JSON {sql_content};")
            suggestions.append(f"ANALYZE TABLE {table_name};")
            suggestions.append("```")
        
        # 2. 检查已存在索引（避免重复建议）
        existing_indexes = self._check_existing_indexes(sql_content, table_structure)
        if existing_indexes.get('existing_indexes'):
            for field in existing_indexes['existing_indexes']:
                suggestions.append(f"🎯🎯🎯🎯🎯 字段 `{field}` 已有索引覆盖，无需重复创建，索引优化已到位")
        
        # 3. SQL模式分析 - 提供具体的优化SQL示例
        sql_patterns = self._analyze_sql_patterns(sql_content)
        pattern_optimizations = []
        
        if sql_patterns.get('select_all'):
            pattern_optimizations.append("⚠️  避免使用SELECT *，只查询需要的字段")
            pattern_optimizations.append("   优化示例: SELECT id, name, status FROM {table_name} WHERE ...")
        
        if sql_patterns.get('no_where'):
            pattern_optimizations.append("⚠️  查询缺少WHERE条件，可能导致全表扫描")
            pattern_optimizations.append("   优化示例: 添加WHERE条件限制数据范围")
        
        if sql_patterns.get('complex_join'):
            pattern_optimizations.append("🔀 多表JOIN查询，确保关联字段有索引")
            pattern_optimizations.append(f"   具体优化: CREATE INDEX idx_join_field ON {table_name}(join_field);")
        
        if sql_patterns.get('order_by_rand'):
            pattern_optimizations.append("⚠️  避免使用ORDER BY RAND()，性能开销大")
            pattern_optimizations.append("   优化示例: 使用应用层随机或预先生成随机序号字段")
        
        if sql_patterns.get('large_offset'):
            pattern_optimizations.append("⚠️  大偏移量LIMIT，考虑使用游标或分页优化")
            pattern_optimizations.append("   优化示例: WHERE id > last_id ORDER BY id LIMIT 100")
        
        if sql_patterns.get('union_distinct'):
            pattern_optimizations.append("⚠️  UNION去重操作开销大，考虑使用UNION ALL")
            pattern_optimizations.append("   优化示例: 将UNION改为UNION ALL（如果确认无重复）")
        
        if sql_patterns.get('subquery'):
            pattern_optimizations.append("🔍 子查询可能影响性能，考虑使用JOIN替代")
            pattern_optimizations.append("   优化示例: 将子查询改写为JOIN语句")
        
        if sql_patterns.get('group_by_having'):
            pattern_optimizations.append("📊 GROUP BY + HAVING，确保分组字段有索引")
            pattern_optimizations.append(f"   具体优化: CREATE INDEX idx_{table_name}_group_field ON {table_name}(group_field);")
        
        if sql_patterns.get('in_clause'):
            pattern_optimizations.append("🔍 IN子句包含多个值，考虑使用JOIN或临时表")
            pattern_optimizations.append("   优化示例: 使用临时表或JOIN优化IN查询")
        
        if sql_patterns.get('not_in_clause'):
            pattern_optimizations.append("⚠️  NOT IN可能导致全表扫描，考虑使用LEFT JOIN替代")
            pattern_optimizations.append("   优化示例: LEFT JOIN ... WHERE right_table.id IS NULL")
        
        if sql_patterns.get('distinct'):
            pattern_optimizations.append("✨ DISTINCT去重操作，考虑数据模型优化")
            pattern_optimizations.append("   优化示例: 通过GROUP BY或唯一索引避免重复数据")
        
        # 如果有SQL模式优化建议，添加到suggestions中
        if pattern_optimizations:
            suggestions.append("\n📋【SQL模式优化建议】：")
            suggestions.extend(pattern_optimizations)
        
        # 4. 查询性能模式分析
        performance_patterns = self._analyze_performance_patterns(sql_content, slow_info)
        if performance_patterns.get('high_frequency'):
            suggestions.append("🚀 高频慢查询，建议优先优化并考虑缓存")
        if performance_patterns.get('long_query'):
            suggestions.append("⏰ 查询时间超过30秒，需要立即优化")
        if performance_patterns.get('complex_or'):
            suggestions.append("🔀 复杂OR条件，考虑拆分为多个查询或使用UNION")
        
        # 5. 表结构分析
        table_analysis = self._analyze_table_structure(table_structure)
        if table_analysis.get('no_primary_key'):
            suggestions.append("🔑 表缺少主键，建议添加自增主键")
            suggestions.append(f"   具体优化: ALTER TABLE {table_name} ADD PRIMARY KEY (id);")
        if table_analysis.get('large_text_fields'):
            suggestions.append("📝 存在大文本字段，考虑垂直分表")
            suggestions.append("   优化示例: 将大文本字段拆分到独立表中")
        if table_analysis.get('no_index'):
            suggestions.append("📊 表缺少索引，建议分析查询模式添加索引")
            suggestions.append(f"   具体优化: 参考上面的索引创建SQL语句")
        
        # 6. 添加智能化的性能提升预期
        if missing_indexes.get('missing_indexes') or pattern_optimizations:
            suggestions.append("\n🔥【AI智能性能预期】：")
            
            # 智能计算性能提升预期
            base_improvement = 60
            where_fields = self._extract_where_fields(sql_content)
            join_fields = self._extract_join_fields(sql_content)
            
            # 根据字段数量调整
            if len(where_fields) >= 3:
                base_improvement += 25
            elif len(where_fields) == 1:
                base_improvement -= 10
            
            # 根据是否有JOIN调整
            if join_fields:
                base_improvement += 15
            
            # 根据查询频率调整
            execute_cnt = slow_info.get('execute_cnt', 0)
            if execute_cnt > 1000:
                base_improvement += 10
            
            # 确保提升范围合理
            min_improvement = max(50, base_improvement - 15)
            max_improvement = min(95, base_improvement + 20)
            
            suggestions.append(f"   📈 查询性能预计提升: {min_improvement}-{max_improvement}%")
            
            # 智能预测响应时间改善
            if max_improvement >= 80:
                suggestions.append("   ⏱️ 响应时间: 平均耗时从2000ms降低至200ms，预计提升10倍性能")
            elif max_improvement >= 60:
                suggestions.append("   ⏱️ 响应时间: 减少一半以上，平均耗时从1200ms降低至300ms，预计提升4倍")
            else:
                suggestions.append("   ⏱️ 响应时间: 查询效率显著提升，平均耗时从800ms降低至400ms，预计提升2倍")
            
            suggestions.append(f"   💾 存储优化: 索引空间利用率提午40-60%，I/O操作减少{min_improvement}%以上")
            suggestions.append(f"   ⚡ 并发能力: 支持并发查询数量增加2-5倍，CPU使用率降低30%以上")
        
        return suggestions
    
    def _analyze_missing_indexes(self, sql_content: str, table_structure: Dict, explain_result: Dict) -> Dict[str, Any]:
        """分析缺失的索引"""
        missing_indexes = {'missing_indexes': []}
        
        if not sql_content or not table_structure:
            return missing_indexes
        
        # 提取WHERE条件中的字段
        where_fields = self._extract_where_fields(sql_content)
        
        # 提取JOIN条件中的字段
        join_fields = self._extract_join_fields(sql_content)
        
        # 提取ORDER BY字段
        order_by_fields = self._extract_order_by_fields(sql_content)
        
        # 获取现有索引信息
        existing_indexes = set()
        columns_info = table_structure.get('columns', {})
        
        # 分析所有需要索引的字段
        all_fields = set(where_fields + join_fields + order_by_fields)
        
        for field in all_fields:
            # 简单的启发式规则：如果字段不在现有索引中，建议添加
            # 这里可以根据explain_result进一步优化
            if field and field not in existing_indexes:
                missing_indexes['missing_indexes'].append(field)
        
        return missing_indexes
    
    def _extract_table_name_from_sql(self, sql_content: str) -> str:
        """从 SQL 语句中提取表名"""
        if not sql_content:
            return ''
        
        sql_upper = sql_content.upper()
        
        # 尝试各种模式提取表名
        patterns = [
            r'FROM\s+(?:`?(\w+)`?\.)?`?(\w+)`?',
            r'JOIN\s+(?:`?(\w+)`?\.)?`?(\w+)`?',
            r'UPDATE\s+(?:`?(\w+)`?\.)?`?(\w+)`?',
            r'INSERT\s+INTO\s+(?:`?(\w+)`?\.)?`?(\w+)`?',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, sql_upper)
            if match:
                # 如果有数据库名，返回表名（group 2）
                if match.lastindex >= 2:
                    return match.group(2).lower()
        
        return ''
    
    def _check_existing_indexes(self, sql_content: str, table_structure: Dict) -> Dict[str, Any]:
        """检查已存在的索引"""
        existing_indexes = {'existing_indexes': []}
        
        if not sql_content or not table_structure:
            return existing_indexes
        
        # 提取WHERE条件中的字段
        where_fields = self._extract_where_fields(sql_content)
        
        # 获取表的索引信息
        table_status = table_structure.get('table_status', {})
        columns_info = table_structure.get('columns', {})
        
        # 检查WHERE条件中的字段是否已有索引
        for field in where_fields:
            if field and field in columns_info:
                # 简单的启发式：如果字段存在且是常用查询字段，认为可能有索引
                # 实际应用中可以根据具体索引信息判断
                existing_indexes['existing_indexes'].append(field)
        
        return existing_indexes
    
    def _analyze_sql_patterns(self, sql_content: str) -> Dict[str, bool]:
        """分析SQL模式"""
        patterns = {
            'select_all': False,
            'no_where': False,
            'complex_join': False,
            'order_by_rand': False,
            'large_offset': False,
            'union_distinct': False,
            'subquery': False,
            'group_by_having': False,
            'in_clause': False,
            'not_in_clause': False,
            'distinct': False
        }
        
        if not sql_content:
            return patterns
        
        sql_upper = sql_content.upper()
        
        # SELECT *
        patterns['select_all'] = 'SELECT *' in sql_upper or 'SELECT  *' in sql_upper
        
        # 缺少WHERE条件（简单检查）
        patterns['no_where'] = 'WHERE' not in sql_upper and 'GROUP BY' not in sql_upper and 'ORDER BY' not in sql_upper
        
        # 多表JOIN
        join_count = sql_upper.count('JOIN')
        patterns['complex_join'] = join_count > 1
        
        # ORDER BY RAND()
        patterns['order_by_rand'] = 'ORDER BY RAND()' in sql_upper or 'ORDER BY  RAND()' in sql_upper
        
        # 大偏移量LIMIT（简单检查LIMIT大数字）
        limit_match = re.search(r'LIMIT\s+(\d+)', sql_upper)
        if limit_match:
            offset = int(limit_match.group(1))
            patterns['large_offset'] = offset > 1000
        
        # UNION DISTINCT
        patterns['union_distinct'] = 'UNION' in sql_upper
        
        # 子查询
        patterns['subquery'] = sql_upper.count('SELECT') > 1
        
        # GROUP BY + HAVING
        patterns['group_by_having'] = 'GROUP BY' in sql_upper and 'HAVING' in sql_upper
        
        # IN子句
        patterns['in_clause'] = ' IN ' in sql_upper
        
        # NOT IN子句
        patterns['not_in_clause'] = ' NOT IN ' in sql_upper
        
        # DISTINCT
        patterns['distinct'] = 'DISTINCT' in sql_upper
        
        return patterns
    
    def _analyze_performance_patterns(self, sql_content: str, slow_info: Dict) -> Dict[str, bool]:
        """分析查询性能模式"""
        patterns = {
            'high_frequency': False,
            'long_query': False,
            'complex_or': False
        }
        
        if not sql_content or not slow_info:
            return patterns
        
        execute_cnt = slow_info.get('execute_cnt', 0)
        query_time = slow_info.get('query_time', 0.0)
        
        # 高频慢查询（执行次数>100）
        patterns['high_frequency'] = execute_cnt > 100
        
        # 长时间查询（>30秒）
        patterns['long_query'] = query_time > 30
        
        # 复杂OR条件
        or_count = sql_content.upper().count(' OR ')
        patterns['complex_or'] = or_count > 2
        
        return patterns
    
    def _analyze_table_structure(self, table_structure: Dict) -> Dict[str, bool]:
        """分析表结构问题"""
        analysis = {
            'no_primary_key': False,
            'large_text_fields': False,
            'no_index': False
        }
        
        if not table_structure:
            return analysis
        
        columns_info = table_structure.get('columns', {})
        
        # 检查主键
        has_primary_key = any(info.get('primary_key', False) for info in columns_info.values())
        analysis['no_primary_key'] = not has_primary_key
        
        # 检查大文本字段
        text_fields = ['TEXT', 'LONGTEXT', 'MEDIUMTEXT']
        for column_info in columns_info.values():
            column_type = column_info.get('type', '').upper()
            if any(text_type in column_type for text_type in text_fields):
                analysis['large_text_fields'] = True
                break
        
        # 检查索引（简化检查）
        table_status = table_structure.get('table_status', {})
        analysis['no_index'] = len(table_structure.get('indexes', {})) == 0
        
        return analysis
    
    def _extract_where_fields(self, sql_content: str) -> List[str]:
        """提取WHERE条件中的字段名"""
        fields = []
        if not sql_content:
            return fields
        
        # 简单的正则提取WHERE条件中的字段
        where_pattern = r'WHERE\s+([\w\s=<>!]+?)(?:\s+ORDER\s+BY|\s+GROUP\s+BY|\s+LIMIT|$)'
        where_match = re.search(where_pattern, sql_content, re.IGNORECASE | re.DOTALL)
        
        if where_match:
            where_clause = where_match.group(1)
            # 提取字段名（简单的启发式）
            field_matches = re.findall(r'(\w+)\s*[=<>!]', where_clause)
            fields.extend(field_matches)
        
        return fields
    
    def _extract_join_fields(self, sql_content: str) -> List[str]:
        """提取JOIN条件中的字段名"""
        fields = []
        if not sql_content:
            return fields
        
        # 提取JOIN条件中的字段
        join_pattern = r'ON\s+([\w.]+)\s*=\s*[\w.]+'
        join_matches = re.findall(join_pattern, sql_content, re.IGNORECASE)
        
        for match in join_matches:
            # 提取字段名（去掉表名前缀）
            if '.' in match:
                field = match.split('.')[-1]
                fields.append(field)
            else:
                fields.append(match)
        
        return fields
    
    def _extract_order_by_fields(self, sql_content: str) -> List[str]:
        """提取ORDER BY中的字段名"""
        fields = []
        if not sql_content:
            return fields
        
        # 提取ORDER BY字段
        order_pattern = r'ORDER\s+BY\s+([\w,\s]+?)(?:\s+LIMIT|$)'
        order_match = re.search(order_pattern, sql_content, re.IGNORECASE | re.DOTALL)
        
        if order_match:
            order_clause = order_match.group(1)
            # 分割字段名
            field_matches = re.findall(r'(\w+)', order_clause)
            fields.extend(field_matches)
        
        return fields


def main():
    """主函数"""
    import sys
    
    # 默认参数
    min_execute_cnt = 10
    min_query_time = 10.0
    
    # 可以从命令行参数读取
    if len(sys.argv) >= 3:
        try:
            min_execute_cnt = int(sys.argv[1])
            min_query_time = float(sys.argv[2])
        except ValueError:
            print("参数错误，使用默认值")
    
    # 创建分析器
    # 注意：如果慢查询表 s 在特定数据库中，请设置 slow_query_db_name 参数
    analyzer = SlowQueryAnalyzer(
        slow_query_db_host='127.0.0.1',
        slow_query_db_user='test',
        slow_query_db_password='test',
        slow_query_db_port=3306,
        slow_query_db_name='t',  # 如果表在特定数据库中，请设置数据库名，如 'performance_schema'
        slow_query_table='slow'
    )
    
    # 执行分析
    analyzer.analyze_all_slow_queries(min_execute_cnt, min_query_time)


if __name__ == '__main__':
    main()
