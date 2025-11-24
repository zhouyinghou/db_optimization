#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库辅助模块
包含数据库连接、查询、索引检查等相关方法
"""

import pymysql
from typing import Dict, Optional, List, Set


class DatabaseHelper:
    """数据库辅助类"""
    
    def __init__(self, business_db_config: Dict = None, slow_query_db_config: Dict = None):
        """
        初始化数据库辅助类
        
        Args:
            business_db_config: 业务数据库配置
            slow_query_db_config: 慢查询数据库配置
        """
        self.business_db_config = business_db_config or {}
        self.slow_query_db_config = slow_query_db_config or {}
        
        # 业务数据库配置
        self.business_db_host = self.business_db_config.get('host', '127.0.0.1')
        self.business_db_port = self.business_db_config.get('port', 3306)
        self.business_db_user = self.business_db_config.get('user', 'test')
        self.business_db_password = self.business_db_config.get('password', 'test')
        
        # 慢查询数据库配置
        self.slow_query_db_host = self.slow_query_db_config.get('host', '127.0.0.1')
        self.slow_query_db_port = self.slow_query_db_config.get('port', 3306)
        self.slow_query_db_user = self.slow_query_db_config.get('user', 'test')
        self.slow_query_db_password = self.slow_query_db_config.get('password', 'test')
        
        self._active_connection = None
    
    def get_standby_hostname(self, master_hostname: str) -> Optional[str]:
        """
        通过cluster表查询获取备库IP地址
        
        Args:
            master_hostname: 主库主机名/IP
            
        Returns:
            Optional[str]: 备库IP地址，如果未找到返回None
        """
        if not master_hostname:
            return None
        
        try:
            # 连接到t数据库查询cluster表（使用业务数据库连接配置）
            conn = pymysql.connect(
                host=self.business_db_host,
                port=self.business_db_port,
                user=self.business_db_user,
                password=self.business_db_password,
                database='t',
                charset='utf8mb4',
                connect_timeout=5
            )
            
            with conn.cursor() as cursor:
                # 查询cluster表获取主库信息
                cursor.execute(
                    """SELECT cluster_name FROM cluster 
                       WHERE ip = %s AND instance_role = 'M'""",
                    (master_hostname,)
                )
                master_result = cursor.fetchone()
                
                if not master_result:
                    print(f"❌ 在cluster表中未找到主库 {master_hostname} 的记录")
                    conn.close()
                    return None
                
                cluster_name = master_result[0]
                
                # 查询同集群的备库
                cursor.execute(
                    """SELECT ip FROM cluster 
                       WHERE cluster_name = %s AND instance_role = 'S'""",
                    (cluster_name,)
                )
                standby_results = cursor.fetchall()
                
                if not standby_results:
                    print(f"❌ 集群 {cluster_name} 未找到备库记录")
                    conn.close()
                    return None
                
                # 返回第一个备库IP（通常只有一个备库）
                standby_hostname = standby_results[0][0]
                
                conn.close()
                return standby_hostname
                
        except Exception as e:
            print(f"❌ 查询cluster表获取备库信息失败: {str(e)}")
            return None
    
    def get_safe_connection(self, hostname: str = None, database: str = None) -> dict:
        """
        安全地获取数据库连接，添加保护层
        
        Returns:
            dict: 包含连接状态和连接对象的字典
        """
        # 🎯 优先使用备库避免主库性能风险（使用业务数据库配置）
        original_host = hostname if hostname and hostname != 'localhost' else self.business_db_host
        
        # 尝试获取备库IP
        standby_host = self.get_standby_hostname(original_host)
        
        if standby_host:
            host = standby_host
        else:
            host = original_host
            print(f"⚠️ 未找到备库信息，使用原主机: {original_host}")
        
        # 检查是否已经有活跃连接（限制只有一个连接）
        if self._active_connection:
            return {
                'status': 'error',
                'message': '已存在活跃数据库连接，不允许创建新连接',
                'connection': None
            }
        
        connection = None
        try:
            # 首先创建一个连接来检查系统状态（使用业务数据库配置）
            check_conn = pymysql.connect(
                host=host,
                port=self.business_db_port,
                user=self.business_db_user,
                password=self.business_db_password,
                charset='utf8mb4',
                connect_timeout=5
            )
            
            with check_conn.cursor() as cursor:
                # 1. 检查活跃会话数是否超过10
                cursor.execute("SELECT COUNT(*) as active_sessions FROM information_schema.processlist WHERE command != 'Sleep'")
                result = cursor.fetchone()
                active_sessions = result[0] if result else 0
                
                if active_sessions > 10:
                    check_conn.close()
                    return {
                        'status': 'error',
                        'message': f'数据库活跃会话数({active_sessions})超过10，暂不执行操作',
                        'connection': None
                    }
                
                # 2. 检查当前用户权限，确保只有查询权限
                cursor.execute("SELECT * FROM information_schema.user_privileges WHERE grantee LIKE %s AND privilege_type IN ('SELECT', 'SELECT, INSERT, UPDATE, DELETE')", 
                             (f"'%{self.business_db_user}%'",))
                privileges = cursor.fetchall()
                
                has_write_privilege = any('INSERT' in str(priv) or 'UPDATE' in str(priv) or 'DELETE' in str(priv) for priv in privileges)
                if has_write_privilege:
                    # 重新连接，设置会话参数（使用业务数据库配置）
                    check_conn.close()
                    connection = pymysql.connect(
                        host=host,
                        port=self.business_db_port,
                        user=self.business_db_user,
                        password=self.business_db_password,
                        charset='utf8mb4',
                        connect_timeout=5,
                        init_command="SET SESSION sql_mode='STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'"
                    )
                else:
                    connection = pymysql.connect(
                        host=host,
                        port=self.business_db_port,
                        user=self.business_db_user,
                        password=self.business_db_password,
                        charset='utf8mb4',
                        connect_timeout=5
                    )
            
            # 设置连接为只读模式
            with connection.cursor() as cursor:
                cursor.execute("SET SESSION sql_safe_updates=1")
                cursor.execute("SET SESSION sql_select_limit=1000")  # 限制查询结果集大小
            
            # 记录活跃连接
            self._active_connection = connection
            
            return {
                'status': 'success',
                'message': '数据库连接创建成功',
                'connection': connection
            }
            
        except Exception as e:
            # 清理连接
            if 'check_conn' in locals() and check_conn:
                try:
                    check_conn.close()
                except:
                    pass
            if connection:
                try:
                    connection.close()
                except:
                    pass
            
            return {
                'status': 'error',
                'message': f'数据库连接失败: {str(e)}',
                'connection': None
            }
    
    def close_safe_connection(self):
        """安全关闭数据库连接"""
        if self._active_connection:
            try:
                self._active_connection.close()
                self._active_connection = None
            except:
                pass
    
    def execute_safe_query(self, query: str, params: tuple = None, hostname: str = None, database: str = None) -> dict:
        """
        安全执行数据库查询
        
        Args:
            query: SQL查询语句
            params: 查询参数
            hostname: 主机名
            database: 数据库名
            
        Returns:
            dict: 查询结果
        """
        # 检查查询语句是否包含危险操作
        dangerous_keywords = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE']
        if any(keyword in query.upper() for keyword in dangerous_keywords):
            print("⚠️ 查询被拒绝：包含危险操作")
            return {
                'status': 'error',
                'message': '查询包含危险操作，仅允许SELECT查询',
                'data': None
            }
        
        # 检查是否全表扫描（没有WHERE条件的SELECT）
        if query.upper().startswith('SELECT') and 'WHERE' not in query.upper():
            # 简单查询可以允许，但复杂查询需要检查
            if 'JOIN' in query.upper() or 'FROM' in query.upper() and query.upper().count('FROM') > 1:
                print("⚠️ 查询被拒绝：可能涉及全表扫描")
                return {
                    'status': 'error',
                    'message': '查询可能涉及全表扫描，请添加适当的WHERE条件',
                    'data': None
                }
        
        # 获取安全连接
        conn_result = self.get_safe_connection(hostname, database)
        if conn_result['status'] != 'success':
            print(f"❌ 连接失败: {conn_result.get('message', '未知错误')}")
            return conn_result
        
        connection = conn_result['connection']
        
        try:
            with connection.cursor() as cursor:
                # 如果指定了数据库，先选择数据库
                if database:
                    cursor.execute(f"USE `{database}`")
                
                cursor.execute(query, params)
                result = cursor.fetchall()
                
                return {
                    'status': 'success',
                    'message': '查询执行成功',
                    'data': result
                }
                
        except Exception as e:
            print(f"❌ 查询执行异常: {str(e)}")
            return {
                'status': 'error',
                'message': f'查询执行失败: {str(e)}',
                'data': None
            }
        finally:
            self.close_safe_connection()
    
    def check_table_exists(self, database: str, table_name: str, hostname: str = None) -> bool:
        """
        检查表是否存在（安全版本）
        """
        if not database or not table_name:
            return False
        
        # 使用安全查询执行
        query_result = self.execute_safe_query(
            "SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
            (database, table_name),
            hostname,
            database
        )
        
        if query_result['status'] == 'success' and query_result['data']:
            # 查询返回的是元组，第一个元素是计数
            try:
                count = int(query_result['data'][0][0]) if query_result['data'][0][0] is not None else 0
                return count > 0
            except (ValueError, TypeError, IndexError):
                # 如果数据格式异常，返回False
                return False
        
        # 如果查询失败，说明数据库连接有问题或表不存在，返回False
        print(f"⚠️ 表存在性检查失败，数据库连接异常或表不存在，返回False")
        return False
    
    def get_table_indexes_from_db(self, database: str, table_name: str, hostname: str = None) -> Optional[Set[str]]:
        """
        从数据库中获取表的索引信息（安全版本）
        
        Args:
            database: 数据库名
            table_name: 表名
            hostname: 主机名（可选），用于连接真实的业务数据库
        
        Returns:
            Optional[Set[str]]: 索引字段集合，如果查询失败返回None
        """
        indexes = set()
        
        if not database or not table_name:
            return indexes
        
        # 使用安全查询获取索引信息（支持hostname参数）
        query_result = self.execute_safe_query(
            f"SHOW INDEX FROM `{table_name}`",
            hostname=hostname,
            database=database
        )
        
        # 区分查询失败和表没有索引的情况
        if query_result['status'] == 'error':
            # 查询失败，返回None表示不确定状态
            print(f"❌ 数据库查询失败: {query_result.get('message', 'Unknown error')}")
            return None
        elif query_result['status'] == 'success':
            if query_result['data']:
                # 查询成功且有数据
                for row in query_result['data']:
                    # SHOW INDEX返回的是元组，需要按位置获取Column_name
                    # MySQL SHOW INDEX的列顺序：Table, Non_unique, Key_name, Seq_in_index, Column_name, ...
                    if len(row) >= 5:  # Column_name在第5个位置（索引4）
                        column_name = row[4]  # Column_name字段
                        if column_name:
                            indexes.add(column_name.lower())
                return indexes
            else:
                # 查询成功但没有数据（表确实没有索引）
                print(f"ℹ️ 表 {table_name} 在数据库 {database} 中没有索引")
                return set()  # 返回空集合表示确认没有索引
        
        # 其他情况返回None表示不确定
        return None
    
    def find_correct_database_for_table(self, table_name: str, hostname: Optional[str] = None) -> str:
        """
        查找包含指定表的正确数据库（安全版本）
        
        Args:
            table_name: 表名
            hostname: 主机名（可选），如果提供则使用该主机查找数据库
            
        Returns:
            包含该表的数据库名，如果未找到返回空字符串
        """
        if not table_name:
            return ""
                
        # 需要排除的数据库
        excluded_dbs = ['information_schema', 'c2c_db', 'mysql', 'performance_schema', 'sys']
        # 添加trans_00到trans_34到排除列表
        for i in range(35):
            excluded_dbs.append(f'trans_{i:02d}')
                
        # 使用安全查询获取所有数据库
        query_result = self.execute_safe_query("SHOW DATABASES", hostname=hostname)
        
        if query_result['status'] != 'success' or not query_result['data']:
            print(f"❌ 获取数据库列表失败: {query_result.get('message', '未知错误')}")
            return ""
        
        # 获取所有数据库
        all_dbs = [db[0] for db in query_result['data']]
        
        # 过滤掉排除的数据库
        candidate_dbs = [db for db in all_dbs if db not in excluded_dbs]
        
        # 在每个候选数据库中查找表
        for db in candidate_dbs:
            # 使用安全查询检查表是否存在
            check_result = self.execute_safe_query(
                "SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                (db, table_name),
                hostname,
                db
            )
            
            if check_result['status'] == 'success' and check_result['data']:
                if check_result['data'][0][0] > 0:  # 元组的第一个元素是计数
                    return db
        
        print(f"❌ 表 '{table_name}' 未在任何数据库中找到")
        return ""
    
    def get_table_row_count(self, database: str, table_name: str, hostname: str = None) -> Optional[int]:
        """
        获取表的行数（使用指定的hostname连接）
        
        Args:
            database: 数据库名称
            table_name: 表名
            hostname: 主机名（可选），如果提供则使用该主机获取数据库IP
            
        Returns:
            Optional[int]: 表的行数，如果查询失败返回None
        """
        if not table_name:
            return None
        
        import pymysql
        
        try:
            # 使用传入的hostname或默认配置
            if hostname and hostname != 'localhost':
                db_host = hostname
            else:
                db_host = self.business_db_host
            
            # 直接创建连接获取表信息
            conn = pymysql.connect(
                host=db_host,
                port=self.business_db_port,
                user=self.business_db_user,
                password=self.business_db_password,
                charset='utf8mb4',
                connect_timeout=5
            )
            
            with conn.cursor() as cursor:
                # 首先通过information_schema获取表的基本信息
                cursor.execute(
                    """SELECT data_length, index_length, engine 
                       FROM information_schema.tables 
                       WHERE table_schema = %s AND table_name = %s""",
                    (database, table_name)
                )
                size_result = cursor.fetchone()
                
                if not size_result:
                    print(f"⚠️ 无法在数据库 {database} 中找到表 {table_name}")
                    conn.close()
                    return None
                
                data_length = size_result[0] or 0
                index_length = size_result[1] or 0
                engine = size_result[2] or 'InnoDB'
                
                print(f"ℹ️ 表 {table_name} 信息: 数据长度={data_length}, 索引长度={index_length}, 引擎={engine}")
                
                # 对于大表，使用information_schema的估算值
                cursor.execute(
                    """SELECT table_rows 
                       FROM information_schema.tables 
                       WHERE table_schema = %s AND table_name = %s""",
                    (database, table_name)
                )
                rows_result = cursor.fetchone()
                
                if rows_result and rows_result[0]:
                    estimated_rows = rows_result[0]
                    if estimated_rows is not None and estimated_rows > 0:
                        print(f"ℹ️ 使用information_schema估算表 {table_name} 行数: {{:,}} (估算值)".format(estimated_rows))
                        conn.close()
                        return estimated_rows
                
                # 如果information_schema不可用，尝试使用SHOW TABLE STATUS
                cursor.execute(f"SHOW TABLE STATUS FROM `{database}` LIKE '{table_name}'")
                table_status_result = cursor.fetchone()
                
                if table_status_result and len(table_status_result) > 4:
                    estimated_rows = table_status_result[4]  # Rows字段
                    if estimated_rows is not None and estimated_rows > 0:
                        print(f"ℹ️ 使用SHOW TABLE STATUS估算表 {table_name} 行数: {{:,}} (估算值)".format(estimated_rows))
                        conn.close()
                        return estimated_rows
                
                # 对于大表，如果上述方法都失败，根据数据长度进行估算
                if data_length > 100 * 1024 * 1024:  # >100MB
                    # 根据经验，假设平均每行1KB，这只是一个粗略估算
                    rough_estimate = data_length // 1024
                    print(f"⚠️ 表 {table_name} 数据量较大 ({{:.1f}}MB)，使用粗略估算: {{:,}}行".format(data_length / 1024 / 1024, rough_estimate))
                    conn.close()
                    return rough_estimate if rough_estimate > 0 else 10000  # 最小返回10000
                
                conn.close()
                print(f"⚠️ 无法获取表 {table_name} 的行数，返回None")
                return None
                
        except Exception as e:
            print(f"❌ 获取表 {table_name} 行数时发生异常: {str(e)}")
            return None

