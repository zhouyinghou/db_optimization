#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库连接管理模块
提供分离的数据库连接配置和安全连接管理
"""

import pymysql
from typing import Dict, Optional
import logging

logger = logging.getLogger('DatabaseConnectionManager')

class DatabaseConnectionManager:
    """数据库连接管理器，支持分离的慢查询和业务数据库连接"""
    
    def __init__(self, slow_query_db_config: Dict = None, business_db_config: Dict = None):
        """
        初始化数据库连接管理器
        
        Args:
            slow_query_db_config: 慢查询数据库配置
            business_db_config: 业务数据库配置
        """
        self.slow_query_db_config = slow_query_db_config or {}
        self.business_db_config = business_db_config or {}
        self._active_connection = None
        
        # 初始化慢查询数据库配置
        self.slow_query_db_host = self.slow_query_db_config.get('host', '127.0.0.1')
        self.slow_query_db_user = self.slow_query_db_config.get('user', 'test')
        self.slow_query_db_password = self.slow_query_db_config.get('password', 'test')
        self.slow_query_db_port = self.slow_query_db_config.get('port', 3306)
        self.slow_query_db_name = self.slow_query_db_config.get('database', '')
        self.slow_query_table = self.slow_query_db_config.get('table', 'slow')
        
        # 初始化业务数据库配置
        self.business_db_host = self.business_db_config.get('host', '127.0.0.1')
        self.business_db_user = self.business_db_config.get('user', 'test')
        self.business_db_password = self.business_db_config.get('password', 'test')
        self.business_db_port = self.business_db_config.get('port', 3306)
    
    def get_slow_query_config(self) -> Dict:
        """获取慢查询数据库配置"""
        return {
            'host': self.slow_query_db_host,
            'port': self.slow_query_db_port,
            'user': self.slow_query_db_user,
            'password': self.slow_query_db_password,
            'database': self.slow_query_db_name,
            'table': self.slow_query_table,
            'charset': 'utf8mb4'
        }
    
    def get_business_db_config(self, hostname: str = None, database: str = None) -> Dict:
        """
        获取业务数据库配置
        
        Args:
            hostname: 主机名，如果为None则使用默认主机
            database: 数据库名
            
        Returns:
            数据库连接配置字典
        """
        return {
            'host': hostname or self.business_db_host,
            'port': self.business_db_port,
            'user': self.business_db_user,
            'password': self.business_db_password,
            'database': database,
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }
    
    def get_standby_hostname(self, master_hostname: str) -> Optional[str]:
        """
        通过cluster表查询获取备库IP地址（使用业务数据库连接）
        
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
                    logger.warning(f"在cluster表中未找到主库 {master_hostname} 的记录")
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
                    logger.warning(f"集群 {cluster_name} 未找到备库记录")
                    conn.close()
                    return None
                
                # 返回第一个备库IP（通常只有一个备库）
                standby_hostname = standby_results[0][0]
                
                conn.close()
                return standby_hostname
                
        except Exception as e:
            logger.error(f"查询cluster表获取备库信息失败: {str(e)}")
            return None
    
    def get_safe_connection(self, hostname: str = None, database: str = None) -> dict:
        """
        安全地获取数据库连接，添加保护层（使用业务数据库配置）
        
        Args:
            hostname: 主机名
            database: 数据库名
            
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
            logger.warning(f"未找到备库信息，使用原主机: {original_host}")
        
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
                'message': f'成功连接到业务数据库: {host}',
                'connection': connection,
                'is_standby': standby_host is not None
            }
            
        except Exception as e:
            error_msg = f"连接业务数据库失败: {str(e)}"
            logger.error(error_msg)
            return {
                'status': 'error',
                'message': error_msg,
                'connection': None
            }
    
    def close_safe_connection(self):
        """安全关闭数据库连接"""
        if self._active_connection:
            try:
                self._active_connection.close()
                self._active_connection = None
                logger.info("已安全关闭数据库连接")
                return True
            except Exception as e:
                logger.error(f"关闭数据库连接失败: {str(e)}")
                return False
        return True
    
    def execute_safe_query(self, query: str, params: tuple = None, hostname: str = None, database: str = None) -> dict:
        """
        安全执行查询（使用业务数据库连接）
        
        Args:
            query: SQL查询语句
            params: 查询参数
            hostname: 主机名
            database: 数据库名
            
        Returns:
            dict: 查询结果
        """
        conn_result = self.get_safe_connection(hostname, database)
        
        if conn_result['status'] != 'success':
            return {
                'status': 'error',
                'message': f"无法获取数据库连接: {conn_result['message']}",
                'data': None
            }
        
        connection = conn_result['connection']
        
        try:
            with connection.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                if query.strip().upper().startswith('SELECT'):
                    result = cursor.fetchall()
                    return {
                        'status': 'success',
                        'message': '查询执行成功',
                        'data': result,
                        'row_count': len(result) if result else 0
                    }
                else:
                    connection.commit()
                    return {
                        'status': 'success',
                        'message': 'SQL执行成功',
                        'data': None,
                        'affected_rows': cursor.rowcount
                    }
                    
        except Exception as e:
            connection.rollback()
            error_msg = f"查询执行失败: {str(e)}"
            logger.error(error_msg)
            return {
                'status': 'error',
                'message': error_msg,
                'data': None
            }
            
        finally:
            self.close_safe_connection()
    
    def get_table_row_count(self, hostname: str, database: str, table_name: str) -> dict:
        """
        安全获取表的行数（使用业务数据库连接）
        
        Args:
            hostname: 主机名
            database: 数据库名
            table_name: 表名
            
        Returns:
            dict: 包含行数信息的字典
        """
        # 验证表名的安全性，防止SQL注入
        table_name = table_name.strip('`').strip("'").strip('"')
        if not table_name.replace('_', '').replace('-', '').isalnum():
            return {
                'status': 'error',
                'message': '表名包含非法字符',
                'row_count': None
            }
        
        query = f"SELECT COUNT(*) as row_count FROM `{database}`.`{table_name}`"
        
        result = self.execute_safe_query(query, hostname=hostname, database=database)
        
        if result['status'] == 'success' and result['data']:
            row_count = result['data'][0]['row_count'] if result['data'][0] else 0
            return {
                'status': 'success',
                'message': f"成功获取表 {table_name} 的行数",
                'row_count': row_count,
                'table_name': table_name
            }
        else:
            return {
                'status': 'error',
                'message': f"获取表行数失败: {result['message']}",
                'row_count': None
            }