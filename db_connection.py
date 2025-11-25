#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据库连接管理模块
提供安全的数据库连接管理功能
"""

import pymysql
import logging
from typing import Dict, Optional, Any, Union
from contextlib import contextmanager

logger = logging.getLogger('ConnectionManager')


class ConnectionManager:
    """数据库连接管理器"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化连接管理器
        
        Args:
            config: 数据库配置字典
        """
        self.config = config
        self.timeout = config.get('timeout', 30)
        self.retries = config.get('retries', 3)
        self.has_active_connection = False
        logger.info(f"连接管理器初始化: {config.get('host', 'unknown')}:{config.get('port', 3306)}")
    
    @contextmanager
    def get_connection(self):
        """
        获取数据库连接的上下文管理器
        
        Returns:
            数据库连接对象
        """
        connection = None
        try:
            connection = self._create_connection()
            yield connection
        except Exception as e:
            logger.error(f"数据库操作失败: {e}")
            raise
        finally:
            if connection:
                connection.close()
    
    def _create_connection(self) -> pymysql.Connection:
        """
        创建数据库连接
        
        Returns:
            PyMySQL连接对象
        """
        try:
            connection = pymysql.connect(
                host=self.config['host'],
                port=self.config.get('port', 3306),
                user=self.config['user'],
                password=self.config['password'],
                database=self.config.get('database', ''),
                charset=self.config.get('charset', 'utf8mb4'),
                connect_timeout=self.config.get('connect_timeout', 5),
                read_timeout=self.config.get('read_timeout', 30),
                write_timeout=self.config.get('write_timeout', 30)
            )
            
            self.has_active_connection = True
            logger.debug(f"成功创建数据库连接: {self.config['host']}:{self.config.get('port', 3306)}")
            return connection
            
        except Exception as e:
            logger.error(f"创建数据库连接失败: {e}")
            self.has_active_connection = False
            raise
    
    def test_connection(self) -> bool:
        """
        测试数据库连接
        
        Returns:
            连接是否成功
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result is not None
        except Exception as e:
            logger.error(f"连接测试失败: {e}")
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """
        获取连接信息
        
        Returns:
            连接信息字典
        """
        return {
            'config': {
                'host': self.config['host'],
                'user': self.config['user'],
                'port': self.config.get('port', 3306),
                'database': self.config.get('database', '')
            },
            'timeout': self.timeout,
            'retries': self.retries,
            'has_active_connection': self.has_active_connection
        }


def create_connection_manager(config: Dict[str, Any]) -> ConnectionManager:
    """
    创建连接管理器实例
    
    Args:
        config: 数据库配置
        
    Returns:
        ConnectionManager实例
    """
    return ConnectionManager(config)


def create_simple_connection(config: Dict[str, Any]) -> pymysql.Connection:
    """
    创建简单的数据库连接
    
    Args:
        config: 数据库配置
        
    Returns:
        PyMySQL连接对象
    """
    return pymysql.connect(
        host=config['host'],
        port=config.get('port', 3306),
        user=config['user'],
        password=config['password'],
        database=config.get('database', ''),
        charset=config.get('charset', 'utf8mb4'),
        connect_timeout=config.get('connect_timeout', 5),
        read_timeout=config.get('read_timeout', 30),
        write_timeout=config.get('write_timeout', 30)
    )


@contextmanager
def get_database_connection(config: Dict[str, Any]):
    """
    获取数据库连接的便利函数
    
    Args:
        config: 数据库配置
    """
    connection = None
    try:
        connection = create_simple_connection(config)
        yield connection
    except Exception as e:
        logger.error(f"数据库操作失败: {e}")
        raise
    finally:
        if connection:
            connection.close()