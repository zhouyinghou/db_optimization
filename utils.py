#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
工具函数模块
包含通用的工具函数，如安全打印、配置加载等
"""

import json
import sys
import io
from typing import Dict, Optional


def safe_print(*args, **kwargs):
    """安全的打印函数，处理 Windows 下的编码问题"""
    try:
        print(*args, **kwargs)
    except UnicodeEncodeError:
        # 如果编码失败，尝试替换无法编码的字符
        output = io.StringIO()
        try:
            print(*args, file=output, **kwargs)
            text = output.getvalue()
            # 移除或替换 emoji 字符
            text = text.encode('ascii', 'ignore').decode('ascii')
            print(text, end='')
        except:
            # 最后的备选方案：只打印 ASCII 字符
            text = ' '.join(str(arg).encode('ascii', 'ignore').decode('ascii') for arg in args)
            print(text, **kwargs)


def setup_encoding():
    """设置 Windows 下的编码"""
    if sys.platform == 'win32':
        try:
            # 尝试设置标准输出为 UTF-8
            if hasattr(sys.stdout, 'reconfigure'):
                sys.stdout.reconfigure(encoding='utf-8')
            if hasattr(sys.stderr, 'reconfigure'):
                sys.stderr.reconfigure(encoding='utf-8')
        except (AttributeError, ValueError):
            # 如果 reconfigure 不可用，使用环境变量
            try:
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
                sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
            except:
                # 如果都失败了，使用安全的 print 函数
                global print
                print = safe_print


def load_db_config(config_file: str = 'db_config.json') -> Optional[Dict]:
    """
    从配置文件加载数据库配置
    支持处理单配置对象或配置数组
    
    Args:
        config_file: 配置文件路径
    
    Returns:
        数据库配置字典，如果加载失败返回None
    """
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
            
            # 处理配置数组格式
            if isinstance(config_data, list):
                # 如果是数组，取第一个配置项作为默认配置
                if not config_data:
                    print(f"❌ 配置文件中没有配置项")
                    return None
                config = config_data[0]
                print(f"⚠️  检测到配置数组，使用第一个配置项")
            else:
                config = config_data
            
            # 验证必要的配置项
            required_fields = ['host', 'user', 'password']
            for field in required_fields:
                if field not in config:
                    print(f"❌ 配置文件缺少必要项: {field}")
                    return None
            
            # 添加慢查询分析默认参数
            config.setdefault('table', 's')  # 默认慢查询表名
            config.setdefault('port', 3306)  # 默认端口
            
            return config
    except FileNotFoundError:
        print(f"❌ 配置文件不存在: {config_file}")
        return None
    except json.JSONDecodeError:
        print(f"❌ 配置文件格式错误: {config_file}")
        return None


# 初始化编码设置
setup_encoding()

