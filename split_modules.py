#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动拆分 database_optimization_report.py 为多个模块
"""

import re
import ast
import os

def extract_methods_from_file(file_path):
    """从文件中提取所有方法"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 使用AST解析
    tree = ast.parse(content)
    
    methods = {}
    current_class = None
    
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            current_class = node.name
            methods[current_class] = []
        elif isinstance(node, ast.FunctionDef):
            if current_class:
                methods[current_class].append({
                    'name': node.name,
                    'start_line': node.lineno,
                    'end_line': node.end_lineno if hasattr(node, 'end_lineno') else node.lineno
                })
    
    return methods

def read_method_code(file_path, start_line, end_line):
    """读取方法的代码"""
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        return ''.join(lines[start_line-1:end_line])

# 由于文件太大，这个脚本主要用于分析
# 实际拆分需要手动完成或使用更复杂的工具

if __name__ == '__main__':
    file_path = 'database_optimization_report.py'
    methods = extract_methods_from_file(file_path)
    
    print("找到的方法:")
    for class_name, method_list in methods.items():
        print(f"\n{class_name}:")
        for method in method_list:
            print(f"  - {method['name']} (lines {method['start_line']}-{method['end_line']})")

