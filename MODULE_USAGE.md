# 模块使用说明

## 已拆分的模块

### 1. utils.py
**功能**: 工具函数
```python
from utils import load_db_config, setup_encoding, safe_print

# 设置编码
setup_encoding()

# 加载配置
config = load_db_config('db_config.json')

# 安全打印
safe_print("Hello, 世界!")
```

### 2. data_masking.py
**功能**: 数据脱敏
```python
from data_masking import DataMasking

# 脱敏数据库名
masked_db = DataMasking.mask_db_name("mydatabase")

# 脱敏表名
masked_table = DataMasking.mask_table_name("user_table")

# 脱敏SQL
masked_sql = DataMasking.mask_sql("SELECT * FROM users WHERE id=1")

# 批量脱敏
masked_data = DataMasking.mask_sensitive_data(data_list)
```

### 3. sql_analyzer.py
**功能**: SQL分析
```python
from sql_analyzer import SQLAnalyzer

sql = "SELECT * FROM users WHERE id=1 AND name='test' ORDER BY created_at"

# 提取表名
table_name = SQLAnalyzer.extract_table_name(sql)

# 提取WHERE字段
where_fields = SQLAnalyzer.extract_where_fields(sql)

# 提取JOIN字段
join_fields = SQLAnalyzer.extract_join_fields(sql)

# 提取ORDER BY字段
order_fields = SQLAnalyzer.extract_order_by_fields(sql)

# 字段优先级排序
sorted_fields = SQLAnalyzer.sort_fields_by_priority(where_fields, sql.lower())
```

### 4. data_processor.py
**功能**: 数据处理
```python
from data_processor import DataProcessor

# 过滤排除的表
filtered = DataProcessor.filter_excluded_tables(queries, ['test_table'])

# 格式化优化建议
suggestion = DataProcessor.format_deepseek_suggestions(deepseek_result, sql)

# 转换分析结果为查询
queries = DataProcessor.convert_analysis_to_queries(analysis_results, format_func)

# 合并分析结果
DataProcessor.merge_analysis_results_to_compare_data(compare_data, results, format_func)

# 创建对比数据
compare_data = DataProcessor.create_compare_data_with_analysis(results, format_func)
```

### 5. database_helper.py
**功能**: 数据库操作
```python
from database_helper import DatabaseHelper

# 初始化
db_helper = DatabaseHelper(
    business_db_config={'host': '127.0.0.1', 'port': 3306, 'user': 'test', 'password': 'test'},
    slow_query_db_config={'host': '127.0.0.1', 'port': 3306, 'user': 'test', 'password': 'test'}
)

# 获取备库主机名
standby = db_helper.get_standby_hostname('master_host')

# 获取安全连接
conn_result = db_helper.get_safe_connection(hostname='127.0.0.1', database='test')

# 执行安全查询
result = db_helper.execute_safe_query("SELECT * FROM users WHERE id=1", database='test')

# 检查表是否存在
exists = db_helper.check_table_exists('test', 'users')

# 获取表索引
indexes = db_helper.get_table_indexes_from_db('test', 'users')

# 关闭连接
db_helper.close_safe_connection()
```

## 在主文件中使用

重构后的主文件应该这样导入和使用：

```python
# database_optimization_report.py
from utils import load_db_config, setup_encoding
from data_masking import DataMasking
from sql_analyzer import SQLAnalyzer
from data_processor import DataProcessor
from database_helper import DatabaseHelper

class DatabaseOptimizationReport:
    def __init__(self, ...):
        setup_encoding()
        # 使用各个模块
        self.data_masking = DataMasking
        self.sql_analyzer = SQLAnalyzer
        self.data_processor = DataProcessor
        self.db_helper = DatabaseHelper(...)
        
    def _mask_sensitive_data(self, data):
        return DataMasking.mask_sensitive_data(data)
    
    def _extract_table_name(self, sql):
        return SQLAnalyzer.extract_table_name(sql)
    
    # ... 其他方法类似
```

## 注意事项

1. **向后兼容**: 所有模块都设计为静态方法或类方法，可以独立使用
2. **配置传递**: `DatabaseHelper` 需要传入数据库配置，其他模块不需要
3. **方法调用**: 原文件中的 `self._method()` 需要改为 `Module.method()` 或 `self.module.method()`
4. **测试**: 拆分后需要测试确保所有功能正常

## 下一步

1. 创建 `report_generator.py` 模块（报告生成相关，代码量很大）
2. 重构主文件 `database_optimization_report.py`，更新所有导入和方法调用
3. 测试确保功能正常

