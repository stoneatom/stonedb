---
id: parameter-tuning
sidebar_position: 7.43
---

# 数据库参数优化

## stonedb_insert_buffer_size
- 注释：insert buffer的大小，单位M
- 默认值：512
- 取值范围：512 ~ 10000
- 建议值：若有大批量插入数据任务，建议设置为2048
## stonedb_ini_servermainheapsize

- 注释：Server的heap大小，单位MB
- 默认值：0，默认为0表示物理内存的1/2
- 取值范围：0 ~ 1000000
- 建议值：物理内存的1/2
## stonedb_distinct_cache_size

- 注释：去重缓冲区大小，单位MB
- 默认值：64
- 取值范围：64 ~ 256
- 建议值：128
## stonedb_bg_load_threads

- 注释：将insert buffer中的数据加载到后台线程池的工作线程数
- 默认值：0
- 取值范围：0 ~ 100
- 建议值：系统CPU核数的1/2
## stonedb_load_threads

- 注释：StoneDB Load线程池的工作线程数
- 默认值：0
- 取值范围：0 ~ 100
- 建议值：系统CPU核数
## stonedb_query_threads

- 注释：StoneDB查询线程池的工作线程数
- 默认值：0
- 取值范围：0 ~ 100
- 建议值：系统CPU核数
