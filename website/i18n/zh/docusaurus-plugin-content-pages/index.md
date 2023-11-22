
```custom-download
StoneDB for MySQL
一个基于MySQL的开源实时HTAP数据库
```

```custom-feature
- 版本特性
    - Features_sqlFilled
        - 100%兼容MySQL 5.7
        - 基于MySQL5.7版本开发，完全兼容所有MySQL语法与接口，支持从MySQL直接更新升级
        1. HTAP for MySQL
        2. 完整的支持MySQL语法及功能
        3. 丰富的MySQL生态圈工具及解决方案等
        4. 业务和数据平滑迁移
        - 核心价值
            - Sql_1Filled
                - 节省开发数据库应用的成本
            - Sql_2Filled
                - 企业不会被数据库厂商锁定
    - Features_htapFilled
        - 事务+分析一体化
        - 事务性/分析性负载一体化解决方案
        1. 无复杂耗时的ETL
        2. 事务性负载与分析性负载有效隔离
        3. 事务性数据“实时”同步到分析引擎
        4. 满足事务ACID特性
        - 核心价值
            - Htap_1Filled
                - 实时数据分析
                - 可直接针对最新数据进行分析，提供最实时的决策支持
            - Htap_2Filled
                - 简化架构
                - 大幅简化实时数据服务的架构复杂度，减少运维成本
            - Htap_3Filled
                - 业务敏捷性
                - 让实时分析业务能很快成型落地，加速数字化转型和业务推进速度
    - Features_openFilled
        - 完全开源
        - 广泛的社区用户&开发者支持
        1. 核心代码，相关工具等完全开源
        2. 丰富的社区人才，产品快速演进
        3. 社区聚力，问题急速响应
        - 核心价值
            - Open_1Filled
                - 开源，国产，核心自主可控
            - Open_2Filled
                - 生态兼容，使用成本低
                - 相关周边解决方案和工具，开发者众多且成熟
    - Features_searchFilled
        - 10倍查询速度
        - 亿级多表关联分析毫秒级响应
        1. 支持向量化执行
        2. 支持知识网格，粗糙集过滤
        3. 急速的数据导入
        4. 支持智能化行列混存急速查索
        - 核心价值
            - Search_1Filled
                - 海量实时数据分析，提供最实时的决策支持，发掘数据中更多的价值
    - Features_inputFilled
        - 10倍初始导入速度
        - 所有数据以列存格式存储内存中
        1. 基于行存数据做水平分区
        2. 分区内部按照schema定义组织成列式存
        3. 智能数据散列，多磁盘并行IO
        4. 多CPU并行，线程池+无锁任务队列
        - 核心价值
            - Input_1Filled
                - 提升分析模型的所依赖数据的完整性和新鲜度并减少分析结果的等待时间
    - Features_storageFilled
        - 1/10的成本
        - 高效压缩算法，节省大量存储空间，最高可实
        1. 对全部数据进行压缩， 可实现40倍压缩比
        2. 支持对业务领域进行技术&架构增强
        3. 无缝对业务方/中间件产品集成
        4. 迁移成本可控
        - 核心价值
            - Sql_1Filled
                - 减少企业的TCO成本，人人皆可获得低成本的数据服务。
```

```custom-roadMap 2023-06-30
- Roadmap
    - StoneDB_5.6_v1.0.0
        1. 一体化行列混存+内存计算架构
        2. 基于代价的智能HTAP查询引擎
        3. 智能压缩技术
        - 2022-06-29
    - StoneDB_5.7_v1.0.0
        - 适配 MySQL 5.7
        - 2022-08-31
    - StoneDB_5.7_v1.0.1
        1. 提升TPC-H中8个慢SQL
        2. 优化查询模块
        3. 增加 delete 功能
        4. 增加 binlog 复制，支持 row 格式
        - 2022-10-24
    - StoneDB_5.7_v1.0.2
        1. 支持自定义函数。
        2. 支持转义功能。
        3. 支持主键和支持(语法上)索引约束。
        4. 支持修改表/字段的字符集。
        5. 支持BIT数据类型：
            - BIT 数据类型创建、更改、删除；
            - BIT 数据类型逻辑运算
        6. 支持 replace into 功能。
        7. 支持(语法上)支持unsigned 和zerofill。
        8. sql_mode增加强制Tianmu引擎参数：MANDATORY_TIANMU。
        - 2023-01-15
    - StoneDB_5.7_v1.0.3
        1. 主备集群能力增强
        2. 部分数据对象的定义
        3. Binlog 改造
        4. 主备集群自动寻主功能
        - 2023-03-20
    - StoneDB_8.0_v1.0.0
        - 适配 MySQL 8.0
        - 2023-05-04
    - StoneDB_5.7_v1.0.4
        1. 存储过程增强
        2. MySQL Event（批量加工能力）
        3. 窗口函数
        4. 安全功能-身份鉴别增强、存储加密
        - 2023-06-30
    - StoneDB_5.7_v1.0.5
        1. 性能增强-并行执行能力（Parallel）
        2. 性能增强-向量计算
        3. 多主一从架构
        4. 数据交换（多种导出格式）
        5. 安全功能-传输加密、数据脱敏
        - 2023-12-30
    - StoneDB_v2.0.0
        1. StoneDB 2.0 提供实时的在线事务支持和数据分析能力。在支持TP事务的同时，支持内置的、对用户透明的AP引擎，百亿级数据join场景下的高性能，相比较MySQL提供100至1000倍的加速。
        2. 支持create/alter table/drop table 等DDL语句。
        3. 支持insert/update/load data等命令对数据做实时更新。
        - 2023-09-25
    - StoneDB_v2.1.0
        1. 支持事务。
        2. 支持主从复制。
        3. 支持常用的内置函数。
        4. 支持存储过程、临时表、视图、触发器等对象。
        - 2023-10-30    
```

```custom-showCase
- 信创国产化
    - kunpeng
        - 鲲鹏技术认证
    - tx
        - 统信软件认证
    - zkfd
        - 中科方德认证
    - hgxx
        - 海光信息认证
    - zkkk
        - 中科可控认证
    - ghzz
        - 光合组织成员单位
    - Opengauss
        - OpenGauss共建社区    

```

```custom-customers
- 我们的用户
    - aliyun
    - xbb
```

```custom-concat
```