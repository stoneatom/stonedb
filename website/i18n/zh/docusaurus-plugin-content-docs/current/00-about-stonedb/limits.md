---
id: limits
sidebar_position: 1.3
---

# 使用限制
StoneDB 100% 兼容 MySQL 5.6、5.7 协议和 MySQL 生态等重要特性，支持 MySQL 常用的功能及语法，但由于 StoneDB 本身的一些特性，部分操作和功能尚未得到支持，以下列出的是不兼容 MySQL 的操作和功能。

# 不支持的DDL
1. optimize table
2. analyze table
3. repair table
4. lock table
5. 创建外键
6. 删除外键

# 不支持跨存储引擎关联查询
StoneDB 默认不支持跨存储引擎关联查询，也就是说其他存储引擎下的表和 StoneDB 下的表进行关联查询会报错。可在参数文件 my.cnf 里定义 tianmu_ini_allowmysqlquerypath=1，这样就支持跨存储引擎表之间的关联查询了。

# 不支持的对象
1. 全文索引
2. 唯一约束
3. 二级索引
4. 外键

# 不支持的数据类型
1. 枚举型 enum
2. 集合型 set
3. json 类型
4. decimal 精度必须小于或等于18，否则不支持，如 decimal(19,x)

# 不支持事务
只有严格遵守 ACID 四大属性，才能真正的支持事务。而 StoneDB 由于没有 redo 和 undo，是不支持事务的。

# 不支持分区
列式存储不支持分区。

# 不支持行锁、表锁
列式存储不支持行锁、表锁。
