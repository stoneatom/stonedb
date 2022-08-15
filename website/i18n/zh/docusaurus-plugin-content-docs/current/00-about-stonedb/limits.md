---
id: limits
sidebar_position: 1.3
---

# 使用限制
StoneDB 100% 兼容 MySQL 5.6、5.7 协议和 MySQL 生态等重要特性，支持 MySQL 常用的功能及语法，但由于 StoneDB 本身的一些特性，部分操作和功能尚未得到支持，以下列出的是不兼容 MySQL 的操作和功能。
## 不支持的 DDL

1. 修改字段的数据类型
2. 修改字段的长度
3. 修改表/字段的字符集
4. 转换表的字符集
5. optimize table
6. analyze table
7. lock table
8. repair table
9. CTAS
10. 重组表
11. 重命名字段
12. 设置字段的默认值
13. 设置字段为空
14. 设置字段非空
15. 添加唯一约束
16. 删除唯一约束
17. 创建索引
18. 删除索引
19. 表修改注释

表和列的相关属性不易被修改，在表设计阶段尽可能定义好字符集、数据类型、约束和索引等。
## 不支持的 DML
1. delete
2. update 关联子查询
3. update 多表关联
4. replace into

StoneDB 不适用于有频繁的更新操作，因为对列式存储来说，更新需要找到对应的每一列，然后分多次更新，而行式存储由于一行紧挨着一行，找到对应的 page 或者 block 就可直接在行上更新，因此 StoneDB 只支持了常规用的单表 update 和 insert。
## 不支持跨存储引擎关联查询
StoneDB 默认不支持跨存储引擎关联查询，也就是说其他存储引擎下的表和 StoneDB 下的表进行关联查询会报错。可在参数文件 my.cnf 里定义 tianmu_ini_allowmysqlquerypath=1，这样就支持跨存储引擎表之间的关联查询了。
## 不支持的对象
1. 全文索引
2. 唯一约束
3. 触发器
4. 临时表
5. 含有自定义函数的存储过程
6. 含有 SQL 的自定义函数
## 不支持的数据类型
1. 位类型 bit
2. 枚举型 enum
3. 集合型 set
4. json 类型
5. decimal 精度必须小于或等于18，否则不支持，如 decimal(19,x)
6. 创建表时不支持使用关键字 unsigned、zerofill
## 不支持事务
只有严格遵守 ACID 四大属性，才能真正的支持事务。而 StoneDB 由于没有 redo 和 undo，是不支持事务的。
## 不支持分区
列式存储不支持分区。
## 不支持行锁、表锁
列式存储不支持行锁、表锁。
## 只支持 statement 的 binlog 格式
列式存储不支持 row、mixed 格式的 binlog。


