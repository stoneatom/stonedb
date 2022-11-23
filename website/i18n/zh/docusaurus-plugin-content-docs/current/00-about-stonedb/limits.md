---
id: limits
sidebar_position: 1.3
---

# 使用限制
StoneDB 100% 兼容 MySQL 5.6、5.7 协议和 MySQL 生态等重要特性，支持 MySQL 常用的功能及语法，但由于 StoneDB 本身的一些特性，部分操作和功能尚未得到支持，以下列出的是不兼容 MySQL 的操作和功能。
## 不支持的 DDL
1. 修改表的字符集
2. optimize table
3. analyze table
4. repair table
5. lock table
6. CTAS
7. 添加唯一约束
8. 删除唯一约束
9. 添加主键约束
10. 删除主键约束
11. 创建索引
12. 删除索引
13. 修改表注释

:::info
表和列的相关属性不易被修改，在表设计阶段尽可能定义好字符集、数据类型、约束和索引等。
:::

## 不支持的 DML
1. update 关联子查询
2. update 多表关联
3. replace into


:::info
如果 replace into 没有主键或者唯一约束冲突，语义逻辑是插入一行，这是支持的；如果 replace into 有主键或者唯一约束冲突，语义逻辑是更新一行，这是不支持的。<br />StoneDB 不适用于有频繁的更新操作，因为对列式存储来说，更新需要找到对应的每一列，然后分多次更新，而行式存储由于一行紧挨着一行，找到对应的 page 或者 block 就可直接在行上更新，因此 StoneDB 只支持了常规用的单表 update 和 insert。
:::

## 不支持跨存储引擎关联查询
StoneDB 默认不支持跨存储引擎关联查询，也就是说其他存储引擎下的表和 StoneDB 下的表进行关联查询会报错。可在参数文件 my.cnf 里定义 tianmu_ini_allowmysqlquerypath=1，这样就支持跨存储引擎表之间的关联查询了。
## 不支持的对象
1. 全文索引
2. 唯一约束
3. 含有自定义函数的存储过程
4. 含有 SQL 的自定义函数

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


