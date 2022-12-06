---
id: release-notes
sidebar_position: 11.0
---

# 发版日志

## StoneDB_5.7_v1.0.1的发行日志 (2022-10-21, RC)
- 功能添加或改变
- 编译相关改动
- 文档变更
- BUG修复

### 功能添加或改变
- 关键项：Tianmu 引擎增加 delete 功能
  - 增加 delete all 单表功能；
  - 增加 delete all 多表功能；
  - 增加 delete where 单表功能；
  - 增加 delete where 多表功能；
- 关键项：Tianmu 引擎增加 alter table 功能
- 关键项：Tianmu 引擎 binlog 日志增加 ROW 格式
- 关键项：Tianmu 引擎增加临时表功能
- 关键项：Tianmu 引擎增加触发器功能
- 关键项：Tianmu 引擎增加 Create table AS...union... 功能
- 关键项：Tianmu 引擎提升了子查询的性能
- 关键项：Tianmu引擎增加 gtest 模块
- 关键项：添加一些 mtr 测试用例
### 编译相关改动
- cmake 增加 -DWITH_MARISA  -DWITH_ROCKSDB 参数
### 文档变更
- 用户手册、编译手册等相关文档发生了变更. ( # address)
### BUG修复
- 修复若干 MySQL 原生 MTR 用例
- 修复及完善 Tianmu 功能: 
>修复 issue 有： [#282](https://github.com/stoneatom/stonedb/issues/282),[#274](about:blank),[#270](https://github.com/stoneatom/stonedb/issues/270),[#663](https://github.com/stoneatom/stonedb/issues/663),[#669](https://github.com/stoneatom/stonedb/issues/669),[#670](https://github.com/stoneatom/stonedb/issues/670),[#675](https://github.com/stoneatom/stonedb/issues/675),[#678](https://github.com/stoneatom/stonedb/issues/678),[#682](https://github.com/stoneatom/stonedb/issues/682),[#487](https://github.com/stoneatom/stonedb/issues/487),[#426](https://github.com/stoneatom/stonedb/issues/426),[#250](https://github.com/stoneatom/stonedb/issues/250),[#247](https://github.com/stoneatom/stonedb/issues/247),[#569](https://github.com/stoneatom/stonedb/issues/569),[#566](https://github.com/stoneatom/stonedb/issues/566),[#290](https://github.com/stoneatom/stonedb/issues/290),[#736](https://github.com/stoneatom/stonedb/issues/736),[#567](https://github.com/stoneatom/stonedb/issues/567),[#500](https://github.com/stoneatom/stonedb/issues/500),[#300](https://github.com/stoneatom/stonedb/issues/300),[#289](https://github.com/stoneatom/stonedb/issues/289),[#566](https://github.com/stoneatom/stonedb/issues/566),[#279](https://github.com/stoneatom/stonedb/issues/279),[#570](https://github.com/stoneatom/stonedb/issues/570)[,#571](https://github.com/stoneatom/stonedb/issues/571),[#580](https://github.com/stoneatom/stonedb/issues/580),[#581](https://github.com/stoneatom/stonedb/issues/581),[#586](https://github.com/stoneatom/stonedb/issues/586),[#589](https://github.com/stoneatom/stonedb/issues/589),[#674](https://github.com/stoneatom/stonedb/issues/674),[#646](https://github.com/stoneatom/stonedb/issues/646),[#280](https://github.com/stoneatom/stonedb/issues/280),[#301](https://github.com/stoneatom/stonedb/issues/301),[#733](https://github.com/stoneatom/stonedb/issues/733) 等。

## StoneDB_5.7_v1.0.0的发行日志 (2022-08-31, 发行版)
- 支持MySQL 5.7
- 功能添加或改变
- 编译相关改动
- 配置相关改动
- 文档变更
- BUG修复

## 支持MySQL 5.7

- **关键项：** StoneDB 数据库支持MySQL 5.7 协议，基线版本：MySQL 5.7.36
## 功能添加或改变

- **关键项：** StoneDB 数据库的列式存储引擎从 StoneDB 变更为 Tianmu
```bash
mysql> show engines;
+----------------+---------+--------------------------+--------------+------+------------+
| Engine         | Support | Comment                  | Transactions | XA   | Savepoints |
+----------------+---------+--------------------------+--------------+------+------------+
| TIANMU         | DEFAULT | Tianmu storage engine    | YES          | NO   | NO         |
+----------------+---------+--------------------------+--------------+------+------------+
```

- **Tianmu:** 提升了 Tianmu 引擎对 decimal 数据类型的聚合能力；
- **Tianmu: **提高了代码的可读性。有些代码在逻辑上没有分开或者变量名称不能体现真实含义。例如：修改 int DoGetSomething(); to int GetSomethingXXX();, int GetNoNulls() to int GetNumOfNulls()。
- **Tianmu: **优化了视图对日期函数的调用能力，日期函数能够被视图正常调用使用 (例如：DATE_ADD, DATE_SUB等函数) ；([BUG #342](https://github.com/stoneatom/stonedb/issues/342))
## 编译相关改动

- Boost 依赖库的版本变更为 1.66.0；
- Rocksdb 满足 StoneDB 数据库构建的版本变更为 6.12.6；
## 配置相关改动

- **关键项：**StoneDB 数据库默认配置文件从 stonedb.cnf 变更为 my.cnf；([feature #182](https://github.com/stoneatom/stonedb/issues/182))
- **关键项：**StoneDB 数据库的默认存储引擎从 Innodb 变更为 Tianmu。([feature #255](https://github.com/stoneatom/stonedb/issues/255))
## 文档变更

- 用户手册、编译手册等相关文档发生了变更. ( [# address](https://stonedb.io/))
## BUG修复

- **修复 mtr 用例: **[BUG #78](https://github.com/stoneatom/stonedb/issues/78), [BUG #73](https://github.com/stoneatom/stonedb/issues/73),[ BUG #170](https://github.com/stoneatom/stonedb/issues/170), [BUG #192](https://github.com/stoneatom/stonedb/issues/192), [BUG #191](https://github.com/stoneatom/stonedb/issues/191), [BUG #227](https://github.com/stoneatom/stonedb/issues/227),  [BUG #245](https://github.com/stoneatom/stonedb/issues/245), [BUG  #263](https://github.com/stoneatom/stonedb/issues/263)
- **修复 Tianmu 缺陷: **[BUG #338](https://github.com/stoneatom/stonedb/issues/388),[ BUG #327](https://github.com/stoneatom/stonedb/issues/327), [BUG #212](https://github.com/stoneatom/stonedb/issues/212), [BUG #142](https://github.com/stoneatom/stonedb/issues/142)

## StoneDB-5.6-V1.0.0

发布时间: 2022 年 6 月 30 号,

