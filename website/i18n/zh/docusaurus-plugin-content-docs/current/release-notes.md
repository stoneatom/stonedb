---
id: release-notes
sidebar_position: 11.0
---

# 发版日志
## StoneDB-5.7-v1.0.3 的发行日志 (2023-03-20, GA)
发布日期： 2023 年 03 月 20 日
### 主备能力

- Binlog 改造 — 指定 DDL 过滤。
- SQL MODE 中增加参数 NO_KEY_ERROR，SQL 层对不支持 DDL 报错忽略。
   - 语法举例：
```sql
# 全局级别
mysql>set global sql_mode='NO_KEY_ERROR';

# 会话级别
mysql>set session sql_mode='NO_KEY_ERROR';

# my.cnf 配置文件
[mysqld] 
sql_mode='NO_KEY_ERROR'
```

### 生态适配

- StoneDB 版本号展示。
### 性能

- 主从同步性能提升 [#1213](https://github.com/stoneatom/stonedb/issues/1213)。
### 问题修复
修复了如下 Bug：

- 执行 ALTER TABLE 语句添加 TIMESTAMP 字段返回结果不正确。[#1327](https://github.com/stoneatom/stonedb/issues/1327)
- 执行 ALTER TABLE 语句后 Update 表数据不正确。[#1253](https://github.com/stoneatom/stonedb/issues/1253) 
- Bigint unsigned 返回结果不正确。 [#1203](https://github.com/stoneatom/stonedb/issues/1203) 
- Load data 报错。[#1209](https://github.com/stoneatom/stonedb/issues/1209)
- AVG 函数返回结果不正确。[#1125](https://github.com/stoneatom/stonedb/issues/1125)
- ALTER TABLE 更改字段数据类型报错。[#752](https://github.com/stoneatom/stonedb/issues/752)
- 其它 Bug。[#103](https://github.com/stoneatom/stonedb/issues/103) [#1230](https://github.com/stoneatom/stonedb/issues/1230) [#1255](https://github.com/stoneatom/stonedb/issues/1255)[#1188](https://github.com/stoneatom/stonedb/issues/1188) [#1262](https://github.com/stoneatom/stonedb/issues/1262)

### 支持平台

- CentOS 7.6 以上
- Ubuntu 20

更多该版本的详细更新记录，请访问 [Github](https://github.com/stoneatom/stonedb/releases) 和 [Gitee](https://gitee.com/StoneDB/stonedb/releases) 查看。

## StoneDB-5.7-v1.0.2 的发行日志 (2023-01-15, GA)
发布日期： 2023 年 01 月 15 日
### 功能开发

- 支持自定义函数。
- 支持转义功能。
- 支持主键，语法上支持索引。
- 支持修改表/字段的字符集。
- 支持BIT数据类型
   - 建表时允许指定字段类型为 BIT，也允许修改表字段类型为 BIT(需要满足类型转换条件)。
   - BIT 数据类型逻辑运算
- 支持replace into 功能。
- 支持(语法上)支持unsigned 和zerofill。
- SQL MODE 中增加参数 MANDATORY TIANMU，用于指定表的默认存储引擎为 TIANMU。
   - 语法举例：
```sql
# 全局级别
mysql>set global sql_mode='STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,MANDATORY_TIANMU';

# 会话级别
mysql>set session sql_mode='STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,MANDATORY_TIANMU';

# my.cnf 配置文件
[mysqld] 
sql_mode        =  'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,MANDATORY_TIANMU'
```

### 易用性

- 安装包自动检测识别能力。
- 快速部署StoneDB为MySQL的从库能力。

### 稳定性

- 做为从库的稳定性增强。

### 问题修复
修复了如下 Bug：

- GROUP_CONCAT() 函数返回错误。[#938](https://github.com/stoneatom/stonedb/issues/938)
- 模糊匹配LIKE 查询问题。[#1162](https://github.com/stoneatom/stonedb/issues/1162) [#1157](https://github.com/stoneatom/stonedb/issues/1157) [#763](https://github.com/stoneatom/stonedb/issues/763)
- 主键AUTO_INCREMENT问题。 [#1144](https://github.com/stoneatom/stonedb/pull/1144) [#1142](https://github.com/stoneatom/stonedb/issues/1142)
- 增加NUMERIC 类型字段返回错误。[#1140](https://github.com/stoneatom/stonedb/issues/1140)
- CI/CD 执行clang-format失败。[#973](https://github.com/stoneatom/stonedb/issues/973)
- INSERT INTO 兼容性。[#965](https://github.com/stoneatom/stonedb/issues/965)
- UNION ALL 返回错误。[#854](https://github.com/stoneatom/stonedb/issues/854)
- EXTRACT() 函数返回错误。[#845](https://github.com/stoneatom/stonedb/issues/845)
- DATE类型select 显示错误。[#829](https://github.com/stoneatom/stonedb/issues/829)
- UPDATE 更改多列不生效。[#781](https://github.com/stoneatom/stonedb/issues/781)
- 子查询场景。[#732](https://github.com/stoneatom/stonedb/issues/732)
- MTR binlog.binlog_unsafe Crash。[#341](https://github.com/stoneatom/stonedb/issues/341)
- 其它BUG。[#682](https://github.com/stoneatom/stonedb/issues/682) [#553](https://github.com/stoneatom/stonedb/issues/553) [#508](https://github.com/stoneatom/stonedb/issues/508)

### 行为变更
使用快速部署StoneDB为MySQL的从库Shell脚本，默认 sql_mode 开启强制 Tianmu 引擎参数：MANDATORY_TIANMU。

### 支持平台

- CentOS 7.6以上。
- Ubuntu 20。

### 其他

- 添加一些MTR测试用例。


## StoneDB-5.7-v1.0.1 的发行日志 (2022-10-21, RC;2022-10-24,GA)
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

## StoneDB-5.7-v1.0.0 的发行日志 (2022-08-31, 发行版)
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

## StoneDB-5.6-v1.0.0

发布时间: 2022 年 6 月 30 号

全面兼容 MySQL5.6 版本。

