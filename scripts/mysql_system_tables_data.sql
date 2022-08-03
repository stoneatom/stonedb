-- Copyright (c) 2007, 2021, Oracle and/or its affiliates.
-- 
-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License, version 2.0,
-- as published by the Free Software Foundation.
--
-- This program is also distributed with certain software (including
-- but not limited to OpenSSL) that is licensed under separate terms,
-- as designated in a particular file or component or in included license
-- documentation.  The authors of MySQL hereby grant you an additional
-- permission to link the program and your derivative works with the
-- separately licensed software that they have included with MySQL.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License, version 2.0, for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

--
-- The inital data for system tables of MySQL Server
--


-- added for tianmu
CREATE TEMPORARY TABLE tmp_decomp LIKE sys_tianmu.decomposition_dictionary;
INSERT INTO tmp_decomp VALUES ('IPv4','PREDEFINED','');
INSERT INTO tmp_decomp VALUES ('IPv4_C','%d.%d.%d.%d','');
INSERT INTO tmp_decomp VALUES ('EMAIL','%s@%s','');
INSERT INTO tmp_decomp VALUES ('URL','%s://%s?%s','');
INSERT INTO sys_tianmu.decomposition_dictionary SELECT * FROM tmp_decomp WHERE @had_decomp_dict_table=0;
DROP TABLE tmp_decomp;

DROP trigger IF EXISTS sys_tianmu.before_insert_on_columns;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_tianmu.before_insert_on_columns BEFORE INSERT ON sys_tianmu.columns FOR EACH ROW BEGIN select count(*) from sys_tianmu.columns where database_name = NEW.database_name and table_name = NEW.table_name and column_name = NEW.column_name into @count; IF @count <> 0 THEN SET @@tianmu_trigger_error = concat ('Insert failed: The decomposition for column ', NEW.database_name, '.', NEW.table_name, '.', NEW.column_name, ' already set.'); END IF; select count(*) from sys_tianmu.decomposition_dictionary where ID = NEW.decomposition into @count; IF @count = 0 THEN SET @@tianmu_trigger_error = concat ('Insert failed: The decomposition \'', NEW.decomposition, '\' does not exist.'); END IF; END ;

DROP trigger IF EXISTS sys_tianmu.before_update_on_columns;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_tianmu.before_update_on_columns BEFORE UPDATE ON sys_tianmu.columns FOR EACH ROW BEGIN SELECT COUNT(*) FROM sys_tianmu.decomposition_dictionary WHERE ID = NEW.decomposition INTO @count; IF @count = 0 THEN SET @@tianmu_trigger_error = concat ('Update failed: The decomposition \'', NEW.decomposition, '\' does NOT exist.'); END IF; END ;

DROP trigger IF EXISTS sys_tianmu.before_insert_on_decomposition_dictionary;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_tianmu.before_insert_on_decomposition_dictionary BEFORE INSERT ON sys_tianmu.decomposition_dictionary FOR EACH ROW BEGIN SELECT COUNT(*) FROM sys_tianmu.decomposition_dictionary WHERE ID = NEW.ID INTO @count; IF @count <> 0 THEN SET @@tianmu_trigger_error = concat ('Insert failed: The decomposition \'', NEW.ID, '\' already exists.'); END IF; select is_decomposition_rule_correct( NEW.RULE ) into @valid; IF @valid = 0 THEN SET @@tianmu_trigger_error = concat ('Insert failed: The decomposition rule \'', NEW.RULE, '\' is invalid.'); END IF; END ;

DROP trigger IF EXISTS sys_tianmu.before_update_on_decomposition_dictionary;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_tianmu.before_update_on_decomposition_dictionary BEFORE UPDATE ON decomposition_dictionary FOR EACH ROW BEGIN IF OLD.RULE <> NEW.RULE then select is_decomposition_rule_correct( NEW.RULE ) into @valid; IF @valid = 0 THEN SET @@tianmu_trigger_error = concat ('Update failed: The decomposition rule \'', NEW.RULE, '\' is invalid.'); END IF; END IF; IF OLD.ID <> NEW.ID then SET @@tianmu_trigger_error = concat ('Update failed: The column ID is not allowed to be changed in DECOMPOSITION_DICTIONARY.'); END IF; END ;

DROP trigger IF EXISTS sys_tianmu.before_delete_on_decomposition_dictionary;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_tianmu.before_delete_on_decomposition_dictionary BEFORE DELETE ON sys_tianmu.decomposition_dictionary FOR EACH ROW BEGIN SELECT COUNT(*) FROM sys_tianmu.columns WHERE decomposition = OLD.ID INTO @count; IF @count <> 0 THEN SET @@tianmu_trigger_error = concat ('Delete failed: The decomposition \'', OLD.ID, '\' IS used IN at least one column.'); END IF; END ;

DROP trigger IF EXISTS sys_tianmu.after_insert_on_columns;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_tianmu.after_insert_on_columns AFTER INSERT ON sys_tianmu.columns FOR EACH ROW BEGIN SET @@global.tianmu_refresh_sys_tianmu = TRUE; END ;

DROP trigger IF EXISTS sys_tianmu.after_update_on_columns;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_tianmu.after_update_on_columns AFTER UPDATE ON sys_tianmu.columns FOR EACH ROW BEGIN SET @@global.tianmu_refresh_sys_tianmu = TRUE; END ;

DROP trigger IF EXISTS sys_tianmu.after_delete_on_columns;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_tianmu.after_delete_on_columns AFTER DELETE ON sys_tianmu.columns FOR EACH ROW BEGIN SET @@global.tianmu_refresh_sys_tianmu = TRUE; END ;

DROP trigger IF EXISTS sys_tianmu.after_insert_on_decomposition_dictionary;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_tianmu.after_insert_on_decomposition_dictionary AFTER INSERT ON sys_tianmu.decomposition_dictionary FOR EACH ROW BEGIN SET @@global.tianmu_refresh_sys_tianmu = TRUE; END ;

DROP trigger IF EXISTS sys_tianmu.after_update_on_decomposition_dictionary;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_tianmu.after_update_on_decomposition_dictionary AFTER UPDATE ON sys_tianmu.decomposition_dictionary FOR EACH ROW BEGIN SET @@global.tianmu_refresh_sys_tianmu = TRUE; END ;

DROP trigger IF EXISTS sys_tianmu.after_delete_on_decomposition_dictionary;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_tianmu.after_delete_on_decomposition_dictionary AFTER DELETE ON sys_tianmu.decomposition_dictionary FOR EACH ROW BEGIN  SET @@global.tianmu_refresh_sys_tianmu = TRUE; END ;

DROP PROCEDURE IF EXISTS `sys_tianmu`.`CREATE_RULE`;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_tianmu`.`CREATE_RULE` ( IN id_ VARCHAR(4000), IN expression_ VARCHAR(4000), IN comment_ VARCHAR(4000) ) BEGIN INSERT INTO sys_tianmu.decomposition_dictionary ( ID, RULE, COMMENT ) VALUES( id_, expression_, comment_ ); END;

DROP PROCEDURE IF EXISTS `sys_tianmu`.`UPDATE_RULE`;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_tianmu`.`UPDATE_RULE` ( IN id_ VARCHAR(4000), IN expression_ VARCHAR(4000) ) BEGIN UPDATE sys_tianmu.decomposition_dictionary SET RULE = expression_ WHERE ID = id_; END;

DROP PROCEDURE IF EXISTS `sys_tianmu`.`DELETE_RULE`;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_tianmu`.`DELETE_RULE` ( IN id_ VARCHAR(4000) ) BEGIN DELETE FROM sys_tianmu.decomposition_dictionary WHERE ID = id_; END;

DROP PROCEDURE IF EXISTS `sys_tianmu`.`CHANGE_RULE_COMMENT`;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_tianmu`.`CHANGE_RULE_COMMENT` ( IN id_ VARCHAR(4000), IN comment_ VARCHAR(4000) ) BEGIN UPDATE sys_tianmu.decomposition_dictionary SET COMMENT = comment_ WHERE ID = id_; END;

DROP PROCEDURE IF EXISTS `sys_tianmu`.`SET_DECOMPOSITION_RULE`;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_tianmu`.`SET_DECOMPOSITION_RULE` ( IN database_ VARCHAR(4000), IN table_ VARCHAR(4000), IN column_ VARCHAR(4000), IN id_ VARCHAR(4000) ) BEGIN DELETE FROM sys_tianmu.columns WHERE database_name = database_ AND table_name = table_ AND column_name = column_; INSERT INTO sys_tianmu.columns ( database_name, table_name, column_name, decomposition ) VALUES( database_, table_, column_, id_ ); END;

DROP PROCEDURE IF EXISTS `sys_tianmu`.`DELETE_DECOMPOSITION_RULE`;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_tianmu`.`DELETE_DECOMPOSITION_RULE` ( IN database_ VARCHAR(4000), IN table_ VARCHAR(4000), IN column_ VARCHAR(4000) ) BEGIN DELETE FROM sys_tianmu.columns WHERE database_name = database_ AND table_name = table_ AND column_name = column_; END;

DROP PROCEDURE IF EXISTS `sys_tianmu`.`SHOW_DECOMPOSITION`;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_tianmu`.`SHOW_DECOMPOSITION` ( IN database_ VARCHAR(4000), IN table_ VARCHAR(4000) ) BEGIN SELECT s.ordinal_position AS '#', s.column_name AS 'column name', s.data_type AS 'data type', MIN( c.decomposition ) AS decomposition, MIN( d.rule ) AS rule FROM information_schema.columns AS s LEFT JOIN sys_tianmu.columns c ON s.table_schema = c.database_name AND s.table_name = c.table_name AND s.column_name = c.column_name LEFT JOIN sys_tianmu.decomposition_dictionary d ON c.decomposition = d.id WHERE s.table_schema = database_ AND s.table_name = table_ GROUP BY s.ordinal_position, s.column_name, s.data_type ORDER BY s.ordinal_position; END;

