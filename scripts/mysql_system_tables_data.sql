-- Copyright (c) 2007, 2014, Oracle and/or its affiliates. All rights reserved.
-- 
-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; version 2 of the License.
-- 
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
-- 
-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

--
-- The inital data for system tables of MySQL Server
--

-- When setting up a "cross bootstrap" database (e.g., creating data on a Unix
-- host which will later be included in a Windows zip file), any lines
-- containing "@current_hostname" are filtered out by mysql_install_db.

-- Get the hostname, if the hostname has any wildcard character like "_" or "%" 
-- add escape character in front of wildcard character to convert "_" or "%" to
-- a plain character
SELECT LOWER( REPLACE((SELECT REPLACE(@@hostname,'_','\_')),'%','\%') )INTO @current_hostname;


-- Fill "db" table with default grants for anyone to
-- access database 'test' and 'test_%' if "db" table didn't exist
CREATE TEMPORARY TABLE tmp_db LIKE db;
INSERT INTO tmp_db VALUES ('%','test','','Y','Y','Y','Y','Y','Y','N','Y','Y','Y','Y','Y','Y','Y','Y','N','N','Y','Y');
INSERT INTO tmp_db VALUES ('%','test\_%','','Y','Y','Y','Y','Y','Y','N','Y','Y','Y','Y','Y','Y','Y','Y','N','N','Y','Y');
INSERT INTO db SELECT * FROM tmp_db WHERE @had_db_table=0;
DROP TABLE tmp_db;


-- Fill "user" table with default users allowing root access
-- from local machine if "user" table didn't exist before
CREATE TEMPORARY TABLE tmp_user LIKE user;
INSERT INTO tmp_user VALUES ('localhost','root','','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','','','','',0,0,0,0,'mysql_native_password','','N');
REPLACE INTO tmp_user SELECT @current_hostname,'root','','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','','','','',0,0,0,0,'mysql_native_password','','N' FROM dual WHERE @current_hostname != 'localhost';
REPLACE INTO tmp_user VALUES ('127.0.0.1','root','','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','','','','',0,0,0,0,'mysql_native_password','','N');
REPLACE INTO tmp_user VALUES ('::1','root','','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','','','','',0,0,0,0,'mysql_native_password','','N');
INSERT INTO tmp_user (host,user) VALUES ('localhost','');
INSERT INTO tmp_user (host,user) SELECT @current_hostname,'' FROM dual WHERE @current_hostname != 'localhost';
INSERT INTO user SELECT * FROM tmp_user WHERE @had_user_table=0;
DROP TABLE tmp_user;

CREATE TEMPORARY TABLE tmp_proxies_priv LIKE proxies_priv;
INSERT INTO tmp_proxies_priv VALUES ('localhost', 'root', '', '', TRUE, '', now());
REPLACE INTO tmp_proxies_priv SELECT @current_hostname, 'root', '', '', TRUE, '', now() FROM DUAL WHERE LOWER (@current_hostname) != 'localhost';
INSERT INTO  proxies_priv SELECT * FROM tmp_proxies_priv WHERE @had_proxies_priv_table=0;
DROP TABLE tmp_proxies_priv;

-- added for stonedb
CREATE TEMPORARY TABLE tmp_decomp LIKE sys_stonedb.decomposition_dictionary;
INSERT INTO tmp_decomp VALUES ('IPv4','PREDEFINED','');
INSERT INTO tmp_decomp VALUES ('IPv4_C','%d.%d.%d.%d','');
INSERT INTO tmp_decomp VALUES ('EMAIL','%s@%s','');
INSERT INTO tmp_decomp VALUES ('URL','%s://%s?%s','');
INSERT INTO sys_stonedb.decomposition_dictionary SELECT * FROM tmp_decomp WHERE @had_decomp_dict_table=0;
DROP TABLE tmp_decomp;

DROP trigger IF EXISTS sys_stonedb.before_insert_on_columns;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_stonedb.before_insert_on_columns BEFORE INSERT ON sys_stonedb.columns FOR EACH ROW BEGIN select count(*) from sys_stonedb.columns where database_name = NEW.database_name and table_name = NEW.table_name and column_name = NEW.column_name into @count; IF @count <> 0 THEN SET @@stonedb_trigger_error = concat ('Insert failed: The decomposition for column ', NEW.database_name, '.', NEW.table_name, '.', NEW.column_name, ' already set.'); END IF; select count(*) from sys_stonedb.decomposition_dictionary where ID = NEW.decomposition into @count; IF @count = 0 THEN SET @@stonedb_trigger_error = concat ('Insert failed: The decomposition \'', NEW.decomposition, '\' does not exist.'); END IF; END ;

DROP trigger IF EXISTS sys_stonedb.before_update_on_columns;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_stonedb.before_update_on_columns BEFORE UPDATE ON sys_stonedb.columns FOR EACH ROW BEGIN SELECT COUNT(*) FROM sys_stonedb.decomposition_dictionary WHERE ID = NEW.decomposition INTO @count; IF @count = 0 THEN SET @@stonedb_trigger_error = concat ('Update failed: The decomposition \'', NEW.decomposition, '\' does NOT exist.'); END IF; END ;

DROP trigger IF EXISTS sys_stonedb.before_insert_on_decomposition_dictionary;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_stonedb.before_insert_on_decomposition_dictionary BEFORE INSERT ON sys_stonedb.decomposition_dictionary FOR EACH ROW BEGIN SELECT COUNT(*) FROM sys_stonedb.decomposition_dictionary WHERE ID = NEW.ID INTO @count; IF @count <> 0 THEN SET @@stonedb_trigger_error = concat ('Insert failed: The decomposition \'', NEW.ID, '\' already exists.'); END IF; select is_decomposition_rule_correct( NEW.RULE ) into @valid; IF @valid = 0 THEN SET @@stonedb_trigger_error = concat ('Insert failed: The decomposition rule \'', NEW.RULE, '\' is invalid.'); END IF; END ;

DROP trigger IF EXISTS sys_stonedb.before_update_on_decomposition_dictionary;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_stonedb.before_update_on_decomposition_dictionary BEFORE UPDATE ON decomposition_dictionary FOR EACH ROW BEGIN IF OLD.RULE <> NEW.RULE then select is_decomposition_rule_correct( NEW.RULE ) into @valid; IF @valid = 0 THEN SET @@stonedb_trigger_error = concat ('Update failed: The decomposition rule \'', NEW.RULE, '\' is invalid.'); END IF; END IF; IF OLD.ID <> NEW.ID then SET @@stonedb_trigger_error = concat ('Update failed: The column ID is not allowed to be changed in DECOMPOSITION_DICTIONARY.'); END IF; END ;

DROP trigger IF EXISTS sys_stonedb.before_delete_on_decomposition_dictionary;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_stonedb.before_delete_on_decomposition_dictionary BEFORE DELETE ON sys_stonedb.decomposition_dictionary FOR EACH ROW BEGIN SELECT COUNT(*) FROM sys_stonedb.columns WHERE decomposition = OLD.ID INTO @count; IF @count <> 0 THEN SET @@stonedb_trigger_error = concat ('Delete failed: The decomposition \'', OLD.ID, '\' IS used IN at least one column.'); END IF; END ;

DROP trigger IF EXISTS sys_stonedb.after_insert_on_columns;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_stonedb.after_insert_on_columns AFTER INSERT ON sys_stonedb.columns FOR EACH ROW BEGIN SET @@global.stonedb_refresh_sys_stonedb = TRUE; END ;

DROP trigger IF EXISTS sys_stonedb.after_update_on_columns;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_stonedb.after_update_on_columns AFTER UPDATE ON sys_stonedb.columns FOR EACH ROW BEGIN SET @@global.stonedb_refresh_sys_stonedb = TRUE; END ;

DROP trigger IF EXISTS sys_stonedb.after_delete_on_columns;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_stonedb.after_delete_on_columns AFTER DELETE ON sys_stonedb.columns FOR EACH ROW BEGIN SET @@global.stonedb_refresh_sys_stonedb = TRUE; END ;

DROP trigger IF EXISTS sys_stonedb.after_insert_on_decomposition_dictionary;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_stonedb.after_insert_on_decomposition_dictionary AFTER INSERT ON sys_stonedb.decomposition_dictionary FOR EACH ROW BEGIN SET @@global.stonedb_refresh_sys_stonedb = TRUE; END ;

DROP trigger IF EXISTS sys_stonedb.after_update_on_decomposition_dictionary;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_stonedb.after_update_on_decomposition_dictionary AFTER UPDATE ON sys_stonedb.decomposition_dictionary FOR EACH ROW BEGIN SET @@global.stonedb_refresh_sys_stonedb = TRUE; END ;

DROP trigger IF EXISTS sys_stonedb.after_delete_on_decomposition_dictionary;
CREATE DEFINER=`root`@`localhost` TRIGGER sys_stonedb.after_delete_on_decomposition_dictionary AFTER DELETE ON sys_stonedb.decomposition_dictionary FOR EACH ROW BEGIN  SET @@global.stonedb_refresh_sys_stonedb = TRUE; END ;

DROP PROCEDURE IF EXISTS `sys_stonedb`.`CREATE_RULE`;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_stonedb`.`CREATE_RULE` ( IN id_ VARCHAR(4000), IN expression_ VARCHAR(4000), IN comment_ VARCHAR(4000) ) BEGIN INSERT INTO sys_stonedb.decomposition_dictionary ( ID, RULE, COMMENT ) VALUES( id_, expression_, comment_ ); END;

DROP PROCEDURE IF EXISTS `sys_stonedb`.`UPDATE_RULE`;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_stonedb`.`UPDATE_RULE` ( IN id_ VARCHAR(4000), IN expression_ VARCHAR(4000) ) BEGIN UPDATE sys_stonedb.decomposition_dictionary SET RULE = expression_ WHERE ID = id_; END;

DROP PROCEDURE IF EXISTS `sys_stonedb`.`DELETE_RULE`;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_stonedb`.`DELETE_RULE` ( IN id_ VARCHAR(4000) ) BEGIN DELETE FROM sys_stonedb.decomposition_dictionary WHERE ID = id_; END;

DROP PROCEDURE IF EXISTS `sys_stonedb`.`CHANGE_RULE_COMMENT`;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_stonedb`.`CHANGE_RULE_COMMENT` ( IN id_ VARCHAR(4000), IN comment_ VARCHAR(4000) ) BEGIN UPDATE sys_stonedb.decomposition_dictionary SET COMMENT = comment_ WHERE ID = id_; END;

DROP PROCEDURE IF EXISTS `sys_stonedb`.`SET_DECOMPOSITION_RULE`;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_stonedb`.`SET_DECOMPOSITION_RULE` ( IN database_ VARCHAR(4000), IN table_ VARCHAR(4000), IN column_ VARCHAR(4000), IN id_ VARCHAR(4000) ) BEGIN DELETE FROM sys_stonedb.columns WHERE database_name = database_ AND table_name = table_ AND column_name = column_; INSERT INTO sys_stonedb.columns ( database_name, table_name, column_name, decomposition ) VALUES( database_, table_, column_, id_ ); END;

DROP PROCEDURE IF EXISTS `sys_stonedb`.`DELETE_DECOMPOSITION_RULE`;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_stonedb`.`DELETE_DECOMPOSITION_RULE` ( IN database_ VARCHAR(4000), IN table_ VARCHAR(4000), IN column_ VARCHAR(4000) ) BEGIN DELETE FROM sys_stonedb.columns WHERE database_name = database_ AND table_name = table_ AND column_name = column_; END;

DROP PROCEDURE IF EXISTS `sys_stonedb`.`SHOW_DECOMPOSITION`;
CREATE DEFINER=`root`@`localhost` PROCEDURE `sys_stonedb`.`SHOW_DECOMPOSITION` ( IN database_ VARCHAR(4000), IN table_ VARCHAR(4000) ) BEGIN SELECT s.ordinal_position AS '#', s.column_name AS 'column name', s.data_type AS 'data type', MIN( c.decomposition ) AS decomposition, MIN( d.rule ) AS rule FROM information_schema.columns AS s LEFT JOIN sys_stonedb.columns c ON s.table_schema = c.database_name AND s.table_name = c.table_name AND s.column_name = c.column_name LEFT JOIN sys_stonedb.decomposition_dictionary d ON c.decomposition = d.id WHERE s.table_schema = database_ AND s.table_name = table_ GROUP BY s.ordinal_position, s.column_name, s.data_type ORDER BY s.ordinal_position; END;

