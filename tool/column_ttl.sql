CREATE TABLE IF NOT EXISTS sys_stonedb.column_ttl (
    database_name VARCHAR( 255 ) NOT NULL,
    table_name VARCHAR( 255 ) NOT NULL,
    column_name VARCHAR( 255 ) NOT NULL,
    database_suffix VARCHAR( 255 ) NOT NULL,
    table_suffix VARCHAR( 255 ) NOT NULL,
    ttl INT NOT NULL ) ENGINE=CSV;

CREATE TABLE IF NOT EXISTS sys_stonedb.logs (
    ts timestamp NOT NULL DEFAULT NOW(),
    msg VARCHAR(255) NOT NULL) ENGINE=CSV;

delimiter |
CREATE EVENT IF NOT EXISTS sys_stonedb.daily_event
ON SCHEDULE EVERY 10 SECOND
DO BEGIN
  DECLARE db_name_base VARCHAR(255);
  DECLARE tab_name_base VARCHAR(255);
  DECLARE col_name VARCHAR(255);
  DECLARE db_suffix VARCHAR(255);
  DECLARE tab_suffix VARCHAR(255);
  DECLARE db_name VARCHAR(255);
  DECLARE tab_name VARCHAR(255);
  DECLARE col_default VARCHAR(255);
  DECLARE nullable VARCHAR(255);
  DECLARE col_type VARCHAR(255);
  DECLARE col_comment VARCHAR(255);
  DECLARE pos INT;
  DECLARE col_def VARCHAR(255);
  DECLARE ttl_val INT;

  DECLARE not_found INT DEFAULT FALSE;

  DECLARE cur1 CURSOR FOR
     SELECT database_name, table_name, column_name, database_suffix, table_suffix, ttl
     FROM sys_stonedb.column_ttl;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET not_found = TRUE;

  OPEN cur1;
  loop1: LOOP
    FETCH cur1 INTO db_name_base,tab_name_base,col_name,db_suffix,tab_suffix,ttl_val;
    IF not_found THEN
      LEAVE loop1;
    END IF;

    BLOCK2: BEGIN
      DECLARE cur2 CURSOR FOR
          SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_DEFAULT,IS_NULLABLE,COLUMN_TYPE,COLUMN_COMMENT,ORDINAL_POSITION
          FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA RLIKE CONCAT(db_name_base, db_suffix)
                AND TABLE_NAME RLIKE CONCAT_WS('_', tab_name_base,
                                               DATE_FORMAT(NOW() - INTERVAL ttl_val+1 DAY, '%Y%m%d'),
                                               tab_suffix)
                AND COLUMN_NAME=col_name;

      OPEN cur2;
      loop2: LOOP
        FETCH cur2 INTO db_name, tab_name, col_default, nullable,col_type,col_comment,pos;

        IF not_found THEN
          LEAVE loop2;
        END IF;

        set col_def = CONCAT_WS(' ', col_name, col_type);

        IF nullable='NO' THEN
            set col_def = CONCAT_WS(' ', col_def, "NOT NULL");
        END IF;

        IF col_default IS NOT NULL THEN
            set col_def = CONCAT_WS(' ', col_def, "DEFAULT", col_default);
        END IF;

        SET col_comment = SUBSTRING(col_comment, 1, instr(col_comment, '; Size[MB]:')-1);

        IF col_comment != '' THEN
            set col_def = CONCAT(col_def, " COMMENT ", QUOTE(col_comment));
        END IF;

        IF pos = 1 THEN
            set col_def = CONCAT_WS(' ', col_def, "FIRST");
        ELSE
            set pos = pos-1;
            select COLUMN_NAME INTO @b4 from information_schema.columns
            where TABLE_SCHEMA=db_name AND TABLE_NAME=tab_name AND ORDINAL_POSITION=pos;
            set col_def = CONCAT_WS(' ', col_def, "AFTER", @b4);
        END IF;

        SET @sql_text = CONCAT('ALTER TABLE ', db_name, '.', tab_name, ' DROP COLUMN ', col_name);
        PREPARE stmt FROM @sql_text;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SET @sql_text = CONCAT('ALTER TABLE ', db_name, '.', tab_name, ' ADD COLUMN ', col_def);
        PREPARE stmt FROM @sql_text;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        INSERT INTO sys_stonedb.logs(msg) VALUES (CONCAT_WS(' ', 'NULLed column', db_name, tab_name, col_def));
      END LOOP;
      close cur2;
    END BLOCK2;
  END LOOP;
END |
delimiter ;

