---
id: character-sets
sidebar_position: 6.1
---

# Character Sets

A character set is a collection of symbols and encodings, where the encodings determine how character strings are stored. A collation is a collection of rules for comparing and sorting character strings. A character set is associated with a collation. If you change either of them, the other also changes.

## Supported character sets
You can execute the `SHOW CHARACTER SET` statement to view the character sets supported by StoneDB.

```sql
> show character set; 
+----------+---------------------------------+---------------------+--------+
| Charset  | Description                     | Default collation   | Maxlen |
+----------+---------------------------------+---------------------+--------+
| big5     | Big5 Traditional Chinese        | big5_chinese_ci     |      2 |
| dec8     | DEC West European               | dec8_swedish_ci     |      1 |
| cp850    | DOS West European               | cp850_general_ci    |      1 |
| hp8      | HP West European                | hp8_english_ci      |      1 |
| koi8r    | KOI8-R Relcom Russian           | koi8r_general_ci    |      1 |
| latin1   | cp1252 West European            | latin1_swedish_ci   |      1 |
| latin2   | ISO 8859-2 Central European     | latin2_general_ci   |      1 |
| swe7     | 7bit Swedish                    | swe7_swedish_ci     |      1 |
| ascii    | US ASCII                        | ascii_general_ci    |      1 |
| ujis     | EUC-JP Japanese                 | ujis_japanese_ci    |      3 |
| sjis     | Shift-JIS Japanese              | sjis_japanese_ci    |      2 |
| hebrew   | ISO 8859-8 Hebrew               | hebrew_general_ci   |      1 |
| tis620   | TIS620 Thai                     | tis620_thai_ci      |      1 |
| euckr    | EUC-KR Korean                   | euckr_korean_ci     |      2 |
| koi8u    | KOI8-U Ukrainian                | koi8u_general_ci    |      1 |
| gb2312   | GB2312 Simplified Chinese       | gb2312_chinese_ci   |      2 |
| greek    | ISO 8859-7 Greek                | greek_general_ci    |      1 |
| cp1250   | Windows Central European        | cp1250_general_ci   |      1 |
| gbk      | GBK Simplified Chinese          | gbk_chinese_ci      |      2 |
| latin5   | ISO 8859-9 Turkish              | latin5_turkish_ci   |      1 |
| armscii8 | ARMSCII-8 Armenian              | armscii8_general_ci |      1 |
| utf8     | UTF-8 Unicode                   | utf8_general_ci     |      3 |
| ucs2     | UCS-2 Unicode                   | ucs2_general_ci     |      2 |
| cp866    | DOS Russian                     | cp866_general_ci    |      1 |
| keybcs2  | DOS Kamenicky Czech-Slovak      | keybcs2_general_ci  |      1 |
| macce    | Mac Central European            | macce_general_ci    |      1 |
| macroman | Mac West European               | macroman_general_ci |      1 |
| cp852    | DOS Central European            | cp852_general_ci    |      1 |
| latin7   | ISO 8859-13 Baltic              | latin7_general_ci   |      1 |
| utf8mb4  | UTF-8 Unicode                   | utf8mb4_general_ci  |      4 |
| cp1251   | Windows Cyrillic                | cp1251_general_ci   |      1 |
| utf16    | UTF-16 Unicode                  | utf16_general_ci    |      4 |
| utf16le  | UTF-16LE Unicode                | utf16le_general_ci  |      4 |
| cp1256   | Windows Arabic                  | cp1256_general_ci   |      1 |
| cp1257   | Windows Baltic                  | cp1257_general_ci   |      1 |
| utf32    | UTF-32 Unicode                  | utf32_general_ci    |      4 |
| binary   | Binary pseudo charset           | binary              |      1 |
| geostd8  | GEOSTD8 Georgian                | geostd8_general_ci  |      1 |
| cp932    | SJIS for Windows Japanese       | cp932_japanese_ci   |      2 |
| eucjpms  | UJIS for Windows Japanese       | eucjpms_japanese_ci |      3 |
| gb18030  | China National Standard GB18030 | gb18030_chinese_ci  |      4 |
+----------+---------------------------------+---------------------+--------+
```

## Variables relevant to character sets

You can execute the following statement to show the variables relevant to character sets.

```sql
> show variables like '%char%';
+--------------------------+----------------------------------+
| Variable_name            | Value                            |
+--------------------------+----------------------------------+
| character_set_client     | utf8                             |
| character_set_connection | utf8                             |
| character_set_database   | utf8mb4                          |
| character_set_filesystem | binary                           |
| character_set_results    | utf8                             |
| character_set_server     | utf8mb4                          |
| character_set_system     | utf8                             |
| character_sets_dir       | /stonedb/install/share/charsets/ |
+--------------------------+----------------------------------+
8 rows in set (0.01 sec)
```

Variable description:

- **character_set_client** specifies the character set used by the server to receive requests from the client.
- **character_set_connection** specifies the character set converted by the server from the character set specified by **character_set_client**.
- **character_set_results** specifies the character set used by the server to respond to requests sent by the client. 

The character set used by a client to send requests to a server and receive response from the server is the character set used by the client OS, which can be specified by the **LC_ALL**, **LC_CTYPE**, or **LANG **variable. Among the three variables, **LC_ALL** has the highest priority, **LC_CTYPE** the second, and then **LANG** the lowest.

If the client OS uses the UTF-8 character set and **default-character-set** is set to **gbk** during the client startup, **character_set_client**, **character_set_connection**, and **character_set_results** are automatically set to **gbk**. Now, the client requests a table that contains Chinese characters. The representation of each Chinese character in the server and that in the client are two different strings. In a UNIX OS, data received by the server will be converted based on the character set specified by **character_set_connection**. In this case, the data presented to the server is garbled text.

:::info
For StoneDB, if no character set is specified for the database when the database is being created, the database uses the character set specified by **character_set_server**, by default. If a table is not specified with a character set when the table is being created, the table uses the character set used by the database. Once a table is created, you cannot change or convert its character set.
:::
