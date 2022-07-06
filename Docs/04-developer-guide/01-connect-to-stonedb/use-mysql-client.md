---
id: use-mysql-client
sidebar_position: 5.21
---

# Use mysql to Connect to StoneDB

This topic describes how to use the MySQL command-line client named mysql to connect to StoneDB.
## Prerequisites

- mysql has been installed and its version is 5.5, 5.6, or 5.7.
- The value of environment variable **PATH** contains the directory that stores mysql.
## Procedure

1. Open mysql.
2. Specify required parameters according to the following format.
```shell
mysql -u -p -h -P -S -A
```
## Parameter description
The following table describes required parameters.

| **Parameter** | **Description** |
| --- | --- |
| -h | The IP address of StoneDB. |
| -u | The username of the account. |
| -p | The password of the account. You can skip this parameter for security purposes and enter the password as prompted later. |
| -P | The port used to connect to StoneDB. By default, the port 3306 is used. You can specify another port as needed. |
| -A | The name of the database. |
| -S | The name of the .sock file that is used to connect to StoneDB. |


:::tip

- If you want to exit the command-line client, enter** exit** and press **Enter.**
- For more information about parameters, run `mysql --help`.

:::
