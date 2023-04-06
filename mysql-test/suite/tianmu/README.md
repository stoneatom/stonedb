Stonedb prepares the rules to be followed by MTR

# Writing Test Cases
- <font color=blue>Test Case Coding Guidelines  </font>

## <font color=blue>Test Case Coding Guidelines  </font>
- Test case file names may use alphanumeric characters (A-Z, a-z, 0-9), underscore ('_') or dash ('-'), but should not start with underscore or dash

## Test Case Content-Formatting Guidelines  
- To write a test case file, use any text editor that uses linefeed (newline) as the end-of-line character.  
- Avoid lines longer than 80 characters unless there is no other choice.  
- A comment in a test case is started with the "#" character.  
- Use spaces, not tabs.  
- Use uppercase for SQL keywords.  
- Use lowercase for identifiers (names of objects such as databases, tables, columns, and so forth).  
- Break a long SQL statement into multiple lines to make it more readable  
- Please include a header in test files that indicates the purpose of the test and references the corresponding worklog task, if any.  
- Comments for a test that is related to a bug report should include the bug number and title.  

## Naming Conventions for Database Objects  
- User names: User names should begin with “mysql” (for example, mysqluser1, mysqluser2)
- Database names: Unless you have a special reason not to, use the default database named test that is already created for you. For tests that need to operate outside the test database, database names should contain “test” or begin with “mysql” (for example, mysqltest1, mysqltest2)
- Table names: t1, t2, t3, ...
- View names: v1, v2, v3, ...

## Use case annotation requirements  
The sample is as follows：
```
--echo #
--echo # Fill in the use case description here
--echo #
```

# Run Test Cases  
1. Setting Running Parameters: mysql-test/include/default_mysqld.cnf
- default-storage-engine=tianmu
- tianmu_insert_delayed=0
