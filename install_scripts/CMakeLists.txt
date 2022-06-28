SET (INSTALL_ENGINE "/stonedb56")
SET (ENGINE_MYSQLDIR   "${INSTALL_ENGINE}/install")
install(FILES my.cnf
              DESTINATION ${ENGINE_MYSQLDIR} COMPONENT storage-engine)
install(PROGRAMS mysql_server install.sh reinstall.sh
              DESTINATION ${ENGINE_MYSQLDIR} COMPONENT storage-engine)
