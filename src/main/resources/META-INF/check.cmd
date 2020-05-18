@echo off

      set "CHECKER_WORK_DIR=%~dp0"
      set "CLSPATH=%CHECKER_WORK_DIR%\lib\dom4j-1.6.1.jar;%CHECKER_WORK_DIR%\lib\druid-1.1.16.jar;%CHECKER_WORK_DIR%\lib\sword-sqlchecker-1.0.0.jar;%CHECKER_WORK_DIR%\lib\ojdbc6.jar;"

      set "sjmx_table=HX_SJMX.SJMX_TABLE"
      set "checker_white_file=%CHECKER_WORK_DIR%\sql-checker-white.properties"
      set "checker_sql_xml_dir=%CHECKER_WORK_DIR%\sql-xmls"

      java -cp %CLSPATH% com.css.sword.sqlchecker.SqlChecker %checker_white_file% %checker_sql_xml_dir% %sjmx_table% > %CHECKER_WORK_DIR%\res.txt 2>&1

      echo check end
      @pause