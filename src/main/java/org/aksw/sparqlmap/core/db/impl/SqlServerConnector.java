package org.aksw.sparqlmap.core.db.impl;

import org.aksw.sparqlmap.core.db.DBAccess;

public class SqlServerConnector extends DBAccess {

  public SqlServerConnector() {
    super(new SqlServerDataTypeHelper());
  }

  public static String SQL_SEVER_NAME = "MSSQLSERVER";
  public static String SQL_SERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

  @Override
  public String getDBName() {

    return SQL_SEVER_NAME;
  }

  @Override
  public String getDriverClassString() {
    return SQL_SERVER_DRIVER;
  }

}
