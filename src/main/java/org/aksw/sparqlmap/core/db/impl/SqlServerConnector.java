package org.aksw.sparqlmap.core.db.impl;

import org.aksw.sparqlmap.core.db.Connector;

public class SqlServerConnector extends Connector{
  
  public static final String SQL_SEVER_NAME = "MSSQLSERVER";
  public static final String SQL_SERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

  @Override
  public String getDBName() {
   
    return SQL_SEVER_NAME;
  }

 
  
  
  @Override
  public String getDriverClassString() {
    return SQL_SERVER_DRIVER;
  }
  

}
