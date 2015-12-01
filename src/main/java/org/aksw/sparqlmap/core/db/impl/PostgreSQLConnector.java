package org.aksw.sparqlmap.core.db.impl;

import org.aksw.sparqlmap.core.db.DBAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgreSQLConnector extends DBAccess {

  public PostgreSQLConnector() {
    super(new PostgreSQLDataTypeHelper());
  }

  public static final String POSTGRES_DBNAME = "PostgreSQL";
  public static final String POSTGRES_DRIVER = "org.postgresql.Driver";

  {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (Exception e) {
      LoggerFactory.getLogger(MySQLConnector.class).info("PostgreSQL driver not present", e);
    }

  }

  private static Logger log = LoggerFactory.getLogger(MySQLConnector.class);

  @Override
  public String getDBName() {
    return POSTGRES_DBNAME;
  }

  @Override
  public String getDriverClassString() {
    return POSTGRES_DRIVER;
  }

}
