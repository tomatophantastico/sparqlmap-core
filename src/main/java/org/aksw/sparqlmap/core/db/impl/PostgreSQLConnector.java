package org.aksw.sparqlmap.core.db.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import org.aksw.sparqlmap.core.db.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PostgreSQLConnector extends Connector {
	
	
	public static final String POSTGRES_DBNAME = "PostgreSQL";
	public static final String POSTGRES_DRIVER = "org.postgresql.Driver";


	{
		try{
			Class.forName("org.postgresql.Driver" ); 
		} catch (Exception e) {
			LoggerFactory.getLogger(MySQLConnector.class).info("PostgreSQL driver not present",e);
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
