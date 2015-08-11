package org.aksw.sparqlmap.core.db.impl;

import javax.annotation.PostConstruct;

import org.aksw.sparqlmap.core.db.Connector;
import org.aksw.sparqlmap.core.exception.QueryingException;
import org.aksw.sparqlmap.core.exception.SetupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.jolbox.bonecp.BoneCPDataSource;

public class MySQLConnector extends Connector {
	
	public static final String MYSQL_DBNAME = "MySQL";
	public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	private static Logger log = LoggerFactory.getLogger(MySQLConnector.class);
	
		
	@PostConstruct
	public void validateDB() throws SetupException{
  String dbConnectionString = connectionPool.getConfig().getJdbcUrl();
    
    if(!dbConnectionString.contains("padCharsWithSpace")){
      throw new SetupException("MYSQL requires padCharsWithSpace=true to be set in the jdbc url");
    }
    if(!dbConnectionString.contains("ANSI_QUOTES")){
      throw new SetupException("MYSQL requires sessionVariables=sql_mode='ANSI_QUOTES' to be set in the jdbc url");
    }
    

	}
	

	
	
	@Override
	public String getDBName() {
		return MYSQL_DBNAME;
	}

	@Override
	public String getDriverClassString() {
	  return MYSQL_DRIVER;
	}
}
		

	
