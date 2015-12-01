package org.aksw.sparqlmap.core.db.impl;

import javax.sql.DataSource;

import org.aksw.sparqlmap.core.db.DBAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCPDataSource;

public class HSQLDBConnector extends DBAccess {
	
	public HSQLDBConnector() {
    super(new HSQLDBDataTypeHelper());
  }

  private String driverClass ="org.hsqldb.jdbcDriver" ;
	
	
	{
		try{
			Class.forName(driverClass); 
		} catch (Exception e) {
			LoggerFactory.getLogger(HSQLDBConnector.class).error("Error loading HSQLDB driver",e);
		}
			
		
	}
	

	public static final String HSQLDB_NAME = "HSQL Database Engine";


	private static Logger log = LoggerFactory.getLogger(HSQLDBConnector.class);
	
	
	@Override
	public String getDBName() {
		return HSQLDB_NAME;
	}	
	
	@Override
	public String getDriverClassString() {
	 
	  return driverClass;
	}
	

}
