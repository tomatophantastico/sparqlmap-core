package org.aksw.sparqlmap.core.db.impl;

import java.sql.Driver;

import org.aksw.sparqlmap.core.db.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.jolbox.bonecp.BoneCPDataSource;

public class HSQLDBConnector extends Connector {
	
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
	public String getDriverVersion() {
		String result = null;
		
		try {
			Driver driver =(Driver)  Class.forName(driverClass).newInstance();
			
			result  = driver.getClass().getName() +  driver.getMajorVersion() + "." + driver.getMinorVersion();
 		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			result  = Connector.DRIVER_NA;
		} 
		
		return result;
	}

	
	
	
}
