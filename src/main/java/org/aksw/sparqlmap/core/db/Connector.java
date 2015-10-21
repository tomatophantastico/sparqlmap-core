package org.aksw.sparqlmap.core.db;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCPDataSource;
/**
 * holds the connection pool and some other interesting stuff
 * @author joerg
 *
 */
public abstract class Connector {
	
	public static final String DRIVER_NA = "Driver not avalilable";
	private static Logger log = LoggerFactory.getLogger(Connector.class);

	public Connector( ){
	}
	
	public void setDs(BoneCPDataSource ds){
		this.connectionPool =ds; 
	}
	

	public Connection getConnection() throws SQLException{
		return connectionPool.getConnection(); 
	}
	

	protected BoneCPDataSource connectionPool = null;
	

	public abstract String getDBName();
	
	public abstract String getDriverClassString();




	public void close(){
		connectionPool.close();
	}
	
		
	 public String getDriverVersion() {
	    String result = null;
	    
	    try {
	      Driver driver =(Driver)  Class.forName(getDriverClassString()).newInstance();
	      
	      result  = driver.getClass().getName() +  driver.getMajorVersion() + "." + driver.getMinorVersion();
	    } catch (InstantiationException | IllegalAccessException
	        | ClassNotFoundException e) {
	      result  = Connector.DRIVER_NA;
	    } 
	    
	    return result;
	  }
	 
	 



}