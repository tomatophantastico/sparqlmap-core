package org.aksw.sparqlmap.core.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.ServiceLoader;

import javax.annotation.PostConstruct;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.SystemInitializationError;
import org.aksw.sparqlmap.core.db.impl.HSQLDBConnector;
import org.aksw.sparqlmap.core.db.impl.HSQLDBDataTypeHelper;
import org.aksw.sparqlmap.core.db.impl.MySQLConnector;
import org.aksw.sparqlmap.core.db.impl.MySQLDataTypeHelper;
import org.aksw.sparqlmap.core.db.impl.PostgreSQLConnector;
import org.aksw.sparqlmap.core.db.impl.PostgreSQLDataTypeHelper;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;

@Component
public class DBAccessConfigurator {
	
	
	@Autowired
	private Environment env;
	
	
	private String dbname;
	
	private BoneCPDataSource bcp;
	
	
	@PostConstruct
	public void setUpConnection() throws SQLException{
		
			
		String dbUrl = env.getProperty("jdbc.url");
		String username = env.getProperty("jdbc.username");
		String password = env.getProperty("jdbc.password");
		Integer poolminconnections = env.getProperty("jdbc.poolminconnections")!=null?Integer.parseInt(env.getProperty("jdbc.poolminconnections")):null;
		Integer poolmaxconnections = env.getProperty("jdbc.poolmaxconnections")!=null?Integer.parseInt(env.getProperty("jdbc.poolmaxconnections")):null;
		
		
		BoneCPConfig config = createConfig(dbUrl, username, password,
				poolminconnections, poolmaxconnections);
		bcp  = new BoneCPDataSource(config);
		
		Connection conn = bcp.getConnection();
		dbname = conn.getMetaData().getDatabaseProductName();
		conn.close();
		
						
	}
	
	
	


	public static BoneCPConfig createConfig(String dbUrl, String username,
			String password, Integer poolminconnections,
			Integer poolmaxconnections) {
		if (poolmaxconnections==null){
			poolmaxconnections =10;
		}
		if (poolminconnections == null){
			poolminconnections = 5;
		}
		
		
		BoneCPConfig config = new BoneCPConfig();
		config.setJdbcUrl(dbUrl); 
		config.setUsername(username); 
		config.setPassword(password);
		config.setMinConnectionsPerPartition(poolminconnections);
		config.setMaxConnectionsPerPartition(poolmaxconnections);
		config.setPartitionCount(1);
		return config;
	}
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DBAccessConfigurator.class);
	
	

	@Bean
	public DataTypeHelper getDataTypeHelper() {
		
		Iterator<DataTypeHelper> dthi =  ServiceLoader.load(DataTypeHelper.class).iterator();
		DataTypeHelper dthToUse = null;
		
		while(dthi.hasNext()){
			DataTypeHelper dth = dthi.next();
			
			
			if(dbname.equals(dth.getDBName())){
				dthToUse = dth;
			}
		}

		if(dthToUse==null){
			throw new SystemInitializationError("Unknown Database " + dbname + " encountered");
		}
		
		return dthToUse;

	}
	
	@Bean
	public DBAccess getDBAccess(){
		
		Iterator<Connector> conns =  ServiceLoader.load(Connector.class).iterator();
		Connector connToUse = null;
		
		while(conns.hasNext()){
			Connector conn = conns.next();
			
			if(log.isInfoEnabled()){
				log.info("Detected :" + conn.getDBName() + " Module, Driver: " +  conn.getDriverVersion());
			}
			
			if(dbname.equals(conn.getDBName())){
				connToUse = conn;
			}
		}

		if(connToUse==null){
			throw new SystemInitializationError("Unknown Database " + dbname + " encountered");
		}
		
		connToUse.setDs(bcp);
		
		return new DBAccess(connToUse);
	}
	
	
	public String getDBName(){
		return dbname;
	}
	
}	