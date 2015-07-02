package org.aksw.sparqlmap.r2rmltestcases;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.aksw.sparqlmap.DBHelper;
import org.aksw.sparqlmap.core.db.Connector;
import org.aksw.sparqlmap.core.db.DBAccessConfigurator;
import org.aksw.sparqlmap.core.db.impl.OracleConnector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggerFactoryBinder;

import com.google.common.collect.Lists;
import com.jolbox.bonecp.BoneCPDataSource;

public class OracleR2RMLTestCase extends R2RMLTest{
	
	
	private static Logger log = LoggerFactory.getLogger(OracleR2RMLTestCase.class);
	

	
	public OracleR2RMLTestCase(String testCaseName, String r2rmlLocation,
			String outputLocation, String referenceOutput,
			String dbFileLocation, boolean createDM) {
		super(testCaseName, r2rmlLocation, outputLocation, referenceOutput,
				dbFileLocation, createDM);
	}

	

	OracleConnector connector;


	@Parameters(name="{0}")
	public static Collection<Object[]> data() {
		return data(getTestCaseLocations());
		
	}
	
  public static String getTestCaseLocations() {
    
    return "./src/test/resources/testcases/oracle/";
  }
  


	
	@Override
	public void flushDatabase() throws ClassNotFoundException, SQLException {
		Connection conn = getConnector().getConnection();
		
		DBHelper.flushDbOracle(conn);

		conn.close();
	}
	

	@Override
	public Properties getDBProperties() {
		
		Properties properties = new Properties();
		try {
			properties.load(ClassLoader.getSystemResourceAsStream("r2rml-test/db-oracle.properties"));
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail("Unable to load properties file");
			
		}
		return properties;
	}


	@After
	public void cleanup(){
		connector.close(); 
		


	}
	
	@Before
	public void before(){
		BoneCPDataSource ds = new BoneCPDataSource(
				DBAccessConfigurator.createConfig(
						getDBProperties().getProperty("jdbc.url"), getDBProperties().getProperty("jdbc.username"), getDBProperties().getProperty("jdbc.password"), 1, 2));
		
		OracleConnector conn = new OracleConnector();
		conn.setDs(ds);
		
		connector = conn;
	}


	@Override
	public Connector getConnector() {
		// TODO Auto-generated method stub
		return connector;
	}

}
