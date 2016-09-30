package org.aksw.sparqlmap.r2rmltestcases;

import java.util.Collection;

import org.aksw.sparqlmap.DockerHelper.DBConnConfig;
import org.aksw.sparqlmap.core.db.impl.OracleConnector;
import org.aksw.sparqlmap.core.db.impl.OracleDataTypeHelper;
import org.aksw.sparqlmap.core.translate.jdbc.DataTypeHelper;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleR2RMLTestCase extends R2RMLTest{
	
	
	private static Logger log = LoggerFactory.getLogger(OracleR2RMLTestCase.class);
	

	
	public OracleR2RMLTestCase(String testCaseName, String r2rmlLocation,
			String outputLocation, String referenceOutput,
			String dbFileLocation, boolean createDM) {
		super(testCaseName, r2rmlLocation, outputLocation, referenceOutput,
				dbFileLocation, createDM);
	}

	
	@BeforeClass
  	public static void setOracleConnection(){
  	dbconf = new DBConnConfig();
    dbconf.jdbcString = "jdbc:oracle:thin:@//192.168.59.104/r2rml";
    dbconf.username =  "r2rmladmin";
    dbconf.password = "r2rmladmin";
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
  DataTypeHelper getDataTypeHelper() {
    return new OracleDataTypeHelper();
  }
}
