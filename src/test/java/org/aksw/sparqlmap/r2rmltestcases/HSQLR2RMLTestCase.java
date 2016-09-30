package org.aksw.sparqlmap.r2rmltestcases;

import java.util.Collection;

import org.aksw.sparqlmap.DockerHelper.DBConnConfig;
import org.aksw.sparqlmap.core.db.impl.HSQLDBDataTypeHelper;
import org.aksw.sparqlmap.core.translate.jdbc.DataTypeHelper;
import org.hsqldb.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized.Parameters;

public class HSQLR2RMLTestCase extends R2RMLTest{
	
	

	
	public HSQLR2RMLTestCase(String testCaseName, String r2rmlLocation,
			String outputLocation, String referenceOutput,
			String dbFileLocation, boolean createDM) {
		super(testCaseName, r2rmlLocation, outputLocation, referenceOutput,
				dbFileLocation, createDM);
	}

	



	@Parameters(name="{0}")
	public static Collection<Object[]> data() {
		return data(getTestCaseLocations());
		
	}
	
	public static String getTestCaseLocations() {
		
		return "./src/test/resources/testcases/hsqldb/";
	}
	

	static Server server = null;
	
	

	
	

  @BeforeClass
  public static void startServer(){
    server = new Server();
    server.setSilent(false);
    server.setDatabaseName(0, "bsbm2-100k");
    server.setDatabasePath(0, "mem:sparqlmaptest\"");
    server.start();
    dbconf = new DBConnConfig();
    dbconf.jdbcString = "jdbc:hsqldb:mem:sparqlmaptest/";
    dbconf.username =  "sa";
    dbconf.password = "";
  }
  
  @AfterClass
  public static void stopServer(){
    server.stop();
  }





  @Override
  DataTypeHelper getDataTypeHelper() {
    return new HSQLDBDataTypeHelper();
  }



}
