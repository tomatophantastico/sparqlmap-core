package org.aksw.sparqlmap.r2rmltestcases;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import org.aksw.sparqlmap.DockerHelper;
import org.aksw.sparqlmap.core.db.Connector;
import org.aksw.sparqlmap.core.db.DBAccessConfigurator;
import org.aksw.sparqlmap.core.db.impl.PostgreSQLConnector;
import org.aksw.sparqlmap.core.db.impl.PostgreSQLDataTypeHelper;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized.Parameters;

import com.jolbox.bonecp.BoneCPDataSource;
import com.spotify.docker.client.DockerException;

public class PostgreSQLR2RMLTestCase extends R2RMLTest {
	
	public PostgreSQLR2RMLTestCase(String testCaseName, String r2rmlLocation,
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
		
		return "./src/test/resources/testcases/postgres/";
	}
	

  @BeforeClass
  public static void startPostgresDocker() throws DockerException, InterruptedException {
   // this approach will work for most dev setups
      dbconf =  DockerHelper.startPostGresDocker();
  }

  @AfterClass
  public static void stopPostgresDocker() throws DockerException, InterruptedException {
    DockerHelper.stopPostGresDocker();
    
  }


  @Override
  public DataTypeHelper getDataTypeHelper() {
    
    return new PostgreSQLDataTypeHelper();
  }
	
	

	



	

}
