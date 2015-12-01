package org.aksw.sparqlmap.r2rmltestcases;

import java.util.Collection;

import org.aksw.sparqlmap.DockerHelper;
import org.aksw.sparqlmap.core.db.impl.PostgreSQLDataTypeHelper;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spotify.docker.client.DockerException;

public class PostgreSQLR2RMLTestCase extends R2RMLTest {
	
	public PostgreSQLR2RMLTestCase(String testCaseName, String r2rmlLocation,
			String outputLocation, String referenceOutput,
			String dbFileLocation, boolean createDM) {
		super(testCaseName, r2rmlLocation, outputLocation, referenceOutput,
				dbFileLocation, createDM);
	}

	
	private static Logger log = LoggerFactory.getLogger(PostgreSQLR2RMLTestCase.class);
	
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
    try{
        dbconf =  DockerHelper.startDirectOrDockerizedPostgres();
    } catch (Exception e) {
      log.error("FAiled to start Docker contaier", e);
      dbIsReachable = false;
    }
  }

  @AfterClass
  public static void stopPostgresDocker() throws DockerException, InterruptedException {
    DockerHelper.stopdirecOrDockerizedPostgres();
    
  }


  @Override
  public DataTypeHelper getDataTypeHelper() {
    
    return new PostgreSQLDataTypeHelper();
  }
	
	

	



	

}
