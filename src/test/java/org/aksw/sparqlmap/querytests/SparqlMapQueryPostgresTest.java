package org.aksw.sparqlmap.querytests;

import java.io.File;
import java.util.Collection;

import org.aksw.sparqlmap.DockerHelper;
import org.aksw.sparqlmap.core.db.impl.PostgreSQLConnector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spotify.docker.client.DockerException;

/**
 * Test using a postgresql instance the test can completely mess up. Default setup is to test against a dockerized boot to docker setup.
 * 
 * e.g.:  docker run --rm -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres
 * @author joerg
 *
 */
public class SparqlMapQueryPostgresTest  extends SparqlMapQueryBaseTest{
  
  private static Logger log = LoggerFactory.getLogger(SparqlMapQueryPostgresTest.class);


  @BeforeClass
  public static void startPostGresDocker() throws DockerException, InterruptedException {
    try{
      dbconf = DockerHelper.startDirectOrDockerizedPostgres();
    } catch (Exception e) {
      log.error("FAiled to start Docker contaier", e);
      canConnect = false;
    }
  }
  


  @AfterClass
  public static void doTeardownHost() throws DockerException, InterruptedException {
    
    DockerHelper.stopdirecOrDockerizedPostgres();
    
  }
  
  
  
  public SparqlMapQueryPostgresTest(String testname, String dsName,
      File mappingFile, File sqlFile, String query) {
    super(testname, dsName, mappingFile, sqlFile, query);
  }
  
  
  @Parameters(name="{0}")
  public static Collection<Object[]> dbdata(){
    return data(PostgreSQLConnector.POSTGRES_DBNAME);
  }
  
  
  

}
