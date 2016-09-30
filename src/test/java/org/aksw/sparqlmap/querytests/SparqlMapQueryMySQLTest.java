package org.aksw.sparqlmap.querytests;

import java.io.File;
import java.util.Collection;

import org.aksw.sparqlmap.DockerHelper;
import org.aksw.sparqlmap.core.db.impl.MySQLConnector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spotify.docker.client.DockerException;

/**
 * Test using a mysql instance the test can completely mess up. Default setup is to test against a dockerized boot to docker setup.
 * 
 * e.g.:  docker run --rm -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=sparqlmaptest -e MYSQL_USER=sparqlmap -e MYSQL_PASSWORD=sparqlmap -p 3306:3306 mysql
 * @author joerg
 *
 */
public class SparqlMapQueryMySQLTest  extends SparqlMapQueryBaseTest{
  
  private static Logger log = LoggerFactory.getLogger(SparqlMapQueryMySQLTest.class);
  
  
  @BeforeClass
  public static void startMySQLDocker() throws DockerException, InterruptedException {
   // this approach will work for most dev setups
    try{
      dbconf =  DockerHelper.startMySQLDocker();
    } catch (Exception e) {
      log.error("FAiled to start Docker contaier", e);
      canConnect = false;
    }
  }

  @AfterClass
  public static void doTeardownHost() throws DockerException, InterruptedException {
    DockerHelper.stopDirectOrDockerizedMysql();
    
  }

  public SparqlMapQueryMySQLTest(String testname, String dsName,
      File mappingFile, File sqlFile, String query) {
    super(testname, dsName, mappingFile, sqlFile, query);
  }
  
  
  @Parameters(name="{0}")
  public static Collection<Object[]> dbdata(){
    return data(MySQLConnector.MYSQL_DBNAME);
  }
  
}
