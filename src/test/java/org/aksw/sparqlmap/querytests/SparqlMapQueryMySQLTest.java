package org.aksw.sparqlmap.querytests;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import jersey.repackaged.com.google.common.collect.Sets;

import org.aksw.sparqlmap.DockerHelper;
import org.aksw.sparqlmap.TestHelper;
import org.aksw.sparqlmap.core.SparqlMap;
import org.aksw.sparqlmap.core.db.impl.MySQLConnector;
import org.aksw.sparqlmap.core.spring.ContextSetup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.context.ApplicationContext;

import com.google.common.collect.Lists;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerCertificateException;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerClient.ListContainersParam;
import com.spotify.docker.client.DockerClient.ListImagesFilterParam;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.Image;
import com.spotify.docker.client.messages.PortBinding;

/**
 * Test using a mysql instance the test can completely mess up. Default setup is to test against a dockerized boot to docker setup.
 * 
 * e.g.:  docker run --rm -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=sparqlmaptest -e MYSQL_USER=sparqlmap -e MYSQL_PASSWORD=sparqlmap -p 3306:3306 mysql
 * @author joerg
 *
 */
public class SparqlMapQueryMySQLTest  extends SparqlMapQueryBaseTest{
  

  
  @BeforeClass
  public static void startMySQLDocker() throws DockerException, InterruptedException {
   // this approach will work for most dev setups
      dbconf =  DockerHelper.startMySQLDocker();
  }

  @AfterClass
  public static void doTeardownHost() throws DockerException, InterruptedException {
    DockerHelper.stopMySQLDocker();
    
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
