package org.aksw.sparqlmap.querytests;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.aksw.sparqlmap.DBHelper;
import org.aksw.sparqlmap.DockerHelper.DBConnConfig;
import org.aksw.sparqlmap.core.SparqlMap;
import org.aksw.sparqlmap.core.db.Connector;
import org.aksw.sparqlmap.core.db.DBAccessConfigurator;
import org.aksw.sparqlmap.core.db.impl.HSQLDBConnector;
import org.aksw.sparqlmap.core.spring.ContextSetup;
import org.apache.jena.atlas.logging.Log;
import org.hsqldb.Server;
import org.hsqldb.cmdline.SqlFile;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.jolbox.bonecp.BoneCPDataSource;



public class SparqlMapQueryHSQLTest extends SparqlMapQueryBaseTest{
  

  public SparqlMapQueryHSQLTest(String testname, String dsName,
      File mappingFile, File sqlFile, String query) {
    super(testname, dsName, mappingFile, sqlFile, query);
  }

  @Parameters(name="{0}")
  public static Collection<Object[]> dbdata(){
    return data(HSQLDBConnector.HSQLDB_NAME);
  }


  private static Logger log = LoggerFactory.getLogger(SparqlMapQueryHSQLTest.class);
  

  
  
  
  private static Server server;
  private SparqlMap r2r;
  private ApplicationContext con;


  @After
  public void close() {
    if(server !=null){
      server.shutdown();
    }
  }
  
  
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
  
  
  
  
  

  public void setupSparqlMap() {
    
    
    Properties props = getDBProperties();
    
    
    
    Map<String,Properties> name2props = new HashMap<String,Properties>();
    name2props.put("hsql-conf", props);
    
    con = ContextSetup.contextFromProperties(name2props);
    r2r = (SparqlMap) con.getBean("sparqlMap");

  }
  
 

  public Connector getConnector() {
    BoneCPDataSource ds = new BoneCPDataSource(
        DBAccessConfigurator.createConfig(
            getDBProperties().getProperty("jdbc.url"), getDBProperties().getProperty("jdbc.username"), getDBProperties().getProperty("jdbc.password"), 1, 2));
    
    
    HSQLDBConnector conn = new HSQLDBConnector();
    conn.setDs(ds);
    return conn;

  }


  
  
  @Override
  public SparqlMap getSparqlMap() {
    
    if(r2r==null){
      setupSparqlMap();
    }
    return r2r;
  }


  
  
  

  

}
