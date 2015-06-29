package org.aksw.sparqlmap;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.aksw.sparqlmap.bsbmtestcases.BSBMHSQLDBTest;
import org.aksw.sparqlmap.core.SparqlMap;
import org.aksw.sparqlmap.core.db.Connector;
import org.aksw.sparqlmap.core.db.DBAccessConfigurator;
import org.aksw.sparqlmap.core.db.impl.HSQLDBConnector;
import org.aksw.sparqlmap.core.spring.ContextSetup;
import org.hsqldb.Server;
import org.hsqldb.cmdline.SqlFile;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.google.common.io.PatternFilenameFilter;
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



  public static String hsqldbFileLocation = "./target/hsqldbfiles/db";
  private Server server;
  private SparqlMap r2r;
  private ApplicationContext con;

  @Before
  public void init() {
    initDatabase();
    setupSparqlMap();

  }

  @After
  public void close() {
//    try {
//      Thread.sleep(3600000);
//    } catch (InterruptedException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
    server.shutdown();
  }

  public void setupSparqlMap() {
    
    
    Properties props = getDBProperties();
    
    
    
    Map<String,Properties> name2props = new HashMap<String,Properties>();
    name2props.put("hsql-conf", props);
    
    con = ContextSetup.contextFromProperties(name2props);
    r2r = (SparqlMap) con.getBean("sparqlMap");

  }

  public void initDatabase() {

    server = new Server();
    server.setSilent(true);
    server.setDatabaseName(0, "bsbm2-100k");
    server.setDatabasePath(0, "file:" + hsqldbFileLocation);

    File hsqlFolder = new File(hsqldbFileLocation +".tmp");
    if (hsqlFolder.exists()) {
      server.start();
    } else {
      server.start();
      Connection conn = null;
      try {
        conn = getConnector().getConnection();
        SqlFile schemaSqlFile = new SqlFile((this.sqlFile));
        schemaSqlFile.setConnection(conn);
        schemaSqlFile.execute();
        conn.commit();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          if (conn != null) {
            conn.close();
          }

        } catch (SQLException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }

    }

  }

  public Connector getConnector() {
    BoneCPDataSource ds = new BoneCPDataSource(
        DBAccessConfigurator.createConfig(
            getDBProperties().getProperty("jdbc.url"), getDBProperties().getProperty("jdbc.username"), getDBProperties().getProperty("jdbc.password"), 1, 2));
    
    
    HSQLDBConnector conn = new HSQLDBConnector();
    conn.setDs(ds);
    return conn;

  }

  public Properties getDBProperties() {
    Properties props = new Properties();
    
    props.put("sm.mappingfile", this.mappingFile.getPath());

    // replicating the values from initDatabase
    props.put("jdbc.url","jdbc:hsqldb:file:" + hsqldbFileLocation);
    props.put("jdbc.username","sa");
    props.put("jdbc.password","");
    
    return props;
  }
  
  
  @Override
  public SparqlMap getSparqlMap() {
    return r2r;
  }

  
  
  
  

  

}
