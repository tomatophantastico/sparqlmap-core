package org.aksw.sparqlmap;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.aksw.sparqlmap.core.SparqlMap;
import org.aksw.sparqlmap.core.db.Connector;
import org.aksw.sparqlmap.core.db.DBAccessConfigurator;
import org.aksw.sparqlmap.core.db.impl.HSQLDBConnector;
import org.aksw.sparqlmap.core.spring.ContextSetup;
import org.apache.jena.atlas.logging.Log;
import org.hsqldb.Server;
import org.hsqldb.cmdline.SqlFile;
import org.junit.After;
import org.junit.Before;
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
  
  public static String hsqldbFileLocationprefix = "./build/hsqldbfiles/";
  
  private Server server;
  private SparqlMap r2r;
  private ApplicationContext con;


  @After
  public void close() {
    if(server !=null){
      server.shutdown();
    }
  }
  
  
  private String  hsqlFileLocation(){
    return hsqldbFileLocationprefix + this.dsName +"/db";
  }

  public void setupSparqlMap() {
    
    
    Properties props = getDBProperties();
    
    
    
    Map<String,Properties> name2props = new HashMap<String,Properties>();
    name2props.put("hsql-conf", props);
    
    con = ContextSetup.contextFromProperties(name2props);
    r2r = (SparqlMap) con.getBean("sparqlMap");

  }
  
  @Override
  public boolean initDb() {

    server = new Server();
    server.setSilent(false);
    server.setDatabaseName(0, "bsbm2-100k");
    server.setDatabasePath(0, "file:" + this.hsqlFileLocation());

    File hsqlFolder = new File(this.hsqlFileLocation() +".tmp");
    if (hsqlFolder.exists()) {
      log.info("Reusing existing dbfolder");
      server.start();
    } else {
      server.start();
      Connection conn = null;
      try {
        conn = getConnector().getConnection();
        SqlFile schemaSqlFile = new SqlFile(this.sqlFile);
        log.info("Loading sql file: " + sqlFile.getAbsolutePath() + " [" +this.hsqlFileLocation()+  "]");

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
    return true;

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
    props.put("jdbc.url","jdbc:hsqldb:file:" + this.hsqlFileLocation());
    props.put("jdbc.username","sa");
    props.put("jdbc.password","");
    
    return props;
  }
  
  
  @Override
  public SparqlMap getSparqlMap() {
    
    if(r2r==null){
      setupSparqlMap();
    }
    return r2r;
  }


  
  
  

  

}
