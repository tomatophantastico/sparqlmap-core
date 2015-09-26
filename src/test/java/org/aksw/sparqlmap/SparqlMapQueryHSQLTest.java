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
import org.hsqldb.Server;
import org.hsqldb.cmdline.SqlFile;
import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized.Parameters;
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



  public static String hsqldbFileLocationprefix = "./build/hsqldbfiles/";
  
  private Server server;
  private SparqlMap r2r;
  private ApplicationContext con;


  @After
  public void close() {

    server.shutdown();
  }
  
  
  private String  hsqlFileLocation(){
    return hsqldbFileLocationprefix + this.dsName +"/db";
  }

  @Before
  public void setupSparqlMap() {
    
    
    Properties props = getDBProperties();
    
    
    
    Map<String,Properties> name2props = new HashMap<String,Properties>();
    name2props.put("hsql-conf", props);
    
    con = ContextSetup.contextFromProperties(name2props);
    r2r = (SparqlMap) con.getBean("sparqlMap");

  }

  public boolean initDb() {

    server = new Server();
    server.setSilent(true);
    server.setDatabaseName(0, "bsbm2-100k");
    server.setDatabasePath(0, "file:" + this.hsqlFileLocation());

    File hsqlFolder = new File(this.hsqlFileLocation() +".tmp");
    if (hsqlFolder.exists()) {
      server.start();
    } else {
      server.start();
      Connection conn = null;
      try {
        conn = getConnector().getConnection();
        SqlFile schemaSqlFile = new SqlFile(this.sqlFile);
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
    return r2r;
  }


  
  
  

  

}
