package org.aksw.sparqlmap.r2rmltestcases;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

import javax.sql.DataSource;

import org.aksw.sparqlmap.DBHelper;
import org.aksw.sparqlmap.DockerHelper.DBConnConfig;
import org.aksw.sparqlmap.core.SparqlMap;
import org.aksw.sparqlmap.core.SparqlMapFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.hsqldb.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

public class HsqlR2RMLTest extends R2RMLTest {
  
  
  private static final Logger log = LoggerFactory.getLogger(HsqlR2RMLTest.class);
  
  public HsqlR2RMLTest(String testCaseName, String r2rmlLocation, String outputLocation, String referenceOutput,
      String dbFileLocation, boolean createDM) {
    super(testCaseName, r2rmlLocation, outputLocation, referenceOutput, dbFileLocation, createDM);
  }

  private static Server server;
  
  private SparqlMap sparqlMap;
  
  private DataContext dcon;
  

  
  HikariDataSource cp ;
  
  @Parameters(name="{0}")
  public static Collection<Object[]> dbdata(){
    File tcLocation = new File("./src/test/resources/testcases/hsqldb/");
    Assert.assertTrue(tcLocation.exists()&&tcLocation.isDirectory());
    return data(tcLocation);
  }


  @BeforeClass
  public static void startServer(){
    server = new Server();
    server.setSilent(false);
    server.setDatabaseName(0, "bsbm2-100k");
    server.setDatabasePath(0, "mem:sparqlmaptest\"");
    server.start();
  }
  
  @Before
  public void init(){
    cp = new HikariDataSource();
    cp.setJdbcUrl("jdbc:hsqldb:mem:sparqlmaptest/");
    cp.setUsername("sa");
    cp.setPassword("");
    dcon = new JdbcDataContext(cp);

    
    
  }
  @After
  public void stop(){
    cp.close();
  }
  
  
  @Override
  public  SparqlMap getSparqlMap() {
    synchronized (this) {
      if(sparqlMap==null){
        sparqlMap = SparqlMapFactory.newSparqlMap().connectJdbcBackend(cp).mappedBy(RDFDataMgr.loadModel(this.r2rmlLocation)).create();
      }
    }
   
    
    return sparqlMap;
  }


  @Override
  public String getDBName() {
    // TODO Auto-generated method stub
    return "HSQLDB";
  }


  @Override
  public void flushDatabase() throws ClassNotFoundException, SQLException {
   
      Connection conn = cp.getConnection();

      DBHelper.flushDb(conn);

      conn.close();
      
    }


  @Override
  DataContext getDatacontext() {
    return dcon;
  }


  @Override
  public void loadFileIntoDB(String file) throws ClassNotFoundException, SQLException, IOException {
    
    log.info(String.format("Loading %s into the database",file));
    
     Connection conn = cp.getConnection();
     DBHelper.loadSqlFile(conn, file);
     
     conn.close();

//    String sql2Execute = FileUtils.readFileToString(new File(file));
//    loadStringIntoDb(sql2Execute);

    
  }
    

  
  
}
