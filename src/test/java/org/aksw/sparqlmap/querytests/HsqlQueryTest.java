package org.aksw.sparqlmap.querytests;

import java.io.File;
import java.util.Collection;

import javax.sql.DataSource;

import org.aksw.sparqlmap.DBHelper;
import org.aksw.sparqlmap.DockerHelper.DBConnConfig;
import org.aksw.sparqlmap.core.SparqlMap;
import org.aksw.sparqlmap.core.SparqlMapFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.hsqldb.Server;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized.Parameters;

import com.zaxxer.hikari.HikariDataSource;

public class HsqlQueryTest extends QueryBaseTest {
  
  private static Server server;
  
  public HsqlQueryTest(String testname, String dsName, File mappingFile, File sqlFile, String query) {
    super(testname, dsName, mappingFile, sqlFile, query);
  }
  
  HikariDataSource cp ;
  
  @Parameters(name="{0}")
  public static Collection<Object[]> dbdata(){
    return data("hsqldb");
  }


  @BeforeClass
  public static void startServer(){
    server = new Server();
    server.setSilent(false);
    server.setDatabaseName(0, "bsbm2-100k");
    server.setDatabasePath(0, "mem:sparqlmaptest\"");
    server.start();
  }
  
  
  @Override
  public void initContext() {
    cp = new HikariDataSource();
    cp.setJdbcUrl("jdbc:hsqldb:mem:sparqlmaptest/");
    cp.setUsername("sa");
    cp.setPassword("");
   
    
    sparqlMap = SparqlMapFactory.create().connectJdbcBackend(cp).mappedBy(RDFDataMgr.loadModel(mappingFile.getAbsolutePath())).finish();
  }

  @Override
  public boolean populateDB() {
    return DBHelper.initDb(cp,dsName,sqlFile);
    
  }
  
  
}
