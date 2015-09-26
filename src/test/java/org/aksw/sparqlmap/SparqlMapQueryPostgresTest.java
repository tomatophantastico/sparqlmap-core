package org.aksw.sparqlmap;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.aksw.sparqlmap.core.SparqlMap;
import org.aksw.sparqlmap.core.db.impl.MySQLConnector;
import org.aksw.sparqlmap.core.db.impl.PostgreSQLConnector;
import org.aksw.sparqlmap.core.spring.ContextSetup;
import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.context.ApplicationContext;

import com.mysql.jdbc.Statement;

/**
 * Test using a postgresql instance the test can completely mess up. Default setup is to test against a dockerized boot to docker setup.
 * 
 * e.g.:  docker run --rm -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres
 * @author joerg
 *
 */
public class SparqlMapQueryPostgresTest  extends SparqlMapQueryBaseTest{
  
  
  public SparqlMapQueryPostgresTest(String testname, String dsName,
      File mappingFile, File sqlFile, String query) {
    super(testname, dsName, mappingFile, sqlFile, query);
  }
  
  
  @Parameters(name="{0}")
  public static Collection<Object[]> dbdata(){
    return data(PostgreSQLConnector.POSTGRES_DBNAME);
  }
  
  
  private String jdbcString = "jdbc:postgresql://192.168.59.103:5432/postgres";
  private String username = "postgres";
  private String password = "postgres";
  

  @Override
  public SparqlMap getSparqlMap() {
    
    Properties props = getDBProperties();
    
    
    
    Map<String,Properties> name2props = new HashMap<String,Properties>();
    name2props.put("mysql-conf", props);
    
    ApplicationContext con =  ContextSetup.contextFromProperties(name2props);
    return (SparqlMap) con.getBean("sparqlMap");
  }
  
  public Properties getDBProperties() {
    Properties props = new Properties();
    
    props.put("sm.mappingfile", this.mappingFile.getPath());

    // replicating the values from initDatabase
    props.put("jdbc.url",jdbcString);
    props.put("jdbc.username",username);
    props.put("jdbc.password",password);
    
    return props;
  }
  

  @Override
  public boolean initDb() {
    String tablename = "sparqlmaptest_" + this.dsName;
    String queryForTestTable = String.format("select 1 from \"%s\" limit 1;",
        tablename);

    try (Connection conn = getConnection();
        java.sql.Statement stmt = conn.createStatement();) {
      boolean createNew = false;
      try (

          ResultSet rs = stmt.executeQuery(queryForTestTable);) {
        
       

        } catch (SQLException e) {
          createNew = true;
        }
      
      if (createNew) {
        DBHelper.flushDb(conn);
        DBHelper.loadSqlFile(conn, sqlFile.getAbsolutePath());
        try (java.sql.Statement stmtInsert = conn.createStatement();) {
          stmtInsert.execute(String.format("Create table %s (id int);", tablename));

        }
      }
    } catch (SQLException e1) {
      e1.printStackTrace();
      return false;
    }
    return true;
  }
  
  


  private Connection getConnection() throws SQLException {

    return DriverManager.getConnection(jdbcString,username,password);
  }



  
  
  
  
  
  

}
