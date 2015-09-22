package org.aksw.sparqlmap;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.aksw.sparqlmap.core.SparqlMap;
import org.aksw.sparqlmap.core.db.impl.MySQLConnector;
import org.aksw.sparqlmap.core.spring.ContextSetup;
import org.junit.Before;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.context.ApplicationContext;

/**
 * Test using a mysql instance the test can completely mess up. Default setup is to test against a dockerized boot to docker setup.
 * 
 * e.g.:  docker run -it -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=sparqlmaptest -e MYSQL_USER=sparqlmap MYSQL_PASSWORD=sparqlmap -d mysql
 * 
 * @author joerg
 *
 */
public class SparqlMapQueryMySQLTest  extends SparqlMapQueryBaseTest{
  
  
  public SparqlMapQueryMySQLTest(String testname, String dsName,
      File mappingFile, File sqlFile, String query) {
    super(testname, dsName, mappingFile, sqlFile, query);
  }
  
  
  @Parameters(name="{0}")
  public static Collection<Object[]> dbdata(){
    return data(MySQLConnector.MYSQL_DBNAME);
  }
  
  
  private String jdbcString = "jdbc:mysql://192.168.59.103:3306/sparqlmaptest?padCharsWithSpace=true&sessionVariables=sql_mode='ANSI_QUOTES'";
  private String username = "sparqlmap";
  private String password = "sparqlmap";
  
  
  @Override
  public boolean canConnect() {
    try {
      getConnection().close();
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      return false;
    }
    
    
    
    
    return true;
  }
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
  
  @Before
  public void initDB() throws SQLException{
    Connection conn = getConnection();

    DBHelper.flushDb(conn);
    DBHelper.loadSqlFile(conn, sqlFile.getAbsolutePath());
    
    
    

    conn.close();
  }
  
  


  private Connection getConnection() throws SQLException {

    return DriverManager.getConnection(jdbcString,username,password);
  }
  
  
  
  
  
  
  
  

}
