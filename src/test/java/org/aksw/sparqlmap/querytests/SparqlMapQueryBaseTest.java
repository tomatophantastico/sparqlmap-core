package org.aksw.sparqlmap.querytests;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.aksw.sparqlmap.DBHelper;
import org.aksw.sparqlmap.DockerHelper.DBConnConfig;
import org.aksw.sparqlmap.TestHelper;
import org.aksw.sparqlmap.core.SparqlMap;
import org.aksw.sparqlmap.core.spring.ContextSetup;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.context.ApplicationContext;

import com.google.common.collect.Lists;
import com.google.common.io.PatternFilenameFilter;

/**
 * This class is loads data set definitions and queries 
 * from a folder/file structure and compares the result 
 * of the translated query with the result executed over a materialized dump.
 * 
 * @author Joerg Unbehauen 
 *
 */
@RunWith(value = Parameterized.class)
public abstract class SparqlMapQueryBaseTest {
  
  /**
   * name of the query
   */
  String testname;
  /**
   * name of the dataset
   */
  String dsName;
  File mappingFile;  
  File sqlFile;
  String query;
  
  SparqlMap sparqlMap;
  
  static DBConnConfig dbconf;
  static Boolean canConnect;
  public SparqlMapQueryBaseTest(String testname, String dsName,
      File mappingFile, File sqlFile, String query) {
    super();
    this.testname = testname;
    this.dsName = dsName;
    this.mappingFile = mappingFile;
    this.sqlFile = sqlFile;
    this.query = query;
  }

  //TODO optional: read drom classpath
  static final File testRootFolder = new File("./src/test/resources/query-test/");

  
  public static Collection<Object[]> data(String dbname) {
    Collection<Object[]> testCases = new ArrayList<Object[]>();
    
    for(File dataSetFolder : testRootFolder.listFiles()){
      String dsname = dataSetFolder.getName();
      
      if(dataSetFolder.isDirectory()){
        File mappingFile = new File(dataSetFolder,"mapping.ttl");
 
        
        File sqlFile = new File(dataSetFolder,"dataset-"+dbname.toLowerCase()+".sql");
        
        if(!sqlFile.exists()){
          sqlFile = new File(dataSetFolder,"dataset.sql");
        }
        
        FilenameFilter dotSparqlFiles = new PatternFilenameFilter("^.*\\.sparql$");
        for(File sparqlFile: Lists.newArrayList(new File(dataSetFolder,"queries").listFiles(dotSparqlFiles))) {
          try {
            String testname = dataSetFolder.getName() + "/" + sparqlFile.getName();
            String query = new String(Files.readAllBytes(sparqlFile.toPath()));
            testCases.add(new Object[]{testname,dsname,mappingFile,sqlFile,query});
          } catch (IOException e) {
            throw new RuntimeException(e);           
            }
        }
      }  
    }

    return testCases;
  }
  
  @Test
  public void runTest() throws SQLException{
    
    if(canConnect ==null){
      canConnect = DBHelper.waitAndConnect(dbconf);
    }
    
    Assume.assumeTrue("Cannot connect to the database", canConnect);  
    
    boolean initResult = DBHelper.initDb(dbconf,dsName,sqlFile);
    Assume.assumeTrue("Could not initialize database ",initResult);
    
    if(query.startsWith("#fails:")){
      String message = query.substring(0,query.indexOf("\n"));
      Assume.assumeTrue(message, false);
    }
    
    
    sparqlMap = getSparqlMap();
    TestHelper.executeAndCompare(sparqlMap, query, this.dsName, testname);
    
  }
  
 
  public SparqlMap getSparqlMap() {
    
    Properties props = getDBProperties();
    
    
    
    Map<String,Properties> name2props = new HashMap<String,Properties>();
    name2props.put("test-conf", props);
    
    ApplicationContext con =  ContextSetup.contextFromProperties(name2props);
    return (SparqlMap) con.getBean("sparqlMap");
  }


  
  
  


  @After
  public void close(){
    if(sparqlMap!=null){
      sparqlMap.close();
    }
  }
  
  
  public Properties getDBProperties() {
    Properties props = new Properties();
    
    props.put("sm.mappingfile", this.mappingFile.getPath());

    // replicating the values from initDatabase
    props.put("jdbc.url",dbconf.jdbcString);
    props.put("jdbc.username",dbconf.username);
    props.put("jdbc.password",dbconf.password);
    
    return props;
  }
  
  
  

}
