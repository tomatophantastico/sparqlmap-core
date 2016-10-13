package org.aksw.sparqlmap.querytests;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.aksw.sparqlmap.DBHelper;
import org.aksw.sparqlmap.DockerHelper.DBConnConfig;
import org.aksw.sparqlmap.TestHelper;
import org.aksw.sparqlmap.core.SparqlMap;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public abstract class QueryBaseTest {
  
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
  
  
  static Boolean canConnect;
  
  private static Logger log = LoggerFactory.getLogger(QueryBaseTest.class);
  
  
  private static Properties fails;
  
  {
      fails = new Properties();
        try {
      InputStream stream = 
                 ClassLoader.getSystemClassLoader().getResourceAsStream("query_failing.properties");
         fails.load(stream);

            stream.close();
        } catch (IOException e) {
            log.error("Problem loading failing test information",e);
        }
  }
  
  public QueryBaseTest(String testname, String dsName,
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
    
    initContext();
    
    String key = testname;
  
   
    
    if(canConnect == null){
      canConnect = DBHelper.waitAndConnect(sparqlMap.getDataContext());
    }
    
    
    
    
    Assume.assumeTrue("Cannot connect to the database", canConnect);  
    
    Assume.assumeTrue("Failure loading the database", populateDB());
    
    initContext();
    
    if(fails.containsKey(key)){
      String value = fails.getProperty(key);
      String dbs = value.split(":")[0];
//      Assume.assumeFalse(value.split(":")[1], 
//              dbs.equals("ALL")
//              || Lists.newArrayList(dbs.split(",")).contains(getDataTypeHelper().getDBName().toLowerCase()));

     }
    
    
    
    
    sparqlMap = this.sparqlMap;
    TestHelper.executeAndCompare(sparqlMap, query, this.dsName, testname);
        
  }
  
  
  public abstract boolean populateDB();
  
  
 

  
  public abstract void initContext();
  



  
  
  


  @After
  public void close(){
    if(sparqlMap!=null){
      sparqlMap.close();
    }
  }
  

  
  

}
