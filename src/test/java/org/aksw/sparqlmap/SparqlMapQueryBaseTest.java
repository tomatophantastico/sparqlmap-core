package org.aksw.sparqlmap;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;

import org.aksw.sparqlmap.core.SparqlMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import com.google.common.io.ByteProcessor;
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
 
        
        File sqlFile = new File(dataSetFolder,"dataset-"+dbname+".sql");
        
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
  public void runTest(){
    
    TestHelper.executeAndCompare(getSparqlMap(), query, this.dsName);
    
  }
  
  
  
  
  
  public abstract SparqlMap getSparqlMap();


}
