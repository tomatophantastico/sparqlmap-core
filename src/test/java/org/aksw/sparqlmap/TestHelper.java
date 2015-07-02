package org.aksw.sparqlmap;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.sql.SQLException;
import java.util.Iterator;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.SparqlMap;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.resultset.ResultSetCompare;
import com.hp.hpl.jena.sparql.resultset.ResultsFormat;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.TDBLoader;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.sys.TDBMaker;

public class TestHelper {
  
  private static Logger log = LoggerFactory.getLogger(TestHelper.class);
  
  public void executeAndCompareConstruct(SparqlMap sm, String sparqlConstruct, String resultmodelLocation) throws SQLException{
    Model expectedResult = ModelFactory.createDefaultModel();
    expectedResult.read(resultmodelLocation);
    Model result = sm.executeConstruct(sparqlConstruct);
    
    assertModelAreEqual(result, expectedResult);
  }
  
  static public void assertModelAreEqual( Model result, Model expectedresult) throws SQLException{
    
  
    StringBuffer models =new StringBuffer();
    
    models.append("Actual result is :\n");
    models.append("=============================");
    ByteArrayOutputStream actualResBos  = new ByteArrayOutputStream();
    RDFDataMgr.write(actualResBos, result,Lang.TURTLE);
    models.append(actualResBos);
    
    models.append("=======================\nExpected was: ");
    ByteArrayOutputStream expectedResBos  =new ByteArrayOutputStream();
    RDFDataMgr.write(expectedResBos, expectedresult,Lang.TURTLE);
    models.append(expectedResBos);
    models.append("=============================");
    
    assertTrue(models.toString(), result.isIsomorphicWith(expectedresult));
  
    
  }
  
 

  public static void assertResultSetsAreEqual(ResultSet result,
      ResultSet expectedRS) {
    result = ResultSetFactory.makeRewindable(result); 
    boolean isEqual  = ResultSetCompare.equalsByTerm(result, expectedRS);
    StringBuffer comparison =new StringBuffer();

    if(!isEqual){
      
      comparison.append("Actual result is :\n");
      comparison.append("=============================");
     
      comparison.append(ResultSetFormatter.asText(result));
      comparison.append("=======================\nExpected was: ");
     
      comparison.append(ResultSetFormatter.asText(expectedRS));
      comparison.append("=============================");
      
    }
    
    assertTrue(comparison.toString(),isEqual);
    
  }

  public static void executeAndCompare(SparqlMap sm, String sparql,

      String tbdname, String queryname) throws SQLException {
    
    String tdbDir = "./build/tdbs/";
    
    
      
      File tdbDirFile = new File(tdbDir);
      Dataset refDs;
      if(!tdbDirFile.exists()){
        tdbDirFile.mkdirs();
        
        refDs = TDBFactory.createDataset(tdbDir);
        DatasetGraph refDsg = refDs.asDatasetGraph();
        
        DatasetGraph dump = sm.dump();
        Iterator<Quad> dumpIter = dump.find();
       
       
        while(dumpIter.hasNext()){
          Quad quad = dumpIter.next();
          refDsg.add(quad);
         
        }
        refDsg.close();
      }
      refDs = TDBFactory.createDataset(tdbDir);
       
      
      
      if(refDs.asDatasetGraph().isEmpty()){
        log.warn("Loaded empty dataset");
      }else{
        log.info("Loaded dataset of size:" + refDs.asDatasetGraph().size());
      }
      
      Query query = QueryFactory.create(sparql);
      
      if(query.isSelectType()){
        ResultSet expected = QueryExecutionFactory.create(query,refDs).execSelect();
        ResultSet acutal  = sm.executeSelect(sparql);
        
        assertResultSetsAreEqual(acutal, expected);
        
        
      }else if(query.isAskType()){
        throw new ImplementationException("implement ASK");
        //TODO implement ask task
//        assertTrue(
//            QueryExecutionFactory.create(query, refDs).execAsk()==
//            false
//            ); 
        
      }else if (query.isConstructType()){
        //construct query
        assertModelAreEqual(
            sm.executeConstruct(sparql), 
            QueryExecutionFactory.create(query,refDs).execConstruct());
        
      }else {
        //must be describe
        assertModelAreEqual(
            sm.executeDescribe(sparql), 
            QueryExecutionFactory.create(query,refDs).execDescribe());
        
      }
      
     
      
 
        
 
    
  }

}
