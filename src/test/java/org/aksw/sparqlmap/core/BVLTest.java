package org.aksw.sparqlmap.core;

import static org.junit.Assert.*;

import org.apache.jena.sparql.core.DatasetGraph;
import org.junit.Test;

public class BVLTest {
  
  
  @Test
  public void testDirectMapping(){
    
    SparqlMap sm = SparqlMapBuilder.newSparqlMap("http://example.org/test/").connectToCsv("./src/test/resources/bvl/le-online-extracted-places.csv").mappedByDefaultMapping().create();
    
    DatasetGraph dsg =  sm.getDumpExecution().dumpDatasetGraph();
    
    dsg.getDefaultGraph().find(null, null, null).forEachRemaining(System.out::println);
    
    
  }

}
