package org.aksw.sparqlmap.core.r2rml;

import static org.junit.Assert.*;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.Test;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class R2RMLModelLoaderTest {

  @Test
  public void testLoaderWithBSBMFull() {
    
    String bsbmPrefix = "http://aksw.org/Projects/sparqlmap/mappings/bsbm-test/";
    
    Model toLoad = ModelFactory.createDefaultModel();
    RDFDataMgr.read(toLoad, ClassLoader.getSystemResourceAsStream("./query-test/bsbm-all/mapping.ttl"), null, Lang.TURTLE);


    Model r2rmlspec = ModelFactory.createDefaultModel();
    r2rmlspec.read(ClassLoader.getSystemResourceAsStream("./vocabularies/r2rml.ttl"),null,"TTL");
    
    R2RMLMapping mapping =  R2RMLModelLoader.loadModel(toLoad, r2rmlspec);
    
    
    
    JDBCMapping jMapping = 
  
    mapping.getQuadMaps();
    
    
    QuadMap product_fkProducer = mapping.getQuadMaps().get(bsbmPrefix+"Product_fkProducer").iterator().next();
    
  }

}
