package org.aksw.sparqlmap.core;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.sql.DataSource;

import org.aksw.sparqlmap.core.automapper.MappingGenerator;
import org.aksw.sparqlmap.core.r2rml.R2RMLMapping;
import org.aksw.sparqlmap.core.r2rml.R2RMLModelLoader;
import org.aksw.sparqlmap.core.r2rml.R2RMLValidationException;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.util.FileManager;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.csv.CsvDataContext;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.mongodb.mongo3.MongoDbDataContext;
import org.eobjects.metamodel.access.AccessDataContext;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

/**
 * Create SparqlMap contexts with this Factory!
 * 
 * 
 * @author joerg
 *
 */
public class SparqlMapFactory {
  
  private String baseIri;
  
  private SparqlMap sparqlMap;
  
  public static SparqlMapFactory  newSparqlMap(){
    String genIri;
    try {
      genIri = "http://" + InetAddress.getLocalHost().getHostName() + "/baseiri/";
    } catch (UnknownHostException e) {
      genIri = "http://localhost /baseiri/";
    }
    return new SparqlMapFactory(genIri);
    
  }
  
  
  public static SparqlMapFactory newSparqlMap(String baseIRI){
    return new SparqlMapFactory(baseIRI);

  }
  
  
  public SparqlMapFactory(String baseIri) {
    sparqlMap = new SparqlMap();
    this.baseIri = baseIri;
  }
  

  public SparqlMapFactory connectTo(DataContext dcon){
    sparqlMap.setDataContext(dcon);
    
    return this;
    
  }
  
  
  public SparqlMapMappingFactory connectToCsv(String location){
    UpdateableDataContext dc = new CsvDataContext(new File(location));
    
    sparqlMap.setDataContext(dc);
    
    return new SparqlMapMappingFactory();
  }
  
  public SparqlMapMappingFactory connectToAccess(String location){
    DataContext dc = new AccessDataContext(new File(location));
    
    sparqlMap.setDataContext(dc);
    
    return new SparqlMapMappingFactory();
  }
  
  
  public SparqlMapMappingFactory connectToMongoDb3(String host, String dbname){
    MongoClient client = new MongoClient(host);
    
    MongoDbDataContext m3dbdc = new MongoDbDataContext(client.getDatabase(dbname));
    sparqlMap.setDataContext(m3dbdc);
    sparqlMap.setCloseable(client);
    
    return new SparqlMapMappingFactory();
    
  }
  
  
  public SparqlMapMappingFactory connectToMongoDb3(MongoDatabase mdb){
    sparqlMap.setDataContext(new MongoDbDataContext(mdb));
    return new SparqlMapMappingFactory();
  }
  
  
  public SparqlMapMappingFactory connectJdbcBackend(DataSource ds){
    JdbcDataContext context = new JdbcDataContext(ds);
    
    
    sparqlMap.setDataContext(context);
    
    return new SparqlMapMappingFactory();
  }
  
  
  public class SparqlMapMappingFactory{
    

    
    public SparqlMapMappingFactory mappedByDefaultMapping(){
      
      return mappedByDefaultMapping(baseIri);
      
    }
   
    
    public SparqlMapMappingFactory mappedByDefaultMapping(String prefix){
      MappingGenerator gen = new MappingGenerator(prefix);
      Model mapping = gen.generateMapping(sparqlMap.getDataContext().getDefaultSchema());
      sparqlMap.setMapping(loadMapping(mapping));

      return this;
    }
    
    
    
    
    
    
    public SparqlMapMappingFactory mappedBy(String location){
      Model r2rmlModel = RDFDataMgr.loadModel(location);
          
      sparqlMap.setMapping(loadMapping(r2rmlModel));
      
      return this;
    }
    
    public SparqlMapMappingFactory mappedBy(Model model){
      
      sparqlMap.setMapping(loadMapping(model));

      return this;
    }
    
    public SparqlMap create(){
      
      //validate, that both the mapping is set and a db connection is present.
      
      
      if(sparqlMap.getMapping()==null){
        throw new SystemInitializationError("No mapping loaded");
      }
      
    
      if(sparqlMap.getContextConf()==null){
        sparqlMap.setContextConf(new ContextConfiguration());
      }
      
      return sparqlMap;
    }
    
    
    
    
    private R2RMLMapping loadMapping(Model model){
      Model r2rmlspec = ModelFactory.createDefaultModel();
      
      FileManager.get().readModel(r2rmlspec, "vocabularies/r2rml.ttl");
      
      R2RMLMapping r2rmlMappig =  R2RMLModelLoader.loadModel(model, r2rmlspec,baseIri);
      
      return r2rmlMappig;
      
      
    }
   
    
  }
  
  
  

}
