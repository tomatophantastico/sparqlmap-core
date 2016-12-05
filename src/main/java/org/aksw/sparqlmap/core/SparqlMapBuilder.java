package org.aksw.sparqlmap.core;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

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

import com.google.common.base.Joiner;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

/**
 * Create SparqlMap contexts with this Factory!
 * 
 * 
 * @author joerg
 *
 */
public class SparqlMapBuilder {
  
  private String baseIri;
  
  private SparqlMap sparqlMap;
  

  
  
  public static SparqlMapBuilder newSparqlMap(String baseIRI){
    if(baseIRI==null){
      try {
        baseIRI = "http://" + InetAddress.getLocalHost().getHostName() + "/baseiri/";
      } catch (UnknownHostException e) {
        baseIRI = "http://localhost/baseiri/";
      }
    }
    
    return new SparqlMapBuilder(baseIRI);

  }
  
  
  public SparqlMapBuilder(String baseIri) {
    sparqlMap = new SparqlMap();
    this.baseIri = baseIri;
  }
  

  public SparqlMapBuilder connectTo(DataContext dcon){
    sparqlMap.setDataContext(dcon);
    
    return this;
    
  }
  
  
  public SparqlMapMappingBuilder connectToCsv(String location){
    UpdateableDataContext dc = new CsvDataContext(new File(location));
    
    sparqlMap.setDataContext(dc);
    
    return new SparqlMapMappingBuilder();
  }
  
  public SparqlMapMappingBuilder connectToAccess(String location){
    DataContext dc = new AccessDataContext(new File(location));
    
    sparqlMap.setDataContext(dc);
    
    return new SparqlMapMappingBuilder();
  }
  
  
  public SparqlMapMappingBuilder connectToMongoDb3(String host, String dbname){
    return connectToMongoDb3(host, dbname, null, null);
    
  }
  
  public SparqlMapMappingBuilder connectToMongoDb3(String host, String dbname, String username, String password){
    
    
    MongoClient client = null;
    
    if(username==null){
      client  =  new MongoClient(host);
    }else{
      MongoCredential mcred = MongoCredential.createCredential(username, dbname, password.toCharArray());
      ServerAddress sAddr = new ServerAddress(host);
      client = new MongoClient(sAddr,Arrays.asList(mcred));
    }
    
   
    
   
    
    MongoDbDataContext m3dbdc = new MongoDbDataContext(client.getDatabase(dbname));
    sparqlMap.setDataContext(m3dbdc);
    sparqlMap.setCloseable(client);
    
    return new SparqlMapMappingBuilder();
    
  }
  
  
  public SparqlMapMappingBuilder connectToMongoDb3(MongoDatabase mdb){
    sparqlMap.setDataContext(new MongoDbDataContext(mdb));
    return new SparqlMapMappingBuilder();
  }
  
  
  public SparqlMapMappingBuilder connectJdbcBackend(DataSource ds){
    JdbcDataContext context = new JdbcDataContext(ds);
    
    
    sparqlMap.setDataContext(context);
    
    return new SparqlMapMappingBuilder();
  }
  
  
  public class SparqlMapMappingBuilder{
    

    
    public SparqlMapMappingBuilder mappedByDefaultMapping(){
      
      return mappedByDefaultMapping(baseIri);
      
    }
   
    
    public SparqlMapMappingBuilder mappedByDefaultMapping(String prefix){
      MappingGenerator gen = new MappingGenerator(prefix);
      Model mapping = gen.generateMapping(sparqlMap.getDataContext().getDefaultSchema());
      sparqlMap.setMapping(loadMapping(mapping));

      return this;
    }
    
    public SparqlMapMappingBuilder mappedByDefaultMapping(String prefix, String mappingPrefix,
      String instancePrefix, String vocabularyPrefix, String primaryKeySeparator){
    
      MappingGenerator gen = new MappingGenerator(prefix,mappingPrefix,instancePrefix,vocabularyPrefix,primaryKeySeparator,null);
      Model mapping = gen.generateMapping(sparqlMap.getDataContext().getDefaultSchema());

      sparqlMap.setMapping(loadMapping(mapping));
      return this;
    }
    
    
    
    
    
    
    public SparqlMapMappingBuilder mappedBy(String location){
      Model r2rmlModel = RDFDataMgr.loadModel(location);
          
      sparqlMap.setMapping(loadMapping(r2rmlModel));
      
      return this;
    }
    
    public SparqlMapMappingBuilder mappedBy(Model model){
      
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
      
      
      List<String> warnins = sparqlMap.validateMapping();
      
      if(!warnins.isEmpty()){
        throw new SystemInitializationError(Joiner.on(System.lineSeparator()).join(warnins));
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
