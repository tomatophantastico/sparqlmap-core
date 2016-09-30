package org.aksw.sparqlmap.core;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.r2rml.R2RMLMapping;
import org.aksw.sparqlmap.core.r2rml.R2RMLModelLoader;
import org.aksw.sparqlmap.core.spring.DBAccessConfigurator;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.util.FileManager;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.mongodb.mongo3.MongoDbDataContext;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
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
  
  public static SparqlMapFactory  create(){
    
    return new SparqlMapFactory();
    
  }
  
  public SparqlMapFactory() {
    sparqlMap = new SparqlMap();
    try {
      baseIri = "http://" + InetAddress.getLocalHost().getHostName() + "/baseiri/";
    } catch (UnknownHostException e) {
      baseIri = "http://localhost /baseiri/";
    }
  }
  
  
  
  public SparqlMapFactory connectToRelational(String user, String password, String jdbcUrl){
    
    BoneCPConfig bcpconf = new BoneCPConfig();
    bcpconf.setJdbcUrl(jdbcUrl);
    bcpconf.setUsername(user);
    bcpconf.setPassword(password);
    
    BoneCPDataSource dataSource = new BoneCPDataSource(bcpconf);
    
    
    JdbcDataContext context = new JdbcDataContext(dataSource);
    
    sparqlMap.setDataContext(context);
    return this;
  }
  
  public SparqlMapFactory connectToMongoDb3(String host, String dbname){
    MongoClient client = new MongoClient(host);
    
    MongoDbDataContext m3dbdc = new MongoDbDataContext(client.getDatabase(dbname));
    sparqlMap.setDataContext(m3dbdc);
    
    return this;
    
  }
  
  
  public SparqlMapFactory connectToMongoDb3(MongoDatabase mdb){
    sparqlMap.setDataContext(new MongoDbDataContext(mdb));
    return this;
  }
  
  
  public SparqlMapFactory connectJdbcBackend(DataSource ds){
    JdbcDataContext context = new JdbcDataContext(ds);
    
    
    sparqlMap.setDataContext(context);
    
    return this;
    
    

  }
  
  
  
  public SparqlMapFactory connectJDBCBackend(String user, String password, String jdbcUrl, int poolminconnections, int poolmaxconnections ) throws SQLException{
    
    DBAccess dba = DBAccessConfigurator.getDBAccess(jdbcUrl, user, password, poolminconnections, poolmaxconnections);
    sparqlMap.setSqlAccess(dba);
    
    return this;
  }
  
  
  
  
  
  public SparqlMapFactory mappedBy(String location){
    Model r2rmlModel = RDFDataMgr.loadModel(location);
        
    sparqlMap.setMapping(loadMapping(r2rmlModel));
    
    return this;
  }
  
  public SparqlMapFactory mappedBy(Model model){
    
    sparqlMap.setMapping(loadMapping(model));

    return this;
  }
  
  public SparqlMap finish(){
    
    //validate, that both the mapping is set and a db connection is present.
    
    
    if(sparqlMap.getMapping()==null){
      throw new SystemInitializationError("No mapping loaded");
    }
    
    if(!(sparqlMap.getJdbcAccess()==null^sparqlMap.getDataContext()==null)){
      throw new SystemInitializationError("Provide exactly one Backend");
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
