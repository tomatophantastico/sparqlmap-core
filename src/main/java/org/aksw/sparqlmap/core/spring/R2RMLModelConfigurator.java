package org.aksw.sparqlmap.core.spring;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.annotation.PostConstruct;

import org.aksw.sparqlmap.core.automapper.MappingGenerator;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.r2rml.R2RMLMapping;
import org.aksw.sparqlmap.core.r2rml.R2RMLModelLoader;
import org.aksw.sparqlmap.core.r2rml.R2RMLValidationException;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCMapping;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCMappingBinder;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.util.FileManager;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;

import net.sf.jsqlparser.JSQLParserException;
/**
 * This class deals with the loading and binding of the R2RML model.
 * 
 * @author joerg
 *
 */
@Configuration
public class R2RMLModelConfigurator {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(R2RMLModelConfigurator.class);
	
	private String mappingfile;
	private String r2rmlvocabmodel;
  private String baseUri;
  private String dmR2rmlDump;	
	

	@Autowired
	private DBAccess dbaccess;
	
	
	@Autowired
	private Environment env;

	@PostConstruct
	public void setValues(){
		mappingfile = env.getProperty("sm.mappingfile");
		r2rmlvocabmodel = env.getProperty("sm.r2rmlvocablocation");
    baseUri = env.getProperty("sm.baseuri");
    dmR2rmlDump = env.getProperty("sm.dmr2rmldump");
		
	}
	
	@Bean
	public JDBCMapping createModel() throws R2RMLValidationException, JSQLParserException, SQLException, IOException{
		
		Model schema = null;
		
		schema = getSchemaModel();
		Model mapping = null;
		mapping = getMappingModel();
		
		
		
		R2RMLMapping r2rmlmapping = R2RMLModelLoader.loadModel(mapping, schema,baseUri);
		
		JDBCMappingBinder jdbcBinder = new JDBCMappingBinder(r2rmlmapping, dbaccess);

		JDBCMapping model = jdbcBinder.bind();
		
		return model;
	}

  public Model getSchemaModel() throws IOException {
    Model schema;
    schema = ModelFactory.createDefaultModel();
		if(r2rmlvocabmodel!=null){
			
			if(r2rmlvocabmodel.startsWith("classpath:")){
				ClassPathResource vocab = new ClassPathResource(r2rmlvocabmodel.substring(10));
				schema.read(vocab.getInputStream(),null, "TTL");
				
			}else{
				FileManager.get().readModel(schema,r2rmlvocabmodel);
			}
			
		
		}else{
			FileManager.get().readModel(schema,"vocabularies/r2rml.ttl");
		}
    return schema;
  }

  public Model getMappingModel() throws FileNotFoundException, SQLException, UnsupportedEncodingException {
    Model mapping;
    mapping = ModelFactory.createDefaultModel();

		if(mappingfile!=null){
			log.info("Loading mapping " + mappingfile);
			
			RDFDataMgr.read(mapping, new FileInputStream(mappingfile),Lang.TURTLE);
					
					
		}else{
			log.info("Using direct mapping");
			mapping = automap();
		}
    return mapping;
  }
	
	

  
  /**
   * Based on the configuration given, a direct mapping R2RML document is created.
   * @return the model with the R2RML mapping
   * @throws SQLException thrown, if the db exploration fails
   * @throws FileNotFoundException thrown, if the file cold not be written.
   * @throws MetaModelException 
   * @throws UnsupportedEncodingException 
   */
  public Model automap() throws SQLException, FileNotFoundException, UnsupportedEncodingException, MetaModelException {
    Connection conn = this.dbaccess.getConnection();
    
    DataContext con = new  JdbcDataContext(conn);
    
    
    MappingGenerator gen = new MappingGenerator(baseUri, baseUri, baseUri, ";",dbaccess.getDataTypeHelper().getRowIdTemplate());

    Model dmR2rml = gen.generateMapping(con.getDefaultSchema());
    if (dmR2rmlDump != null && !dmR2rml.isEmpty()) {
      dmR2rml.write(new FileOutputStream(new File(dbaccess.getDBName() + "-dm.ttl")), "TTL");
    }
    conn.close();
    return dmR2rml;
  }
	

}
