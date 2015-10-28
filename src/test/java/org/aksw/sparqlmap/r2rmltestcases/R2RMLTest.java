package org.aksw.sparqlmap.r2rmltestcases;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import jersey.repackaged.com.google.common.collect.Lists;

import org.aksw.sparqlmap.DBHelper;
import org.aksw.sparqlmap.TestHelper;
import org.aksw.sparqlmap.DockerHelper.DBConnConfig;
import org.aksw.sparqlmap.core.SparqlMap;
import org.aksw.sparqlmap.core.automapper.MappingGenerator;
import org.aksw.sparqlmap.core.db.Connector;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.WebContent;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.PropertiesPropertySource;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;

@RunWith(value = Parameterized.class)
public abstract class R2RMLTest {
	
	public static String baseUri = "http://example.com/base/";
	
	
	static DBConnConfig dbconf;
	
	String testCaseName;
	String r2rmlLocation;
	String outputLocation;
	String referenceOutput;
	String dbFileLocation;
	boolean createDM;
	
	
	
	
	private static Logger log = LoggerFactory.getLogger(R2RMLTest.class);
	
	private static Properties fails;
	
	{
	    fails = new Properties();
        try {
	    InputStream stream = 
	               ClassLoader.getSystemClassLoader().getResourceAsStream("bsbm_failing.properties");
	       fails.load(stream);

            stream.close();
        } catch (IOException e) {
            log.error("Problem loading failing test information",e);
        }
	}
	
	

	static Boolean dbIsReachable = null;


	public R2RMLTest(String testCaseName, String r2rmlLocation,
			String outputLocation, String referenceOutput,
			String dbFileLocation, boolean createDM) {
		super();
		this.testCaseName = testCaseName;
		this.r2rmlLocation = r2rmlLocation;
		this.outputLocation = outputLocation;
		this.referenceOutput = referenceOutput;
		this.dbFileLocation = dbFileLocation;
		this.createDM = createDM;
	}


	@Test
	public void runTestcase() throws ClassNotFoundException, SQLException, IOException{
	  if(fails.containsKey(testCaseName)){
	      String value = fails.getProperty(testCaseName);
	      String dbs = value.split(":")[0];
	      Assume.assumeFalse(value.split(":")[1], 
	              dbs.equals("ALL")
	              || Lists.newArrayList(dbs.split(",")).contains(getDataTypeHelper().getDBName().toLowerCase()));

	  }
	  
	    
	    
	  if(dbIsReachable==null){
      dbIsReachable = DBHelper.waitAndConnect(dbconf);
	  }
	  Assume.assumeTrue("Database not reachable in previous test, skipping: ",dbIsReachable);
	 
	  
	
    
	  
	  flushDatabase();
		loadFileIntoDB(dbFileLocation);
		
		
		if(createDM){
			createDM(r2rmlLocation);
		}
		
		//let the mapper run.
		
		map();
	
		
		assertAreEqual(outputLocation,referenceOutput);
		
	}
	
	
	
	 
	  
	

	private void map() throws SQLException, FileNotFoundException {
		AnnotationConfigApplicationContext ctxt = new AnnotationConfigApplicationContext();
		Properties sm = new Properties();
		sm.put("sm.baseuri", baseUri);
		//sm.put("sm.r2rmlvocablocation", new File("./src/main/resources/vocabularies/r2rml.ttl").getAbsolutePath());
		sm.put("sm.mappingfile", r2rmlLocation);
		
		ctxt.getEnvironment().getPropertySources().addFirst(new PropertiesPropertySource("sm", sm));
		ctxt.getEnvironment().getPropertySources().addFirst(new PropertiesPropertySource("db", getDBProperties()));
		
		ctxt.scan("org.aksw.sparqlmap");
		ctxt.refresh();
		
		SparqlMap r2r = ctxt.getBean(SparqlMap.class);
		r2r.dump(new FileOutputStream(new File(outputLocation)),WebContent.contentTypeNTriples);
		ctxt.close();
	}
	
	
	public static Collection<Object[]> data(String tcFolder) {
		Collection<Object[]> testCases = new ArrayList<Object[]>();

		try {
			log.info("Reading testcases from folder: " + new File(tcFolder).getAbsolutePath());
			for(File folder: new File(tcFolder).listFiles()){
				if(folder.isDirectory()&&!folder.isHidden()){ 

				Model manifest = ModelFactory.createDefaultModel();
				manifest.read(new FileInputStream(new File(folder.getAbsolutePath()+"/manifest.ttl")), null,"TTL");
				
				
				
				
				// get the direct mapping test cases
				
				com.hp.hpl.jena.query.ResultSet dmRS = 
						QueryExecutionFactory.create(QueryFactory.create("PREFIX test: <http://www.w3.org/2006/03/test-description#> \n" + 
								"PREFIX dcterms: <http://purl.org/dc/elements/1.1/> \n" + 
								"PREFIX  rdb2rdftest: <http://purl.org/NET/rdb2rdf-test#> " +
								"SELECT * WHERE {\n " +
								"   ?tc a rdb2rdftest:DirectMapping ;	\n" + 
								"	dcterms:title ?title ; \n" + 
								"	dcterms:identifier ?identifier ;\n" + 
								"	test:purpose ?purpose ;\n" + 
								"	test:specificationReference ?reference ;\n" + 
								"	test:reviewStatus ?reviewStatus ;\n" + 
								"	rdb2rdftest:hasExpectedOutput ?expectedOutput ;\n" + 
								"	rdb2rdftest:database ?db ;\n" +
								"	rdb2rdftest:output ?outfname .\n" + 
								"   ?db rdb2rdftest:sqlScriptFile ?dbfile .\n" + 

								" } "),manifest).execSelect();
				
				while(dmRS.hasNext()){
					Binding bind = dmRS.nextBinding();
					String title = bind.get(Var.alloc("title")).getLiteral().toString();
					String identifier = bind.get(Var.alloc("identifier")).getLiteral().toString();
					String purpose = bind.get(Var.alloc("purpose")).getLiteral().toString();
					String reference = bind.get(Var.alloc("reference")).getLiteral().toString();
					String expectedOutput = bind.get(Var.alloc("expectedOutput")).getLiteral().toString();
					String outfname = bind.get(Var.alloc("outfname")).getLiteral().toString();
					String dbname = bind.get(Var.alloc("db")).getURI();
					String dbFileName = bind.get(Var.alloc("dbfile")).getLiteral().toString();
					
					testCases.add(new Object[]{identifier,makeAbsolute(folder, "dm_r2rml.ttl") ,getFileOutName(folder, outfname),makeAbsolute(folder, outfname),makeAbsolute(folder, dbFileName),true});
				}
				
				// get the regular test cases
				
				
				com.hp.hpl.jena.query.ResultSet r2rRs = 
						QueryExecutionFactory.create(QueryFactory.create("PREFIX test: <http://www.w3.org/2006/03/test-description#> \n" + 
								"PREFIX dcterms: <http://purl.org/dc/elements/1.1/> \n" + 
								"PREFIX  rdb2rdftest: <http://purl.org/NET/rdb2rdf-test#> " +
								"SELECT * WHERE {\n " +
								"   ?tc a rdb2rdftest:R2RML ;	\n" + 
								"	dcterms:title ?title ; \n" + 
								"	dcterms:identifier ?identifier ;\n" + 
								"	test:purpose ?purpose ;\n" + 
								"	test:specificationReference ?reference ;\n" + 
								"	test:reviewStatus ?reviewStatus ;\n" + 
								"	rdb2rdftest:hasExpectedOutput ?expectedOutput ;\n" + 
								"	rdb2rdftest:database ?db ;\n" + 
								"	rdb2rdftest:output ?outfname ;\n" + 
								"	rdb2rdftest:mappingDocument ?mappingfname .\n" + 
								"   ?db rdb2rdftest:sqlScriptFile ?dbfile .\n" + 

								" } "),manifest).execSelect();
				
				while(r2rRs.hasNext()){				
					Binding bind = r2rRs.nextBinding();
					String title = bind.get(Var.alloc("title")).getLiteral().toString();
					String identifier = bind.get(Var.alloc("identifier")).getLiteral().toString();
					String purpose = bind.get(Var.alloc("purpose")).getLiteral().toString();
					String reference = bind.get(Var.alloc("reference")).getLiteral().toString();
					String expectedOutput = bind.get(Var.alloc("expectedOutput")).getLiteral().toString();
					String outfname = bind.get(Var.alloc("outfname")).getLiteral().toString();
					String mappingfname = bind.get(Var.alloc("mappingfname")).getLiteral().toString();
					String dbFileName = bind.get(Var.alloc("dbfile")).getLiteral().toString();

					testCases.add(new Object[]{identifier,makeAbsolute(folder, mappingfname) ,getFileOutName(folder, outfname),makeAbsolute(folder, outfname),makeAbsolute(folder, dbFileName),false});

					}
				}	
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return testCases;
		
	}
	
	
	
	
	public void createDM(String wheretowrite) throws ClassNotFoundException, SQLException, FileNotFoundException, UnsupportedEncodingException, MetaModelException{
		Connection conn = DBHelper.getConnection(dbconf);
		
		
		
		MappingGenerator db2r2rml = new MappingGenerator( "http://example.com/base/", "http://example.com/base/", "http://example.com/base/",";",getDataTypeHelper().getRowIdTemplate());
		
		DataContext con = new JdbcDataContext(conn);
		Model mapping = db2r2rml.generateMapping(con.getDefaultSchema());
		conn.close();
		mapping.write(new FileOutputStream(new File(wheretowrite)), "TTL", null);
		
		
	}
	
	
	private static String makeAbsolute(File folder, String name){
		return folder.getAbsolutePath() + "/"  +name;
	}
	
	private static String getFileOutName(File folder, String outfname) {
		return folder.getAbsolutePath() + "/" + outfname.split("\\.")[0]
				+ "-sparqlmap." + outfname.split("\\.")[1];
	}
	
	


	
	 public Properties getDBProperties() {
	    Properties props = new Properties();
	    // replicating the values from initDatabase
	    props.put("jdbc.url",dbconf.jdbcString);
	    props.put("jdbc.username",dbconf.username);
	    props.put("jdbc.password",dbconf.password);
	    
	    return props;
	  }
	
	/**
	 * closes the connection
	 * @param conn
	 */
	public void closeConnection(Connection conn){
		//makeshift connection handling is ok here.
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	
	/**
	 * load the file into the database
	 * @param file
	 * @return
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 */
	public void loadFileIntoDB(String file) throws ClassNotFoundException, SQLException, IOException{
	  
	  log.info(String.format("Loading %s into the database",file));
	  
	   Connection conn = DBHelper.getConnection(dbconf);
	   DBHelper.loadSqlFile(conn, file);
	   
	   conn.close();

//		String sql2Execute = FileUtils.readFileToString(new File(file));
//		loadStringIntoDb(sql2Execute);

		
	}


	/**
	 * deletes all tables of the database
	 * @return true if delete was successfull
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public void flushDatabase() throws ClassNotFoundException, SQLException{
		Connection conn = DBHelper.getConnection(dbconf);

		DBHelper.flushDb(conn);

		conn.close();
		
	}
	


	/**
	 * compares the two files for equality
	 * @param outputLocation2
	 * @param referenceOutput2
	 * @return true if they are equal
	 * @throws FileNotFoundException 
	 * @throws SQLException 
	 */
	
	public void assertAreEqual(String outputLocation, String referenceOutput) throws FileNotFoundException, SQLException {
		
	  boolean result = false;
	  
		Model m1 = ModelFactory.createDefaultModel();
		String fileSuffixout = outputLocation.substring(outputLocation.lastIndexOf(".")+1).toUpperCase();
		
		if(fileSuffixout.equals("NQ")){
			DatasetGraph dsgout = RDFDataMgr.loadDatasetGraph(outputLocation);
			DatasetGraph dsdref = RDFDataMgr.loadDatasetGraph(referenceOutput);
			
			Assert.assertFalse("One result is empty, the other not.",dsgout.isEmpty() != dsdref .isEmpty());

			
			Iterator<Node> iout = dsgout.listGraphNodes();
			List<Node> iref = Lists.newArrayList(dsdref.listGraphNodes());
		
	    while (iout.hasNext()){
	      Node outNode = (Node) iout.next();
	      Graph outgraph =  dsgout.getGraph(outNode);
	      Graph refGRaf = dsdref.getGraph(outNode);
	      
	      if(refGRaf==null){
	        log.info("Missing graph in reference output :" + outNode);
	        break;
	      }else{
	        
	        TestHelper.assertModelAreEqual(
	            ModelFactory.createModelForGraph(outgraph), 
	            ModelFactory.createModelForGraph(refGRaf));
	        
  
  	      }
	      iref.remove(outgraph);
	    }
	    if(!iref.isEmpty()){
	      log.info("not all reference graphs were created"  + iref.toString());
	      result = false;
	    }
	    
	    
	    result =  true;
			    
		}else {
		//if(fileSuffixout.equals("TTL")){
			m1.read(new FileInputStream(outputLocation),null,"TTL");
			Model m2 = ModelFactory.createDefaultModel();
			m2.read(new FileInputStream(referenceOutput),null,"TTL");
			
			TestHelper.assertModelAreEqual(m1, m2);
		}	
	}
	
	 abstract DataTypeHelper getDataTypeHelper();
}
