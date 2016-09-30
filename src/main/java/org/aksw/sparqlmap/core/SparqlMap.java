package org.aksw.sparqlmap.core;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.mapper.Mapper;
import org.aksw.sparqlmap.core.mapper.finder.Binder;
import org.aksw.sparqlmap.core.mapper.finder.FilterFinder;
import org.aksw.sparqlmap.core.mapper.finder.MappingBinding;
import org.aksw.sparqlmap.core.mapper.finder.QueryInformation;
import org.aksw.sparqlmap.core.normalizer.QueryNormalizer;
import org.aksw.sparqlmap.core.r2rml.R2RMLMapping;
import org.aksw.sparqlmap.core.translate.metamodel.DumperMetaModel;
import org.aksw.sparqlmap.core.translate.metamodel.MetaModelContext;
import org.aksw.sparqlmap.core.translate.metamodel.MetaModelQueryExecution;
import org.aksw.sparqlmap.core.translate.metamodel.TranslationContextMetaModel;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.LangBuilder;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.sparql.resultset.ResultsFormat;
import org.apache.jena.sparql.syntax.Template;
import org.apache.metamodel.DataContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  The main class of Sparqlmap.
 *  Provides methods for executing SPARQL-queries over mapped databases. 
 * @author joerg
 *
 */
public class SparqlMap {
  
  private static Logger log = LoggerFactory.getLogger(SparqlMap.class);

  /** total queries translated by this instance.*/
  private Integer querycount = 0;
  
  private ContextConfiguration contextConf;
  
  private Mapper mapper;
  
  private R2RMLMapping mapping;
 
  /**
   * the pure sql data access
   */
  private DBAccess jdbcAccess;
  
  /**
   * the meta model access
   */
  private DataContext dataContext;
  
     


  public DBAccess getJdbcAccess() {
    return jdbcAccess;
  }

  public void setSqlAccess(DBAccess jdbcAccess) {
    this.jdbcAccess = jdbcAccess;
  }

  public DataContext getDataContext() {
    return dataContext;
  }

  public void setDataContext(DataContext dataContext) {
    this.dataContext = dataContext;
  }
  
  public void setMapping(R2RMLMapping mapping) {
    this.mapping = mapping;
  }
  
  public R2RMLMapping getMapping() {
    return mapping;
  }
  
  protected Mapper getMapper() {
    return mapper;
  }
  
  public ContextConfiguration getContextConf() {
    return contextConf;
  }
  
  public void setContextConf(ContextConfiguration contextConf) {
    this.contextConf = contextConf;
  }

  public org.apache.jena.query.QueryExecution execute(String query){
    TranslationContext tcontext = new  TranslationContext();
    tcontext.setQueryString(query);
    return execute(tcontext);
    
  }
  
  public QueryExecution execute(TranslationContext tcontext){
    QueryExecution result = null;
    //Perform the common tasks
    
    //check if the query is compiled
    if(tcontext.getQuery()==null){
      tcontext.profileStartPhase("parse");
      tcontext.setQuery(QueryFactory.create(tcontext.getQueryString()));
    }
    
    if(log.isDebugEnabled()){
      log.debug(tcontext.getQuery().toString());
    }
    QueryNormalizer.normalize(tcontext); 
    
    
    
    
    if(log.isDebugEnabled()){
      log.debug(tcontext.getBeautifiedQuery().toString());
    }
    
    tcontext.profileStartPhase("bind");

    //Analyze Query
    QueryInformation qi = FilterFinder.getQueryInformation(tcontext.getBeautifiedQuery());
    tcontext.setQueryInformation(qi);
    
    
    Binder binder = new Binder(this.mapping);
    MappingBinding mb = binder.bind(tcontext);
    tcontext.setQueryBinding(mb);
    if(log.isDebugEnabled()){
      log.debug(tcontext.getQueryBinding().toString());
    }

    //delegate to the backend.
    
    
    if(dataContext!=null){
      // use the metamodel backend
      
      TranslationContextMetaModel mmtc = new TranslationContextMetaModel(tcontext);
      MetaModelQueryExecution mmqe = new MetaModelQueryExecution(mmtc, dataContext);
      
      
      
      result = mmqe;
    }
    
    
    return result;
    
  }
  
  
  
  public Dumper getDumpExecution(){
    Dumper result = null;
    if(dataContext!=null){
      result = new DumperMetaModel(new MetaModelContext(dataContext, contextConf),mapping);
    }
    
    
    
    return result;
    
    
  }
  
  
  
  

  /**
   * Returns the result of a SPARQL query as a String.
   * Use {@link SparqlMap.execute()} instead.
   * 
   * 
   * @param qstring
   * @param rt
   * @return
   * @throws SQLException
   */
  @Deprecated
  public String executeSparql(String qstring, Object rt) throws SQLException {
    ByteArrayOutputStream resBos = new ByteArrayOutputStream();
    executeSparql(qstring, rt, resBos);
    return resBos.toString();
  }

  /**
   * Executes a SPARQL query and writes its result into the Outputstream.
   * 
   * Use {@link SparqlMap.execute()} instead.
   * 
   * @param qstring the query
   * @param rt the Return type, either as Lang or as ResultsFormat.
   * @param out the stream into which the result gets written into.
   * @throws SQLException thrown if an db error occurs.
   */
  @Deprecated
  public void executeSparql(String qstring, Object rt, OutputStream out) throws SQLException {
    executeSparql(qstring, rt, out, "Unnamed query " + this.querycount++);
  }

  /**
   * Takes some of the functionality of QueryExecutionbase.
   * 
   * @param queryname
   *          an query name, just for the log.
   * @param qstring
   *          the SPARQL query that should be executed
   * @param out
   *          the result gets printed into this stream.
   * @throws SQLException thrown if an db error occurs.
   */
  @Deprecated
  protected void executeSparql(String qstring, Object rf, OutputStream out, String queryname) throws SQLException {

    TranslationContext context = new TranslationContext();
    context.setQueryString(qstring);
    context.setQueryName(queryname);
    context.profileStartPhase("Query Compile");
    context.setQuery(QueryFactory.create(qstring));
    context.setTarget(rf);

    try {
      if (context.getQuery().isAskType()) {

          ResultSetFormatter.out(out, executeAsk(context));
        

      }
      if (context.getQuery().isConstructType()) {

        if (context.getTarget() != null && context.getTarget().equals(Lang.NTRIPLES)) {
          executeConstruct(context, out);
        } else {
          Model model = executeConstruct(context);
          RDFDataMgr.write(out, model, (Lang) context.getTarget());

        }

      }
      if (context.getQuery().isSelectType()) {
        ResultSet rs = executeSelect(context);

        if (context.getTarget() == null) {
          context.setTarget(ResultsFormat.FMT_RDF_XML);
        }

        ResultSetFormatter.output(out, rs, (ResultsFormat) context.getTarget());

      }
      if (context.getQuery().isDescribeType()) {

       Model model = executeDescribe(context);

        RDFDataMgr.write(out, model, (Lang) context.getTarget());

      }

    } catch (Throwable e) {
      log.error("An error occured while translating\n\n " + context.toString(), e);
      throw e;
    }

  }
  /**
   * Use {@link SparqlMap.execute()} instead.
   */
  @Deprecated
  public boolean executeAsk(String query) throws SQLException{
    TranslationContext context = new TranslationContext();
    context.setQueryString(query);
    context.setQuery(QueryFactory.create(query));
    return executeAsk(context);
    
  }
  
  /**
   * Use {@link SparqlMap.execute()} instead.
   * 
   * @param context
   * @return
   */
  @Deprecated 
  public boolean executeAsk(TranslationContext context){
    
    return execute(context).execAsk();
    
  }

  @Deprecated
  public Model executeConstruct(String query) throws SQLException {
    TranslationContext context = new TranslationContext();
    context.setQueryString(query);
    try {

      context.setQuery(QueryFactory.create(query));
      return executeConstruct(context);
    } catch (Exception e) {
      context.setProblem(e);
      log.error(context.toString());
      throw e;
    }

  }
  
  @Deprecated
  public Model executeConstruct(TranslationContext context) throws SQLException {
    return execute(context).execConstruct();

  }

  /**
   * performing an construct like this does build up an in-memory representation of the data and streams it right to the
   * client.
   * 
   * Use only with ntriples.
   * 
   * @param context
   * @param out
   * @throws SQLException
   */
  @Deprecated
  public void executeConstruct(TranslationContext context, OutputStream out) throws SQLException {
    // take the graph pattern and convert it into a select query.
    Template template = context.getQuery().getConstructTemplate();
    context.getQuery().setQueryResultStar(true);
    // execute it
    ResultSet rs = executeSelect(context);

    // bind it
    int i = 0;
    Graph graph = GraphFactory.createDefaultGraph();
    while (rs.hasNext()) {
      Set<Triple> set = new HashSet<Triple>();
      Map<Node, Node> bNodeMap = new HashMap<Node, Node>();
      Binding binding = rs.nextBinding();
      template.subst(set, bNodeMap, binding);

      for (Triple t : set) {
        graph.add(t);

      }

      if (++i % 1000 != 0) {
        RDFDataMgr.write(out, graph, LangBuilder.create().contentType(context.getTarget().toString()).build());
      }
    }
    RDFDataMgr.write(out, graph, LangBuilder.create().contentType(context.getTarget().toString()).build());

  }
  
  /**
   * Execute a describe query on this context.
   * 
   * @param query
   * @return
   * @throws SQLException
   */
  
  @Deprecated
  public Model executeDescribe(String query) throws SQLException{
    TranslationContext context = new TranslationContext();
    context.setQueryString(query);
    try {

      context.setQuery(QueryFactory.create(query));
      return executeDescribe(context);
    } catch (SQLException e) {
      context.setProblem(e);
      log.error(context.toString());
      throw e;
    }
  }
  
  /**
   * 
   * Executes the describe query, in three steps. First, the list of resources to be describes is created. This is the case when the describe query contains a pattern.
   * Second, a construct query that queries for all triples, where the resources is in the subject positions and third in the object position is executed. The unioned result is then reutrned.
   *
   * 
   * @param context
   * @return
   * @throws SQLException
   */
  @Deprecated
  public Model executeDescribe(TranslationContext context) throws SQLException{
    return execute(context).execDescribe();
    
  }
  

  

  
  @Deprecated
  public ResultSet executeSelect(String query) throws SQLException {

    TranslationContext context = new TranslationContext();
    context.setQueryString(query);
    context.setQuery(QueryFactory.create(query));

    return executeSelect(context);

  }
  
  @Deprecated
  public ResultSet executeSelect(final TranslationContext context) throws SQLException {
    return execute(context).execSelect();
  }
  
public void close(){
  
    if(jdbcAccess!=null){
      jdbcAccess.close();
    }
    
  
  }

  


}
