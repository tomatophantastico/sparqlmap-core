package org.aksw.sparqlmap.core.translate.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.aksw.sparqlmap.core.ContextConfiguration;
import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.TranslationContext;
import org.aksw.sparqlmap.core.TranslationContextJDBC;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.syntax.Template;
import org.apache.jena.sparql.util.Context;
import org.slf4j.LoggerFactory;

public class JDBCSparqlExecution implements QueryExecution{
  
  
  private TranslationContextJDBC tcontext;
  private ContextConfiguration conConf;
  private DBAccess dbaccess;
  

  @Override
  public void setInitialBinding(QuerySolution binding) {
    throw new UnsupportedOperationException("Initial Bindings not possible with RDB2RDF query translation");
    
  }

  @Override
  public Dataset getDataset() {
    return null;
  }

  @Override
  public Context getContext() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Query getQuery() {
    return tcontext.getQuery();
  }

  @Override
  public ResultSet execSelect() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Model execConstruct() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Model execConstruct(Model model) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Iterator<Triple> execConstructTriples() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Model execDescribe() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Model execDescribe(Model model) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Iterator<Triple> execDescribeTriples() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean execAsk() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void abort() {
    throw new ImplementationException("abort is currently not suported");
    
  }

  @Override
  public void close() {
    throw new ImplementationException("close is currently not suported");
    
  }

  @Override
  public boolean isClosed() {
    throw new ImplementationException("close currently not suported");
  }

  @Override
  public void setTimeout(long timeout, TimeUnit timeoutUnits) {
    throw new ImplementationException("Timeouts are currently not suported");
    
  }

  @Override
  public void setTimeout(long timeout) {
    throw new ImplementationException("Timeouts are currently not suported");
    
  }

  @Override
  public void setTimeout(long timeout1, TimeUnit timeUnit1, long timeout2, TimeUnit timeUnit2) {
    throw new ImplementationException("Timeouts are currently not suported");
  }

  @Override
  public void setTimeout(long timeout1, long timeout2) {
    throw new ImplementationException("Timeouts are currently not suported");    
  }

  @Override
  public long getTimeout1() {
    return -1;
  }

  @Override
  public long getTimeout2() {
    return -1;
  }
  
  
  
  private ResultSet executeSelect(){
    TranslationContext context = tcontext.getContext();
    try {
      context.profileStartPhase("Rewriting");

      mapper.rewrite(context);
      
      ResultSet rs = null;
      
      // if we got an empty binding, we can shortcut the translation process
      if(!context.getQueryBinding().isEmpty()){
        LoggerFactory.getLogger("sqllog").debug("SQL " + context.getQueryName() + " " + context.getSqlQuery());

        rs = dbConf.executeSQL(context, baseUri);
      }else{
        //create an empty result set with the query vars
        rs = new ResultSet() {
          
          @Override
          public QuerySolution nextSolution() {
            return null;
          }
          
          @Override
          public Binding nextBinding() {
            return null;
          }
          
          @Override
          public QuerySolution next() {
            return null;
          }
          
          @Override
          public boolean hasNext() {
            return false;
          }
          
          @Override
          public int getRowNumber() {
            return 0;
          }
          
          @Override
          public List<String> getResultVars() {
            
            List<Var>  vars = context.getQueryInformation().getProject().getVars();
            List<String> stringVars = new ArrayList<String>();
            
            for(Var var :vars){
              stringVars.add(var.getName());
            }

            return stringVars;
          }
          
          @Override
          public Model getResourceModel() {
            return null;
          }
          
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
          
        };
      }

     

      return rs;

    } catch (Throwable e) {
      context.setProblem(e);

      log.error(context.toString());

      throw e;
    }
    
  }
  
  
  private Model executeDescribe(){
    TranslationContext context = tcontext.getContext();
    if (context.getTargetContentType() == null) {
      context.setTargetContentType(Lang.TURTLE);
    }

    Model model = ModelFactory.createDefaultModel();
    List<Node> iris = context.getQuery().getResultURIs();
    if ((iris == null || iris.isEmpty())) {
      Var var = context.getQuery().getProjectVars().get(0);


      ResultSet rs = executeSelect();
      while (rs.hasNext()) {
        QuerySolution qs = rs.next();
        if(qs.contains(var.getName())){
          iris.add(qs.get(var.getName()).asNode());
        }
      }

    }

    for (Node node : iris) {
      String con1 =
        "CONSTRUCT {?s_sm ?p_sm <" + node.getURI() + "> } WHERE { ?s_sm ?p_sm <" + node.getURI() + "> }";
      
      TranslationContext subCon1 = new TranslationContext();
      subCon1.setTargetContentType(context.getTargetContentType());
      subCon1.setQueryString(con1);
      subCon1.setQueryName("construct incoming query");
      subCon1.setQuery(copyFromAndFromNamedGraph(QueryFactory.create(con1), context.getQuery()) );

      model.add(executeConstruct(subCon1));
      String con2 = "CONSTRUCT { <" + node.getURI() + "> ?p_sm ?o_sm} WHERE { <" + node.getURI() + "> ?p_sm ?o_sm}";
      TranslationContext subCon2 = new TranslationContext();
      subCon2.setTargetContentType(context.getTargetContentType());
      subCon2.setQueryString(con2);
      subCon2.setQuery(copyFromAndFromNamedGraph(QueryFactory.create(con2),context.getQuery()));
      subCon2.setQueryName("construct outgoing query");

      model.add(executeConstruct(subCon2));

    }
    
    return model;
  }
  
  private boolean executeAsk(){
    TranslationContext context = tcontext.getContext();
    context.getQuery().setLimit(1);
    ResultSet rs = executeSelect(context);
    return rs.hasNext();
  }
  
  public Model executeConstruct(TranslationContext context) throws SQLException {
    Model model = ModelFactory.createDefaultModel();
    Template template = context.getQuery().getConstructTemplate();
    context.getQuery().setQueryResultStar(true);
    // execute it

    ResultSet rs = executeSelect(context);

    while (rs.hasNext()) {
      Set<Triple> generatedTriples = new HashSet<Triple>();
      Map<Node, Node> bNodeMap = new HashMap<Node, Node>();
      Binding binding = rs.nextBinding();
      template.subst(generatedTriples, bNodeMap, binding);

      for (Triple generatedTriple : generatedTriples) {
        if (generatedTriple.isConcrete()) {
          model.getGraph().add(generatedTriple);
        } else {
          log.warn("Unconcrete triple created by template, skipping: " + generatedTriple.toString());
        }

      }

    }

    return model;

  }

  
  private Query copyFromAndFromNamedGraph(Query into, Query source){
    
    
    for(String from: source.getGraphURIs()){
      into.addGraphURI(from);
    }
    for(String fromNamed: source.getNamedGraphURIs()){
      into.addNamedGraphURI(fromNamed);
    }
    
    return into;
  }

  

}
