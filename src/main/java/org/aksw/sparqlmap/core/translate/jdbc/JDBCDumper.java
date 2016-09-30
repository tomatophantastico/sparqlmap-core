package org.aksw.sparqlmap.core.translate.jdbc;

import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.aksw.sparqlmap.core.ContextConfiguration;
import org.aksw.sparqlmap.core.TranslationContext;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.mapper.QueryDeunifier;
import org.aksw.sparqlmap.core.mapper.finder.FilterFinder;
import org.aksw.sparqlmap.core.mapper.finder.MappingBinding;
import org.aksw.sparqlmap.core.mapper.finder.QueryInformation;
import org.aksw.sparqlmap.core.r2rml.R2RML;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCQuadMap;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.algebra.AlgebraGenerator;
import org.apache.jena.sparql.algebra.AlgebraQuad;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.algebra.op.OpQuadPattern;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

public class JDBCDumper {
  
  
  private static final Logger log = LoggerFactory.getLogger(JDBCDumper.class);
  
  private DBAccess dbaccess;
  private ContextConfiguration conconf;
  
  
  /**
   * dumps into the whole config into the writer.
   * 
   * @param writer
   * @throws SQLException
   */

  public void dump(OutputStream out, String format) throws SQLException {
    PrintStream writer = new PrintStream(out);

    List<TranslationContext> contexts = generatedumpTranslationContexts();
    for (TranslationContext context : contexts) {

      log.info("SQL: " + context.getSqlQuery());
      org.apache.jena.query.ResultSet rs = dbaccess.executeSQL(context, conconf.getBaseUri());
      DatasetGraph graph = DatasetGraphFactory.createMem();
      int i = 0;
      while (rs.hasNext()) {
      
        Binding bind = rs.nextBinding();
        addDumpBindingToDataset(bind, graph);
          
        if (++i % 10000 == 0) {
          RDFDataMgr.write(out, graph, RDFFormat.NQUADS);
          graph.deleteAny(null, null, null, null);
        }
      }
      RDFDataMgr.write(out, graph, RDFFormat.NQUADS);
    

      writer.flush();
    }
  }

  /**
   * dumps into the whole config into the writer.
   * 
   * @param writer
   * @throws SQLException
   */

  public DatasetGraph dump() throws SQLException {

    DatasetGraph dataset = DatasetGraphFactory.createMem();

    List<TranslationContext> contexts = generatedumpTranslationContexts();
    for (TranslationContext context : contexts) {
      log.debug("SQL: " + context.getSqlQuery());
      org.apache.jena.query.ResultSet rs = dbaccess.executeSQL(context,
          conconf.getBaseUri());
      while (rs.hasNext()) {
        Binding bind = rs.nextBinding();
        addDumpBindingToDataset(bind, dataset);
      }
    }
    return dataset;
  }
  
  
  private void addDumpBindingToDataset(Binding bind, DatasetGraph dsg){
    
    try {
      Node g = bind.get(Var.alloc("g"));
      Node s = bind.get(Var.alloc("s"));
      Node p = bind.get(Var.alloc("p"));
      Node o = bind.get(Var.alloc("o"));
      if (s != null && p != null && o != null) {
        if(g!=null
            && !(g.equals(Quad.defaultGraphNodeGenerated)
            || g.hasURI(R2RML.DEFAULTGRAPH.getURI()))){
          Quad toadd = new Quad(g, s, p, o);
          dsg.add(toadd);
        }else{
          Triple triple = new Triple(s, p, o);
          dsg.getDefaultGraph().add(triple);
        }
       
      }
    } catch (Exception e) {

      log.error("Error:", e);
      if (!conconf.isContinueWithInvalidUris()) {
        throw new RuntimeException(e);
      }
    }
  }
  
  
  
 
  private List<TranslationContext> generatedumpTranslationContexts() {
    
    List<TranslationContext> contexts = new ArrayList<TranslationContext>();
    
//    Query spo = QueryFactory.create("SELECT ?s ?p ?o {{?s ?p ?o}}");
    Query spo = QueryFactory.create("SELECT ?g ?s ?p ?o {GRAPH ?g {?s ?p ?o}}");
    
    AlgebraGenerator gen = new AlgebraGenerator();
    Op qop = AlgebraQuad.quadize(gen.compile(spo));
    
    //Triple triple = ((OpBGP)((OpGraph)((OpProject)qop).getSubOp()).getSubOp()).getPattern().get(0);
    Quad triple = ((OpQuadPattern)(((OpProject)qop)).getSubOp()).getPattern().get(0);
    
    
    
    
    
    for(JDBCQuadMap fullTrm: mappingConf.getTripleMaps()){
      for(PO po :fullTrm.getPos()){

        TranslationContext context = new TranslationContext();
        JDBCQuadMap singleTrm = fullTrm.getShallowCopy();
        singleTrm.getPos().clear();
        singleTrm.addPO(po.getPredicate(), po.getObject());

        context.setQuery(spo);
        context.setQueryName("dump query");

        Map<Quad, Collection<JDBCQuadMap>> bindingMap =
          new HashMap<Quad, Collection<JDBCQuadMap>>();
        bindingMap.put(triple, Arrays.asList(singleTrm));

        MappingBinding qbind = new MappingBinding(bindingMap);
        context.setQueryBinding(qbind);

        QueryInformation qi = FilterFinder.getQueryInformation(qop);
        qi.setProject((OpProject) qop);

        context.setQueryInformation(qi);

        QueryBuilderVisitor qbv =
          new QueryBuilderVisitor(context, dth, exprconv, filterUtil,tmf);

        OpWalker.walk(qop, qbv);

        // prepare deparse select
        StringBuilder sbsql = new StringBuilder();
        Select select = qbv.getSqlQuery();
        SelectDeParser selectDeParser = dbconf.getSelectDeParser(sbsql);

        if (fopt.isOptimizeSelfUnion()) {

          QueryDeunifier unionOpt =
            new QueryDeunifier(context.getQueryInformation(),
              context.getQueryBinding(), dth, exprconv, colhelp, fopt);
          if (!unionOpt.isFailed()) {
            QueryInformation newqi = unionOpt.getQueryInformation();
            newqi.setProjectionPushable(context.getQueryInformation()
              .isProjectionPush());
            context.setQueryInformation(newqi);
            context.setQueryBinding(unionOpt.getQueryBinding());
          }
        }

        selectDeParser.setBuffer(sbsql);
        // ExpressionDeParser expressionDeParser =
        // mappingConf.getR2rconf().getDbConn().getExpressionDeParser(selectDeParser,
        // out);
        // selectDeParser.setExpressionVisitor(expressionDeParser);
        if (select.getWithItemsList() != null
          && !select.getWithItemsList().isEmpty()) {
          sbsql.append("WITH ");
          for (Iterator iter = select.getWithItemsList().iterator(); iter
            .hasNext();) {
            WithItem withItem = (WithItem) iter.next();
            sbsql.append(withItem);
            if (iter.hasNext())
              sbsql.append(",");
            sbsql.append(" ");
          }
        }
        select.getSelectBody().accept(selectDeParser);

        context.setSqlQuery(sbsql.toString());
        contexts.add(context);
      
      }
    }
      
    
    
    
    return contexts;
  }

}
