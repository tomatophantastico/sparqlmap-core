package org.aksw.sparqlmap.core.translate.metamodel;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.aksw.sparqlmap.core.r2rml.QuadMap;
import org.aksw.sparqlmap.core.r2rml.QuadMap.LogicalTable;
import org.aksw.sparqlmap.core.r2rml.R2RML;
import org.aksw.sparqlmap.core.r2rml.TermMap;
import org.aksw.sparqlmap.core.r2rml.TermMapColumn;
import org.aksw.sparqlmap.core.r2rml.TermMapConstant;
import org.aksw.sparqlmap.core.r2rml.TermMapTemplate;
import org.aksw.sparqlmap.core.r2rml.TermMapTemplateTuple;
import org.aksw.sparqlmap.core.util.QuadPosition;
import org.apache.jena.datatypes.BaseDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.parser.QueryParser;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aol.cyclops.data.async.Queue;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import com.google.common.collect.Maps;

/**
 * 
 * @author joerg
 *
 */
public class MetaModelSelectiveDump implements Runnable{
  
  private AtomicInteger threadCount;
  
  private static final Logger LOGGER = LoggerFactory.getLogger(MetaModelSelectiveDump.class);

  public MetaModelSelectiveDump(LogicalTable ltable, Collection<QuadMap> quadmaps, DataContext dcontext, Queue<Multimap<Node,Triple>> queue, AtomicInteger threadCount) {
    super();
    this.ltable = ltable;
    this.quadmaps = quadmaps;
    this.dcontext = dcontext;
    this.queue = queue;
    this.threadCount = threadCount;
    prepare();
  }


  private LogicalTable ltable;
  private Collection<QuadMap> quadmaps;
  private DataContext dcontext;
  private Queue<Multimap<Node,Triple>> queue;
  
  private Query query;
  
  
  private Map<String,Column> colnameSelectItem = Maps.newHashMap();
  

  
  

  
  
  
  private void prepare(){
     
    query = new Query();
    if(ltable.getTablename()==null){
      
      QueryParser qp = new QueryParser(dcontext, ltable.getQuery().replaceAll("\"", ""));
      Query subQuery = qp.parse();
      FromItem fi = new FromItem( subQuery);
      fi.setAlias("sq");  
      query.from(fi);
    }else{
      query.from(dcontext.getTableByQualifiedLabel(ltable.getTablename()));
    }
    Set<String> cols = Sets.newHashSet();
    
    for(QuadMap qm: quadmaps){
      if(!qm.getLogicalTable().equals(ltable)){
        throw new IllegalArgumentException("QuadMap not of supplied logical table");
      }
      
      for(QuadPosition pos: QuadPosition.values()){
        TermMap tm = qm.get(pos);
        if(tm instanceof TermMapColumn){
          cols.add(((TermMapColumn) tm).getColumn());
        }else if (tm instanceof TermMapTemplate){
          for(TermMapTemplateTuple tmtt : ((TermMapTemplate) tm).getTemplate()){
            cols.add(tmtt.getColumn());
           
          }
        }
      }
    }
    
    for(String col:cols){
      Column column;
      if(ltable.getTablename()==null){
        
        MutableColumn mcolumn = new MutableColumn(col);
        if(dcontext instanceof JdbcDataContext){
          mcolumn.setQuote(
          ((JdbcDataContext) dcontext).getIdentifierQuoteString());
        }
        column = mcolumn;
      }else{
        column = dcontext.getTableByQualifiedLabel(ltable.getTablename()).getColumnByName(col);
      }
      
      
      
      colnameSelectItem.put(col, column);
      query.select(column);
    }
    
  }
  
  
  
  public void dump(){
    int count = 0;
    try(DataSet ds =  dcontext.executeQuery(query)){
      while(ds.next()){
        Row row = ds.getRow();
        count++;
        Multimap<Node,Triple> rowTriples = HashMultimap.create();
        for(QuadMap qm: quadmaps){
          Node graph = materialize(row,qm.getGraph());
          Node subject = materialize(row, qm.getSubject());
          Node predicate = materialize(row, qm.getPredicate());
          Node object = materialize(row, qm.getObject());
          if(graph!=null&& subject!=null&&predicate!=null &&object!=null){
            rowTriples.put(graph, new Triple(subject, predicate, object));      
          }
        }
        queue.offer(rowTriples);
      }
    }catch(MetaModelException e){
    LOGGER.error("Error executing: "+ query.toSql() ,e);
    }finally{
      synchronized (threadCount) {
        if(threadCount.get()>1){
          threadCount.decrementAndGet();
        }else{
          queue.close();

        }
      }
      LOGGER.debug("Query {} executed \n with {} results", query.toSql(),count);
    }

   
    
  }
  
  private Node materialize(Row row, TermMap tm){
    Node result = null;
    String cfString = assembleString(tm, row);
      if(cfString!=null){
      if(tm.getTermTypeIRI().equals(R2RML.IRI_STRING)){
        result = NodeFactory.createURI(cfString);
      }else if(tm.getTermTypeIRI().equals(R2RML.BLANKNODE_STRING)){
        result = NodeFactory.createBlankNode(cfString);
      }else{
        if(tm.getLang()!=null){
          result = NodeFactory.createLiteral(cfString, tm.getLang());
        }else if(tm.getDatatypIRI()!=null){
          result = NodeFactory.createLiteral(cfString, new BaseDatatype(tm.getDatatypIRI()));
        }else{
          result = NodeFactory.createLiteral(cfString);
        }
      }
    }
    return result;
  }
  
  private String assembleString(TermMap tm, Row row){
    String result = null;
    if(tm instanceof TermMapConstant){
      result = ((TermMapConstant) tm).getConstant();
    }else if(tm instanceof TermMapTemplate){
      StringBuffer sb = new StringBuffer();
      for(TermMapTemplateTuple tmtt : ((TermMapTemplate) tm).getTemplate()){
        if(tmtt.getPrefix()!= null){
          sb.append(tmtt.getPrefix());
        }
        if(tmtt.getColumn()!=null){
          sb.append(row.getValue(colnameSelectItem.get(tmtt.getColumn())));
        }
      }
      result = sb.toString();
    } else if(tm instanceof TermMapColumn){
      if( row.getValue(colnameSelectItem.get(((TermMapColumn) tm).getColumn()))!=null){
        result = row.getValue(colnameSelectItem.get(((TermMapColumn) tm).getColumn())).toString();

      }
      
      
    }
    return result;
  }



  @Override
  public void run() {
    dump();
  }
  
  

}
