package org.aksw.sparqlmap.core.translate.metamodel;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

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
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableColumn;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import jersey.repackaged.com.google.common.collect.Maps;

/**
 * 
 * @author joerg
 *
 */
public class MetaModelSelectiveDump {
  
  public MetaModelSelectiveDump(LogicalTable ltable, Collection<QuadMap> quadmaps, DataContext dcontext) {
    super();
    this.ltable = ltable;
    this.quadmaps = quadmaps;
    this.dcontext = dcontext;
  }


  private LogicalTable ltable;
  private Collection<QuadMap> quadmaps;
  private DataContext dcontext;
  
  
  
  private Map<String,Column> colnameSelectItem = Maps.newHashMap();
  
  public Multimap<Node,Triple> getDump(){
    
    Query query = prepare();
    
    
    Multimap<Node,Triple> dump  = execute(query);
    
    return dump;
  }
  
  
  private Query prepare(){
    
    
    Query query = new Query();
    if(ltable.getTablename()==null){

      
      FromItem fi = new FromItem( ltable.getQuery());
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
        column = new MutableColumn(col);
      }else{
        column = dcontext.getTableByQualifiedLabel(ltable.getTablename()).getColumnByName(col);
      }
      
      
      
      colnameSelectItem.put(col, column);
      query.select(column);
    }
    
    return query;
  }
  
  
  
  private Multimap<Node,Triple> execute(Query query){
    Multimap<Node,Triple>  result = HashMultimap.create();
    DataSet ds =  dcontext.executeQuery(query);
    while(ds.next()){
      Row row = ds.getRow();
      for(QuadMap qm: quadmaps){
        Node graph = materialize(row,qm.getGraph());
        Node subject = materialize(row, qm.getSubject());
        Node predicate = materialize(row, qm.getPredicate());
        Node object = materialize(row, qm.getObject());
        if(graph!=null&& subject!=null&&predicate!=null &&object!=null){
          result.put(graph, new Triple(subject, predicate, object));      
        }
      }
    }
    return result;
    
    
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
        }
        result = NodeFactory.createLiteral(cfString);
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
  
  

}
