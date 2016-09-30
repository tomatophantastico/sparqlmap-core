package org.aksw.sparqlmap.core.r2rml.jdbc;

import java.sql.SQLException;
import java.util.Map;

import org.aksw.sparqlmap.core.BindingException;
import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.r2rml.QuadMap;
import org.aksw.sparqlmap.core.r2rml.QuadMap.LogicalTable;
import org.aksw.sparqlmap.core.r2rml.R2RMLMapping;

import com.google.common.collect.Maps;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SelectBodyString;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * connects an R2RML representation of an mapping to a database.
 * @author joerg
 *
 */

public class JDBCMappingBinder {
  
  private Map<LogicalTable,FromItem> fromItems = Maps.newHashMap();
  
  
  private R2RMLMapping r2rmlmapping;
  private DBAccess dbaccess;

  
  
  
  public JDBCMappingBinder(R2RMLMapping r2rmlmapping, DBAccess dbaccess) {
    super();
    this.r2rmlmapping = r2rmlmapping;
    this.dbaccess = dbaccess;
  }




  public  JDBCMapping bind(){
    
    JDBCMapping jdbcmapping = new JDBCMapping();
    
    // first construct all the Quad Bindings

    
    for(QuadMap quadMap: r2rmlmapping.getQuadMaps().values()){
      JDBCQuadMap jQuadMap = new JDBCQuadMap();
      
      jQuadMap.setBoundFrom(quadMap);
      jQuadMap.setUri(quadMap.getTriplesMapUri());
      
      FromItem from = null;
      if(fromItems.containsKey( quadMap.getLogicalTable())){
        from= fromItems.get(quadMap.getLogicalTable());
      }else{
        from  = getValidatedFromItem(quadMap);
        fromItems.put(quadMap.getLogicalTable(), from);
      }      
   
      
      jdbcmapping.add(jQuadMap);
    }
    
    
    // then construct all the term maps in the bindings
    // this allows easier resolution of referencing triplesmaps
    JDBCTermMapBinder jTermMapBinder = new JDBCTermMapBinder( dbaccess,fromItems);
    
    for(JDBCQuadMap jQuadMap:  jdbcmapping.getTripleMaps()){
      FromItem fi = this.fromItems.get(jQuadMap.getBoundFrom().getLogicalTable());
      jQuadMap.setGraph(jTermMapBinder.bind(jQuadMap.getBoundFrom().getGraph(), fi));
      jQuadMap.setSubject(jTermMapBinder.bind(jQuadMap.getBoundFrom().getSubject(), fi));
      jQuadMap.setPredicate(jTermMapBinder.bind(jQuadMap.getBoundFrom().getPredicate(), fi));
      jQuadMap.setObject(jTermMapBinder.bind(jQuadMap.getBoundFrom().getObject(), fi));
    }
    
    
    //R2RML only allows the referencing map in the object position, we check it anyways everywhere
    for(JDBCQuadMap jQuadMap:  jdbcmapping.getTripleMaps()){
      
      jQuadMap.setGraph(jTermMapBinder.bindRefMap(jQuadMap.getBoundFrom().getGraph(), jQuadMap, jdbcmapping,fromItems));
      jQuadMap.setSubject(jTermMapBinder.bindRefMap(jQuadMap.getBoundFrom().getSubject(), jQuadMap, jdbcmapping,fromItems));
      jQuadMap.setPredicate(jTermMapBinder.bindRefMap(jQuadMap.getBoundFrom().getPredicate(), jQuadMap, jdbcmapping,fromItems));
      jQuadMap.setObject(jTermMapBinder.bindRefMap(jQuadMap.getBoundFrom().getObject(), jQuadMap, jdbcmapping,fromItems));
    }
    
    
    
    return jdbcmapping;
    
    
  }
  
  
  
  
  private FromItem getValidatedFromItem(QuadMap qmap){
    
    LogicalTable ltable = qmap.getLogicalTable();
    FromItem result = null;
    
    //construct the from item from a simple table name
    if(ltable.getTablename()!=null){
      String tablename = ltable.getTablename();
      Table fromTable = new Table(null, tablename);
      fromTable.setAlias("table_" + dbaccess.getIncrementalViewId());
      result = fromTable;
      
      
    }else if(ltable.getQuery() !=null){
      String query = ltable.getQuery();
      SubSelect subsel = new SubSelect();
      subsel.setAlias("query_" + dbaccess.getIncrementalViewId());
      subsel.setSelectBody(new SelectBodyString(query));
  
      result = subsel;
    //fallback  
    }else{
     throw new ImplementationException("No table or query defined");
    }
    try {
      dbaccess.validateFromItem(result);
    } catch (SQLException e) {
     throw new BindingException("Unable to validate query or table for quadmap:" + qmap.getTriplesMapUri() ,e);
    }
    
    return result;
    
  }
  
 
  


}
