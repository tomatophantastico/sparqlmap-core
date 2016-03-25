package org.aksw.sparqlmap.core.r2rml;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectBodyString;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.util.BaseSelectVisitor;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.QuadPosition;

public class JDBCHelper {
  
  
  private static Logger log = LoggerFactory.getLogger(JDBCHelper.class);
  
//  /**
//   * if a triple map s based on a query, we attempt to decompose it.
//   */
//  public static void decomposeVirtualTableQueries(List<JDBCQuadMap> tripleMaps, DataTypeHelper dth) {
//    TERMMAPLOOP: for(JDBCQuadMap trm: tripleMaps){
//      FromItem fi = trm.getFrom();
//      if(fi instanceof SubSelect){
//        SelectBody sb  = ((SubSelect) fi).getSelectBody();
//        if(sb instanceof SelectBodyString){
//          String queryString = ((SelectBodyString) sb).getQuery();
//          CCJSqlParser sqlParser = new CCJSqlParser(new StringReader(queryString));
//          try {
//            sb = sqlParser.SelectBody();
//          } catch (ParseException e) {
//            log.warn("Could not parse query for optimization " + queryString);
//            continue TERMMAPLOOP;
//          }   
//        }
//        
//        if(sb instanceof PlainSelect){
//          cleanColumnNames((PlainSelect) sb);
//          //validate that there are only normal joins on tables are here
//          List<Table> tables = new ArrayList<Table>();
//          List<EqualsTo> joinConds = new ArrayList<EqualsTo>();
//          
//          if(!(((PlainSelect) sb).getFromItem() instanceof Table)){
//            continue;
//          }
//          tables.add((Table) ((PlainSelect) sb).getFromItem());
//          
//          if (((PlainSelect) sb).getJoins() != null) {
//            for (Join join : ((PlainSelect) sb).getJoins()) {
//              if ((join.isSimple()) || join.isFull()
//                  || join.isLeft()
//                  || !(join.getRightItem() instanceof Table)) {
//                log.warn("Only simple joins can be opzimized");
//                continue TERMMAPLOOP;
//              }
//
//              Table tab = (Table) join.getRightItem();
//              if (tab.getAlias() == null) {
//                log.warn("Table: "
//                    + tab.getName()
//                    + " needs an alias in order to be optimized");
//                continue TERMMAPLOOP;
//              }
//
//              tables.add(tab);
//
//              // check if we can make use of the on condition.
//
//              Expression onExpr = join.getOnExpression();
//
//              // shaving of parenthesis
//              if (onExpr instanceof Parenthesis) {
//                onExpr = ((Parenthesis) onExpr).getExpression();
//              }
//
//              if (!(onExpr instanceof EqualsTo)) {
//                log.warn("only simple equals statements can be processed, aborting optimization ");
//                continue TERMMAPLOOP;
//              }
//
//              joinConds.add((EqualsTo) onExpr);
//
//            }
//          }
//          // create a projection map
//          Map<String,Column> projections = new HashMap<String,Column>();
//          
//          for(SelectItem si : ((PlainSelect) sb).getSelectItems()){
//            if(si instanceof SelectExpressionItem){
//              if(!(((SelectExpressionItem) si).getExpression() instanceof Column)){
//                //no  a column in there, so we skip this query
//                continue TERMMAPLOOP;
//              }
//              Column col = (Column) ((SelectExpressionItem) si).getExpression();
//              if(col.getTable().getAlias()==null){
//                col.getTable().setAlias(col.getTable().getName());
//              }
//              
//              if(((SelectExpressionItem) si).getAlias()==null){
//                ((SelectExpressionItem) si).setAlias(col.getColumnName());
//              }
//              
//              String alias = ((SelectExpressionItem) si).getAlias();
//              projections.put( alias,col ); 
//            }
//          }
//          
//          // modify the columns in the term maps
//          
//          JDBCTermMap s = trm.getSubject();
//          trm.setGraph(replaceColumn(trm.getGraph(), trm, projections, tables, joinConds, dth));
//
//          trm.setSubject(replaceColumn(trm.getSubject(), trm, projections, tables, joinConds, dth));
//          trm.setPredicate(replaceColumn(trm.getPredicate(),trm, projections, tables, joinConds,dth));
//          trm.setObject(replaceColumn(trm.getObject(),trm, projections, tables, joinConds,dth));
//
//          
//          log.info("Rewrote query " + trm.getFrom());
//          
//        }
//      }
//      
//    }
//    
//  }
  
  
  
  private static  JDBCTermMap replaceColumn(JDBCTermMap tm,BoundQuadMap trm, Map<String,Column> name2Col, List<Table> tables, List<EqualsTo> joinConditions, DataTypeHelper dth){
    List<Expression> expressions =  new ArrayList<Expression>();
    //we use this to make sure constant value triple maps do not get the column set.
    boolean hasReplaced = false;
    for(Expression casted : tm.getExpressions()){
      String castType = DataTypeHelper.getCastType(casted);
      Expression uncast = DataTypeHelper.uncast(casted);
      
      if(uncast instanceof Column){
        Column col =  name2Col.get(((Column) uncast).getColumnName());
        expressions.add(dth.cast(col, castType));
        hasReplaced = true;
        
      }else if (DataTypeHelper.constantValueExpressions.contains(uncast.getClass())){
        expressions.add(dth.cast(uncast, castType));
      }else{
        throw new ImplementationException("unknown expression in TermMap");
      } 
    }
    JDBCTermMap newTm =JDBCTermMap.createTermMap(dth, expressions);
    if(hasReplaced){
      for(Table table: tables){
        newTm.getAlias2fromItem().put(table.getAlias(), table);
      }
      newTm.getJoinConditions().addAll(joinConditions);
    }
    return newTm; 
  }
  
  
  public static void cleanColumnNames(PlainSelect sb) {
    
    
    SelectVisitor cleaningVisitior = new BaseSelectVisitor(){
      @Override
      public void visit(Column tableColumn) {
        super.visit(tableColumn);

        tableColumn.setColumnName(R2RMLHelper.unescape(tableColumn.getColumnName())); 
        
      }
      
      @Override
      public void visit(Table table) {
        super.visit(table);
        table.setAlias(R2RMLHelper.unescape(table.getAlias()!=null?table.getAlias():table.getName()));
        table.setName(R2RMLHelper.unescape(table.getName()));
        table.setSchemaName(R2RMLHelper.unescape(table.getSchemaName()));
      }
      @Override
      public void visit(SelectExpressionItem selectExpressionItem) {
        super.visit(selectExpressionItem);
        selectExpressionItem.setAlias(R2RMLHelper.unescape(selectExpressionItem.getAlias()));
      }
      
    };
    
    sb.accept(cleaningVisitior);
  }
  
}
