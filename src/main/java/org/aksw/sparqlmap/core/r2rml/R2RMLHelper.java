package org.aksw.sparqlmap.core.r2rml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.util.BaseSelectVisitor;

import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.exception.ImplementationException;
import org.aksw.sparqlmap.core.exception.R2RMLValidationException;
import org.aksw.sparqlmap.core.mapper.translate.ColumnHelper;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;

import com.hp.hpl.jena.rdf.model.Resource;

/**
 * Helper methods for dealing with R2RML mappings
 * 
 * @author joerg
 *
 */
public abstract class R2RMLHelper {
  
  /**
   * unquote a string, i.e. remove any quatation marks in either the beginning or the end.
   * 
   * @param toUnescape
   * @return
   */
  public static String unescape(String toUnescape) {
    if (toUnescape != null && toUnescape.startsWith("\"")
        && toUnescape.endsWith("\"")) {
      return toUnescape.substring(1, toUnescape.length() - 1);
    } else {
      // not escaped, so we need to see how the database handles the
      // string.
      return toUnescape;
    }
  }
  
  /**
   * The splits of a template converted into Database specific SQL Expressions
   * 
   * @param template
   * @param fi
   * @param dth
   * @return
   */
  public List<Expression> templateToResourceExpression(String[] template,
      FromItem fi, DataTypeHelper dth) {

    List<String> altSeq = Arrays.asList(template);
    List<Expression> newExprs = new ArrayList<Expression>();

    // now create a big concat statement.
    for (int i = 0; i < altSeq.size(); i++) {
      if (i % 2 != 0) {
        String colName = unescape(altSeq.get(i));
        // validate and register the colname first
        // dbaccess.getDataType(fi,colName);
        newExprs.add(dth.cast(
            ColumnHelper.createColumn(fi.getAlias(), colName),
            dth.getStringCastType()));
      } else {
        newExprs.add(dth.cast(new StringValue("\"" + altSeq.get(i)
            + "\""), dth.getStringCastType()));
      }
    }
    return newExprs;

  }
  
private void cleanColumnNames(PlainSelect sb) {
    
    
    SelectVisitor cleaningVisitior = new BaseSelectVisitor(){
      @Override
      public void visit(Column tableColumn) {
        super.visit(tableColumn);

        tableColumn.setColumnName(unescape(tableColumn.getColumnName())); 
        
      }
      
      @Override
      public void visit(Table table) {
        super.visit(table);
        table.setAlias(unescape(table.getAlias()!=null?table.getAlias():table.getName()));
        table.setName(unescape(table.getName()));
        table.setSchemaName(unescape(table.getSchemaName()));
      }
      @Override
      public void visit(SelectExpressionItem selectExpressionItem) {
        super.visit(selectExpressionItem);
        selectExpressionItem.setAlias(unescape(selectExpressionItem.getAlias()));
      }
      
    };
    
    sb.accept(cleaningVisitior);
    

    
  }

  public  TermMap replaceColumn(TermMap tm,TripleMap trm, Map<String,Column> name2Col, List<Table> tables, List<EqualsTo> joinConditions, DataTypeHelper dth){
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
    TermMap newTm =TermMap.createTermMap(dth, expressions);
    if(hasReplaced){
      for(Table table: tables){
        newTm.alias2fromItem.put(table.getAlias(), table);
      }
      newTm.joinConditions.addAll(joinConditions);
    }
    
    newTm.trm = trm;
    

    return newTm; 
  }
  
  
  
 
  
  /**
   * removes all apostrophes from the template and ensure they are correctly
   * capitalized.
   * 
   * @return
   * @throws R2RMLValidationException 
   */

  public String[] cleanTemplate(String template) throws R2RMLValidationException {

    // ((?<!\\\\)\\{)|(\\})
    List<String> altSeq = Arrays.asList(template
        .split("((?<!\\\\)\\{)|(?<!\\\\)\\}"));
    List<String> cleaned = new ArrayList<String>();

    for (int i = 0; i < altSeq.size(); i++) {
      if (i % 2 != 0) {

        cleaned.add(altSeq.get(i));

      } else {
        // static part, no need to change anything, just remove the
        // escape patterns;

        cleaned.add(altSeq.get(i).replaceAll("\\\\", ""));

      }
    }

    return cleaned.toArray(new String[0]);
  }

  public static String cleanSql(String toUnescape) {
    if (toUnescape != null) {

      toUnescape = toUnescape.trim();
      toUnescape = toUnescape.replaceAll("\r\n", " ").replaceAll("\n",
          " ");
      if (toUnescape.endsWith(";")) {
        toUnescape = toUnescape.substring(0, toUnescape.length() - 1);
      }

      return toUnescape;
    } else {
      return toUnescape;
    }
  }


  
  
  
  
  

  
  
  public static Expression resourceToExpression(Resource res, DataTypeHelper dth){
      return dth.cast(new StringValue("\"" + res.getURI() + "\""), dth.getStringCastType());  
  }

}
