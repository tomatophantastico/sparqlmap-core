package org.aksw.sparqlmap.core.r2rml.jdbc;

import java.util.ArrayList;
import java.util.List;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.translate.jdbc.DataTypeHelper;
import org.apache.jena.rdf.model.RDFNode;
import org.springframework.beans.factory.annotation.Autowired;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;


public class JDBCColumnHelper {

  public static String R2R_COL_SUFFIX = "_R2R";

  public static String COL_NAME_RDFTYPE = R2R_COL_SUFFIX + "_01TP";

  public static String COL_NAME_LITERAL_TYPE = R2R_COL_SUFFIX + "_02LITT";

  public static String COL_NAME_LITERAL_LANG = R2R_COL_SUFFIX + "_03LITL";

  public static String COL_NAME_LITERAL_STRING = R2R_COL_SUFFIX + "_04LS";

  public static String COL_NAME_LITERAL_NUMERIC = R2R_COL_SUFFIX + "_05LN";

  public static String COL_NAME_LITERAL_DATE = R2R_COL_SUFFIX + "_06LD";

  public static String COL_NAME_LITERAL_BOOL = R2R_COL_SUFFIX + "_07LB";

  public static String COL_NAME_LITERAL_BINARY = R2R_COL_SUFFIX + "_08_LZ";

  public static String COL_NAME_RESOURCE_COL_SEGMENT = R2R_COL_SUFFIX + "_09R";

  public static String COL_NAME_ORDER_BY = R2R_COL_SUFFIX + "_XXOB";

  public static String COL_NAME_INTERNAL = "B";

  public static Integer COL_VAL_TYPE_RESOURCE = 1;

  public static Integer COL_VAL_TYPE_LITERAL = 2;

  public static Integer COL_VAL_TYPE_BLANK = 3;

  public static Integer COL_VAL_SQL_TYPE_RESOURCE = -9999;

  public static Integer COL_VAL_SQL_TYPE_CONSTLIT = -9998;

  public static Integer COL_VAL_RES_LENGTH_LITERAL = 0;
  
  public static Integer COL_VAL_LITERAL_TYPE_NONE = 0;
  
  public static Integer COL_VAL_LITERAL_TYPE_NUM = 1;
  
  public static Integer COL_VAL_LITERAL_TYPE_STRING = 2;
  
  public static Integer COL_VAL_LITERAL_TYPE_BOOL = 3;
  
  public static Integer COL_VAL_LITERAL_TYPE_DATE = 4;
  
  public static Integer COL_VAL_LITERAL_TYPE_BINARY = 5;



  

  @Autowired
  private DBAccess dbaccess;

  @Autowired
  private DataTypeHelper dth;

 
  public static String colnameBelongsToVar(String colalias) {
    return colalias.substring(0, colalias.indexOf(R2R_COL_SUFFIX));
  }

  public static boolean isColnameResourceSegment(String colalias) {
    
    boolean result = false;
    String substringToCheck = colalias.substring(colalias.indexOf(R2R_COL_SUFFIX) );
    result = substringToCheck.startsWith(COL_NAME_RESOURCE_COL_SEGMENT);

    return result;
  }
  
  /*
   * Here be helper methods.
   */

  public static Integer getRDFType(RDFNode node) {
    Integer rdfType;
    if (node.isURIResource()) {
      rdfType = COL_VAL_TYPE_RESOURCE;
    } else if (node.isLiteral()) {
      rdfType = COL_VAL_TYPE_LITERAL;
    } else if (node.isAnon()) {
      rdfType = COL_VAL_TYPE_BLANK;
    } else {
      throw new ImplementationException("Encountered unknown Node type");
    }

    return rdfType;
  }

  public static Expression asExpression(String string, DataTypeHelper dth) {
    return dth.cast(new StringValue("\"" + string + "\""), dth.getStringCastType());
  }

  public static Table createTable(String tablename) {
    return createTable(null, tablename);
  }

  public static Table createTable(String schema, String tablename) {
    Table tab = new Table();
    tab.setName(tablename);
    tab.setAlias(tablename);
    tab.setSchemaName(schema);
    return tab;

  }

  public static Column createColumn(String table, String column) {
    return createColumn(null, table, column);
  }

  public static Column createColumn(String schema, String table, String column) {
    Column col = new Column();
    col.setColumnName(column);
    Table tab = createTable(schema, table);
    col.setTable(tab);

    return col;

  }

  //
  // public void createBooleanExpressions(Expression boolExpr, DataTypeHelper dth){
  //
  //
  // getBaseExpressions(COL_VAL_TYPE_LITERAL, 0, XSDDatatype.XSDboolean.toString(), null, null);
  //
  // //return getExpression(boolExpr, dth);
  // }

  public static Expression getTermType(List<Expression> expressions) {
    return expressions.get(0);
  }

  public static Expression getDataType(List<Expression> expressions) {
    return expressions.get(1);
  }

  public static Expression getLanguage(List<Expression> expressions) {
    return expressions.get(2);
  }

  public static Expression getLiteralStringExpression(List<Expression> expressions) {
    return expressions.get(3);
  }

  public static Expression getLiteralNumericExpression(List<Expression> expressions) {
    return expressions.get(4);
  }

  public static Expression getLiteralDateExpression(List<Expression> expressions) {
    return expressions.get(5);
  }

  public static Expression getLiteralBoolExpression(List<Expression> expressions) {

    return expressions.get(6);
  }

  public static Expression getLiteralBinaryExpression(List<Expression> expressions) {
    return expressions.get(7);
  }

  public static List<Expression> getResourceExpressions(List<Expression> expressions) {

    return expressions.subList(8, expressions.size());
  }

  public static List<Expression> getLiteralExpression(List<Expression> expressions) {
    List<Expression> exps = new ArrayList<Expression>();
    exps.add(getLiteralStringExpression(expressions));
    exps.add(getLiteralNumericExpression(expressions));
    exps.add(getLiteralDateExpression(expressions));
    exps.add(getLiteralBoolExpression(expressions));
    exps.add(getLiteralBinaryExpression(expressions));

    return exps;
  }

}
