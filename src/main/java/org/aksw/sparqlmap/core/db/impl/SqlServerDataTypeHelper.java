package org.aksw.sparqlmap.core.db.impl;

import net.sf.jsqlparser.expression.Expression;

import org.aksw.sparqlmap.core.UntranslatableQueryException;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;

public class SqlServerDataTypeHelper extends DataTypeHelper{

  @Override
  public String getDBName() {
    return SqlServerConnector.SQL_SEVER_NAME;
  }

  @Override
  public String getBinaryDataType() {

    return "ntext";
  }

  @Override
  public String getStringCastType() {
    return "ntext";
  }

  @Override
  public String getNumericCastType() {
    return "decimal";
  }

  @Override
  public String getBooleanCastType() {
    return "ntext";
  }

  @Override
  public String getDateCastType() {
    return "datetime2";
  }

  @Override
  public String getIntCastType() {
    return "int";
  }

  @Override
  public boolean needsSpecialCastForBinary() {
    return false;
  }

  @Override
  public Expression binaryCastPrep(Expression expr) {
    return null;
  }

  @Override
  public boolean needsSpecialCastForChar() {
    return false;
  }

  @Override
  public Expression charCastPrep(Expression expr, Integer fieldlength) {
    return null;
  }

  @Override
  public Expression regexMatches(Expression literalValString, String regex, String flags) {
    
    throw new UntranslatableQueryException("Regex is not supported on SQL Server");
  }


}
