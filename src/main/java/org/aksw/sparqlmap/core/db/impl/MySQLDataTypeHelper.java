package org.aksw.sparqlmap.core.db.impl;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringExpression;

import org.aksw.sparqlmap.core.UntranslatableQueryException;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;

public class MySQLDataTypeHelper extends DataTypeHelper {
	
	
	public  String getDBName() {
		return MySQLConnector.MYSQL_DBNAME;
	}

	@Override
	public String getStringCastType() {
		return "CHAR";
	}

	@Override
	public String getNumericCastType() {
		return "DECIMAL";
	}

	@Override
	public String getBooleanCastType() {
		return "CHAR";
	}

	@Override
	public String getDateCastType() {
		return "DATETIME";
	}

	@Override
	public String getIntCastType() {
		return "INT";
	}

	@Override
	public String getBinaryDataType() {
		return "BINARY";
	}

	@Override
	public boolean needsSpecialCastForBinary() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Expression binaryCastPrep(Expression expr) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean needsSpecialCastForChar() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Expression charCastPrep(Expression expr,Integer length) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] binaryResultSetTreatment(byte[] bytes) {
		return bytes;
	}



	
	 @Override
	public Expression cast(Expression expr, String castTo) {
	  return expr;
	}
	 
	 @Override
	public String getRowIdTemplate() {
	    
	    return "select *, @sparqlmapCurRow := @sparqlmapCurRow + 1 AS sm_rowid  from \"%1$s\",(SELECT @sparqlmapCurRow := 0) sparqlmap_rowid;";
	}

  @Override
  public Expression regexMatches(Expression literalValString, String regex, String flags) {
    
    if(flags==null || !flags.equals("i")){
      throw new UntranslatableQueryException("MySQL REGEXP cannot deal with case sensitivity, please consider using the 'i' flag.");
    
    }
    
    
    StringExpression regexp = new StringExpression(literalValString.toString()  +" REGEXP \"" + regex + "\"");
    
    
    
    Parenthesis regexParenthesis = new Parenthesis(regexp);
    
    return regexParenthesis;
  }

}
