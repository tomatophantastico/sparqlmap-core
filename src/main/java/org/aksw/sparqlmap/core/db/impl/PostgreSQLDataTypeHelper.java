package org.aksw.sparqlmap.core.db.impl;

import java.util.Arrays;

import org.aksw.sparqlmap.core.UntranslatableQueryException;
import org.aksw.sparqlmap.core.translate.jdbc.DataTypeHelper;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;


public class PostgreSQLDataTypeHelper extends DataTypeHelper {
	
	
	public String getDBName() {
		return PostgreSQLConnector.POSTGRES_DBNAME;
	}

	@Override
	public String getStringCastType() {
		return "TEXT";
	}

	@Override
	public String getNumericCastType() {
		return "NUMERIC";
	}

	@Override
	public String getBooleanCastType( ) {
		return "BOOLEAN";
	}

	@Override
	public String getDateCastType( ) {
		return "TIMESTAMP";
	}

	@Override
	public String getIntCastType() {
		
		return "INT";
	}

	@Override
	public String getBinaryDataType() {
	
		return "BYTEA";
	}



	@Override
	public boolean needsSpecialCastForChar() {
		
		return true;
	}

	@Override
	public Expression charCastPrep(Expression expr,Integer fieldlength) {
		
		Function rpad = new Function();
		rpad.setName("RPAD");
		ExpressionList padexprlist = new ExpressionList();
		padexprlist.setExpressions(Arrays.asList((Expression) expr, (Expression) new LongValue(fieldlength.toString()) ));
		rpad.setParameters(padexprlist);
		
		
		
		
		return rpad;
	}

	@Override
	public byte[] binaryResultSetTreatment(byte[] bytes) {
		return Arrays.copyOfRange(bytes, 4, bytes.length);
	}


	@Override
	public String getRowIdTemplate() {
	  
	  return "SELECT \"%1$s\".* , row_number() OVER () as sm_rowid FROM \"%1$s\";";
	}

  @Override
  public Expression regexMatches(Expression literalValString, String regex, String flags) {
    
    String regexoperator = "~";
    
    if(flags!=null){
      if(flags.equals("i")){
        regexoperator += "*";
      }else{
        throw new UntranslatableQueryException("Postgres cannot deal with flags other than 'i'");
      }
    }
    
    StringExpression regexp = new StringExpression(literalValString.toString()  + regexoperator + "\"" + regex + "\"");
    
    Parenthesis regexParenthesis = new Parenthesis(regexp);
    
    return regexParenthesis;
  }


	




}
