package org.aksw.sparqlmap.core.db.impl;

import java.util.Arrays;

import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionWithString;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;


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


	




}
