package org.aksw.sparqlmap.core.db.impl;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import org.aksw.sparqlmap.core.UntranslatableQueryException;
import org.aksw.sparqlmap.core.translate.jdbc.DataTypeHelper;

import com.google.common.collect.Lists;

public class OracleDataTypeHelper extends DataTypeHelper {
	
	
	public String getDBName() {
		return OracleConnector.ORACLE_DB_NAME;
	}

	@Override
	public String getStringCastType() {
		return "VARCHAR2(4000)";
	}

	@Override
	public String getNumericCastType() {
		return "NUMBER";
	}

	@Override
	public String getBooleanCastType() {
		return "CHAR";
	}

	@Override
	public String getDateCastType() {
		return "DATE";
	}

	@Override
	public String getIntCastType() {
		return "NUMBER";
	}

	@Override
	public String getBinaryDataType() {
		return "VARCHAR2(4000)";
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
	public Expression charCastPrep(Expression expr,Integer length) {
		return null;
	}

	@Override
	public byte[] binaryResultSetTreatment(byte[] bytes) {
		return bytes;
	}


	
	@Override
	public String getValidateFromQuery(String from) {
		return "select *\n" + 
				"  from  \n" + 
				"( select * \n" + 
				"    from " + from + " \n" + 
				"  ) \n" + 
				" where ROWNUM <= 1";
	}
	@Override
	public String getColnameQuery(String colname, String from) {
		
		return "select *\n" + 
				"  from  \n" + 
				"( select "+ colname +" \n" + 
				"    from " + from + " \n" + 
				"  ) \n" + 
				" where ROWNUM <= 1";

	}
	
	@Override
	public Expression getDateDefaultExpression() {
	  
	  Function to_date = new Function();
	  to_date.setName("to_date");
	  to_date.setParameters(new ExpressionList(Lists.newArrayList((Expression)new StringValue("'01-01-0001'"),new StringValue("'DD-MM-YYYY'"))));
	  return to_date; 
	}

  @Override
  public Expression regexMatches(Expression literalValString, String regex, String flags) {
    
    String param = "c";
    
    if(flags!=null){
      if(flags.equals("i")){
        param = "i";
      }else{
        throw new UntranslatableQueryException("SparqlMap/Oracle cannot deal with flags other than 'i'");
      }
    }

    ExpressionList exprList = new ExpressionList(Lists.newArrayList(literalValString,  new StringValue("'"+regex+"'"),  new StringValue("'"+param+"'")));

    Function regexp_like = new Function();
    regexp_like.setName("REGEXP_LIKE");
    regexp_like.setParameters(exprList);
    return regexp_like;
  }
}
