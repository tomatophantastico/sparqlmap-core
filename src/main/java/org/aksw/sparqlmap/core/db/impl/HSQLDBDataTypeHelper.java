package org.aksw.sparqlmap.core.db.impl;

import java.util.Arrays;

import org.aksw.sparqlmap.core.UntranslatableQueryException;
import org.aksw.sparqlmap.core.translate.jdbc.DataTypeHelper;

import jersey.repackaged.com.google.common.collect.Lists;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;

public class HSQLDBDataTypeHelper extends DataTypeHelper {
	
	
	public String getDBName() {
		return HSQLDBConnector.HSQLDB_NAME;
	}

	@Override
	public String getBinaryDataType() {
		return "LONGVARCHAR";
	}

	@Override
	public String getStringCastType() {
		return "LONGVARCHAR";
	}

	@Override
	public String getNumericCastType() {
		return "DOUBLE";
	}

	@Override
	public String getBooleanCastType() {
		return"BOOLEAN";
	}

	@Override
	public String getDateCastType() {
		return "DATETIME";
	}

	@Override
	public String getIntCastType() {
		return "INTEGER";
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
    
    if(flags!=null){
      if(flags.equals("i")){
        regex = "(?i)" + regex;
      }else{
        throw new UntranslatableQueryException("HSQL cannot deal with flags other than 'i'");
      }
    }
  
    
    Function regex_matches = new Function();
    regex_matches.setName("REGEXP_SUBSTRING");
    ExpressionList explist = new ExpressionList();
    explist.setExpressions(Arrays.asList(literalValString, new StringValue("'"+regex+"'")));
    regex_matches.setParameters(explist);
    
    Function length = new Function();
    length.setName("LENGTH");
    ExpressionList lengthParams = new ExpressionList();
    lengthParams.setExpressions(Lists.newArrayList((Expression)regex_matches));
    length.setParameters(lengthParams);
    
    GreaterThan gt = new GreaterThan();
    gt.setLeftExpression(length);
    gt.setRightExpression(new LongValue("0"));
    
    

    return gt;

  }

	




}
