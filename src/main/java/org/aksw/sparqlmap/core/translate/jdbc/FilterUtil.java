package org.aksw.sparqlmap.core.translate.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.mapper.compatibility.URIHelper;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCTermMap;
import org.aksw.sparqlmap.core.r2rml.jdbc.JDBCTermMapBinder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

@Component
public class FilterUtil {

	static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(FilterUtil.class);
	
	@Autowired
	private JDBCOptimizationConfiguration optConf;
	
	@Autowired
	private DBAccess dbaccess;
	
	@Autowired
	private DataTypeHelper dth;
	
	@Autowired
	private JDBCTermMapBinder tmf;
	

	
	public JDBCOptimizationConfiguration getOptConf() {
		return optConf;
	}
	
public static String CONCAT = "CONCAT";


	public static Expression concat(Expression... expr) {
		
		if(expr.length==0){
			return new NullValue();
		}else if(expr.length==1){
			return expr[0];
		}
		
		Function concat = new Function();
		concat.setName(CONCAT);
		ExpressionList explist = new ExpressionList();
		explist.setExpressions(Arrays.asList(expr));
		concat.setParameters(explist);

		return concat;
	}
	public static Expression coalesce(Expression... expr) {
		if(expr.length==1){
			return expr[0];
		}else if(expr.length>1){
		
		Function concat = new Function();
		concat.setName("COALESCE");
		ExpressionList explist = new ExpressionList();
		explist.setExpressions(Arrays.asList(expr));
		concat.setParameters(explist);

		return concat;
		}else{
			return null;
		}
	}




	
	

	
	
	public static void splitFilters(Expression expr,List<Expression> putInto){
		
		if(expr instanceof AndExpression){
			splitFilters(((AndExpression) expr).getLeftExpression(), putInto);
			splitFilters(((AndExpression) expr).getRightExpression(), putInto);
		}else{
			putInto.add(expr);
		}
		

	}
	
	
	public static Expression conjunct(Collection<Expression> exprs) {
		exprs = new ArrayList<Expression>(exprs);
		if (exprs.isEmpty()) {
			return null;
		} else if (exprs.size() == 1) {
			return exprs.iterator().next();
		} else {
			Expression exp = exprs.iterator().next();
			exprs.remove(exp);
			AndExpression and = new AndExpression(exp, conjunct(exprs));
			return and;
		}
	}
	
	public static Expression disjunct(Collection<Expression> exprs) {
		exprs = new ArrayList<Expression>(exprs);
		if (exprs.isEmpty()) {
			return null;
		} else if (exprs.size() == 1) {
			return exprs.iterator().next();
		} else {
			Expression exp = exprs.iterator().next();
			exprs.remove(exp);
			OrExpression or = new OrExpression(exp, disjunct(exprs));
			return or;
		}
	}
	
	
	
	

	/**
	 * Here the join is created.
	 * 
	 * @param left
	 * @param right
	 * @param test
	 * @return
	 */
	
	public JDBCTermMap compareTermMaps(JDBCTermMap left, JDBCTermMap right, Class<? extends BinaryExpression> test){
	  
		List<Expression> eqs = new ArrayList<Expression>();
		
		try{
		  
		  if(test.equals(EqualsTo.class)){
		    Iterator<Expression> leftI = left.getLiteralVals().iterator();
		    Iterator<Expression> rightI = right.getLiteralVals().iterator();
		    
		    while(leftI.hasNext()){
		      Expression leftExpr = leftI.next();
		      Expression rightExpr = rightI.next();
		      if(leftExpr.equals(rightExpr)){
		          // are equal, we can skip this expression
		      }else{
		      
		      
    		      EqualsTo eq = new EqualsTo();
    		      eq.setLeftExpression(leftExpr);
    		      eq.setRightExpression(rightExpr);
    		      eqs.add(eq);
		      
		      }
		    
		    }
		    
	      
	    }else{
	      
	    

  		if(!isAlwaysTrue(left.literalValBinary, right.literalValBinary)){
  			Expression literalBinaryEquality = bothNullOrBinary(left.literalValBinary, right.literalValBinary, test.newInstance(),dth);
  			eqs.add(andTypesAreEqual(literalBinaryEquality,left,right));
  		}	
  		if(!isAlwaysTrue(left.literalValBool, left.literalValBool)){
  			Expression literalBoolEquality = bothNullOrBinary(left.literalValBool,right.literalValBool,test.newInstance(),dth);
  			eqs.add(andTypesAreEqual(literalBoolEquality,left,right));
  		}
  		
  		if(!isAlwaysTrue(left.literalValDate, right.literalValDate)){
  			Expression literalDateEquality = bothNullOrBinary(left.literalValDate, right.literalValDate, test.newInstance(),dth);
  			eqs.add(andTypesAreEqual(literalDateEquality,left,right));
  		}
  		
  		if(!isAlwaysTrue(left.literalValNumeric, right.literalValNumeric)){
  			Expression literalNumericEquality = bothNullOrBinary(left.literalValNumeric, right.literalValNumeric, test.newInstance(),dth);
  			eqs.add(andTypesAreEqual(literalNumericEquality,left,right));
  		}
  		
  		if(!isAlwaysTrue(left.literalValString, right.literalValString)){
  			Expression literalStringEquality = bothNullOrBinary(left.literalValString,right.literalValString, test.newInstance(),dth);
  			eqs.add(andTypesAreEqual(literalStringEquality,left,right));
  		}
  		
	    }
  		
  		//and check for the resources
  		
  		if(left.resourceColSeg.size()==0&&right.resourceColSeg.size()==0){
  			//no need to do anything
  		}else{
  			if(test.equals(NotEqualsTo.class)||test.equals(EqualsTo.class)){
  				Expression resourceEquality = compareResource(left, right, test);
  				if(resourceEquality!=null){
  					eqs.add( andTypesAreEqual(resourceEquality, left, right));
  				}
  				
  			}else{
  				//only equals and not-equals are defined.
  				eqs.clear();
  				eqs.add(new NullValue());
  			}
  		}
  		
  		// and check that not all of any side are null
  		
	    

		
		} catch (InstantiationException | IllegalAccessException e) {
			log.error("Error creating xpathtest",e);
		}
		
		if(eqs.isEmpty()){
			return tmf.createBoolTermMap( new StringExpression("true"));
		}else{
			return  tmf.createBoolTermMap(new Parenthesis(conjunct(eqs)));
		}
		
	}
	/**
	 * adds the term type and literal type check to the expression.
	 * @return
	 */
	private Expression andTypesAreEqual(Expression toWrap, JDBCTermMap left, JDBCTermMap right){
	    List<Expression> results = Lists.newArrayList();
	    results.add(toWrap);
	  if(!left.getTermType().equals(right.getTermType())){  
    	  EqualsTo termTypeEquals = new EqualsTo();
    	  termTypeEquals.setLeftExpression(left.getTermType());
    	  termTypeEquals.setRightExpression(right.getTermType());
    	  results.add(termTypeEquals);
	  }
	  
	  if(left.getLiteralType().equals(right.getLiteralType())){
    	  EqualsTo literalTypeEquals = new EqualsTo();
    	  literalTypeEquals.setLeftExpression(left.getLiteralType());
    	  literalTypeEquals.setRightExpression(right.getLiteralType());
    	  results.add( literalTypeEquals);	  
	  }
	  
	  return new Parenthesis(conjunct(results));
	  
	}
	
	
	/**
	 * 
	 * @param exprs
	 * @return null, if there is at least one constant value, otherwise an expression with the necessary checks.
	 */
	private Expression areAllNull(List<Expression> exprs){
		List<Expression> areNulls = Lists.newArrayList();
		for(Expression expr: exprs){
			
			expr = DataTypeHelper.uncast(expr);
			
			// check if we have an constant value 
			if(!(expr instanceof NullValue) && !(expr instanceof Column)){
				return null;
			}
			
			if(expr instanceof NullValue){
				// make sure there is at least one "true" value in it
				if (areNulls.isEmpty()) {
					areNulls.add(
							dth.cast(new StringValue("true"), 
									dth.getBooleanCastType()));
				}
			}else{
				IsNullExpression in = new IsNullExpression();
				in.setLeftExpression(expr);
				areNulls.add(in);
			}
						
			
		}
		
		return conjunct(areNulls);
			
	}
	
	
	private Expression compareResource(JDBCTermMap left, JDBCTermMap right,
			Class<? extends BinaryExpression> test)
			throws InstantiationException, IllegalAccessException {
		
		List<Expression> tests = new ArrayList<Expression>();
		
		
		//check if any of the term maps is produced by a subselect
		
		boolean isSubsel = hasSubsel(left) || hasSubsel(right);

		
		if(!optConf.shortcutFilters||isSubsel){
	    // if it is a subselect, we cannot optimize

			BinaryExpression resourceEq=  test.newInstance();
			
			resourceEq.setLeftExpression(FilterUtil.concat(left.resourceColSeg.toArray(new Expression[0])));
			resourceEq.setRightExpression(FilterUtil.concat(right.resourceColSeg.toArray(new Expression[0])));
			
			Expression resourceEquality = bothNullOrBinary(FilterUtil.concat(left.resourceColSeg.toArray(new Expression[0])), FilterUtil.concat(right.resourceColSeg.toArray(new Expression[0])),resourceEq,dth);
			return resourceEquality;
		}else{
		   // no subselect involved, here we go

			Iterator<Expression> leftExprIter = left.getResourceColSeg().iterator();
			Iterator<Expression> rightExprIter = right.getResourceColSeg().iterator();
			Object currentLeft = null;
			Object currentRight = null;
			
			while( currentLeft!=null || leftExprIter.hasNext() ||  currentRight!=null|| rightExprIter.hasNext()){
				
				if(currentLeft==null && leftExprIter.hasNext()){
					currentLeft = DataTypeHelper.uncast(leftExprIter.next());
				}
				if(currentRight==null && rightExprIter.hasNext()){
					currentRight = DataTypeHelper.uncast(rightExprIter.next());
				}
				
				if(currentLeft==null||currentRight==null){
					// there is something left, for which the other part has nothing to match, so it is false;
					return new StringExpression("false");
				}
				
				
				
				
				if(currentLeft instanceof StringValue){
					String leftString = ((StringValue) currentLeft).getNotExcapedValue();
					if(currentRight instanceof StringValue){
						//both String case
						String rightString = ((StringValue) currentRight).getNotExcapedValue();
						String newLeftString = removeStringValue(leftString, rightString);
						
						
						if(newLeftString == null){
							return new NullValue();
						}else if(!newLeftString.isEmpty()){
							currentLeft = new StringValue("'"+newLeftString+"'");
						}else{
							currentLeft = null;
						}
						String newRightString = removeStringValue(rightString, leftString);
						if(newRightString == null){
							return new NullValue();
						}else	if(!newRightString.isEmpty()){
							currentRight = new StringValue("'"+newRightString+"'");
						}else{
							currentRight = null;
						}
						
					}else{
						// one col, one String case
						Column rightColumn = (Column) currentRight;
						//remove everything from the string, the column could produce
						String remains = removeColValue(leftString, rightColumn);
						
						currentRight = null;
						
						if(remains == null){
							return new NullValue();
						}
						String testVal = leftString.substring(0, leftString.length()-remains.length());
						BinaryExpression comp = test.newInstance();
						comp.setLeftExpression(rightColumn);
						comp.setRightExpression(new StringValue("'" + testVal+"'"));
						tests.add(comp);
						
						if(!remains.isEmpty()){
							currentLeft = new StringValue("'"+remains+"'");
						}else{
							currentLeft = null;
						}
						
					
					}
					
				}else{
					Column leftColumn = (Column) currentLeft;
					
					if(currentRight instanceof StringValue){
						String rightString = ((StringValue) currentRight).getNotExcapedValue();
						String remains = removeColValue(rightString,leftColumn );
						
						currentLeft = null;
						if(remains == null){
							return new NullValue();
						}
						String testVal = rightString.substring(0, rightString.length()-remains.length());
						BinaryExpression comp = test.newInstance();
						comp.setLeftExpression(leftColumn);
						comp.setRightExpression(new StringValue("'" + testVal+"'"));
						tests.add(comp);
						
						if(!remains.isEmpty()){
							currentRight = new StringValue("'"+remains+"'");
						}else{
							currentRight = null;
						}
						
						
					}else{
						//both Column, as no sophisticated check for their content is available, they are just assumed to be comparable
						Column rightColumn = (Column) currentRight;
						
						BinaryExpression comp = test.newInstance();
						comp.setLeftExpression(leftColumn);
						comp.setRightExpression(rightColumn);
						tests.add(comp);
						
						currentLeft =null;
						currentRight = null;
						
					}
					
				}
			}		
			return FilterUtil.conjunct(tests);
		}
	}
	public boolean hasSubsel(JDBCTermMap left) {
		for (Expression expr : left.getResourceColSeg()){
			 if(expr instanceof Column){
				 //maybe not the best way to determine that.
				 if(((Column) expr).getTable().getName().startsWith(PlainSelectWrapper.SUBSEL_SUFFIX)){
					 return true;
				 }
			 }
		 }
		return false;
	}
	
	
	public String removeColValue(String uripart, Column col){
		
		Integer colDataType =  dbaccess.getDataType(col.getTable().getName(), col.getColumnName());
		String coldRdfDataTypeString= dth.getCastTypeString(colDataType);
		
		if(coldRdfDataTypeString==null || dth.getStringCastType().equals(coldRdfDataTypeString)){
			//we need to go to the next uri delimiter, which require going 
			byte[] chars = uripart.getBytes();
			int i = 0;
			
			
			for(;i<chars.length;i++){
				if(URIHelper.RESERVED.get(chars[i])){
					break;
				}
			}
			return uripart.substring(i);
			
		}else if(dth.getNumericCastType().equals(dth.getCastTypeString(colDataType))){
			char[] chars = uripart.toCharArray();
			int i = 0;
			for(;i<chars.length;i++){
				if(!Character.isDigit(chars[i])){
					break;
				}
			}
			return uripart.substring(i);

			
		}else{
			throw new ImplementationException("Check for non-String/Integer data types in uris");
		}
	
	}
	
	public String removeStringValue(String left, String right){
		if(left.length()<right.length() && right.startsWith(left)){
			return "";
		}else if(left.startsWith(right)){
			return left.substring(right.length());
		}else{
			return null;
		}
	}
	
	public boolean compareColumns(Column left, Column right){
		
		//check here for basic compati
		return true;
	}


	
	
	private Expression bothNullOrBinary(Expression expr1, Expression expr2, BinaryExpression function, DataTypeHelper dth){
		
		// odd, but left and right seems to be twisted
		function.setLeftExpression(expr2);
		function.setRightExpression(expr1);
		
		return function;
		
	}
	
	/**
	 * Check if the comparison is between two null values and will therefore always be true. Also check for same content for static values.
	 * @return
	 */
	private boolean isAlwaysTrue(Expression left, Expression right){
		if(optConf.shortcutFilters){
			left = DataTypeHelper.uncast(left);
			right = DataTypeHelper.uncast(right);
			//check for both null
			
			if(left instanceof NullValue && right instanceof NullValue){
				return true;
			}
			//check for both same constant value
			if(left instanceof StringValue && right instanceof StringValue){
				if (((StringValue)left).getNotExcapedValue().equals(((StringValue)right).getNotExcapedValue())){
					return true;
				}
			}
			if(left instanceof LongValue && right instanceof LongValue){
				if (((LongValue)left).getStringValue().equals(((LongValue)right).getStringValue())){
					return true;
				}
			}
		}
		return false;
	}
	
	

}
